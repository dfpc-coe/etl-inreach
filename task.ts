import { Feature } from '@tak-ps/node-cot';
import type { Static, TSchema } from '@sinclair/typebox';
import { Type } from '@sinclair/typebox';
import ETL, { DataFlowType, SchemaType, handler as internal, local, InvocationType } from '@tak-ps/etl';
import type { Event } from '@tak-ps/etl';

const GARMIN_BASE = 'https://share.garmin.com';
const HISTORY_WINDOW_MS = 30 * 60 * 1000;

export interface Share {
    ShareId: string;
    CallSign?: string;
    Password?: string;
}

/**
 * A single position pushed by the MapHub SignalR hub
 * Field meanings decoded from the MappointModel in share.garmin.com's map.js bundle
 */
export interface MapShareLocation {
    L: number;              // Latitude
    N: number;              // Longitude
    A: number;              // Altitude in meters
    G: number;              // Speed in km/h - 0 also marks the course as invalid
    C: number;              // Course in 22.5 degree increments (0-15)
    D: string;              // UTC timestamp
    S: number;              // Point type: 3=TextMessage, 5=LocationResponse, 6=TrackPoint, 7=WaypointNav
    X: string;              // Message text
    M: number;              // Message ID
    T?: number;             // Track ID
    P?: number | null;      // Battery: 0=Normal, non-zero=Low, null=Unknown
}

export interface MapShareUser {
    Id: number;
    EncryptedUserId: string;
    AssignedDeviceId: number | null;
    FirstName: string;
    LastName: string;
    MapDisplayName: string;
    TrackingStatus: boolean;
    HasData: boolean;
    MatchesCurrentFilter: boolean | null;
    InSOS: boolean;
    Locations: MapShareLocation[];
    LastSeenTime: string | null;
    GroupID: number;
    ColorCode: string;
}

interface HubMessage {
    H?: string;             // Hub name
    M?: string;             // Method name
    A?: unknown[];          // Arguments
}

interface HubEnvelope {
    C?: string;             // Message cursor
    M?: HubMessage[];       // Hub messages
}

/**
 * Pull users & positions for a MapShare page over the anonymous SignalR MapHub websocket
 * Flow: negotiate => websocket connect (server pushes AddUsers) => invoke
 * updateFilter with a time window (server pushes UpdateUser with Locations)
 */
export async function fetchMapShare(
    shareId: string,
    opts: {
        startTime: Date;
        endTime: Date;
        timeout?: number;
        settle?: number;
    }
): Promise<MapShareUser[]> {
    const shareUrl = `${GARMIN_BASE}/${shareId}`;

    // Garmin's CloudFlare worker pins the SignalR connection to a backend region
    // using the Referer header - without it negotiate succeeds but the MapHub
    // never associates the socket with a share and no user list is ever pushed
    const httpHeaders = { 'Referer': shareUrl };

    const params = (extra: Record<string, string> = {}): string => {
        return new URLSearchParams({
            clientProtocol: '2.1',
            connectionData: JSON.stringify([{ name: 'maphub' }]),
            // The referrer query param is how the server determines which MapShare to serve
            referrer: shareUrl,
            ...extra
        }).toString();
    };

    const negres = await fetch(`${GARMIN_BASE}/signalr/negotiate?${params()}`, { headers: httpHeaders });
    if (!negres.ok) throw new Error(`${shareId}: SignalR negotiate failed: HTTP ${negres.status}`);
    const neg = await negres.json() as { ConnectionToken?: string };
    if (!neg.ConnectionToken) throw new Error(`${shareId}: SignalR negotiate did not return a ConnectionToken`);

    const connectParams = params({
        transport: 'webSockets',
        connectionToken: neg.ConnectionToken
    });

    return await new Promise<MapShareUser[]>((resolve, reject) => {
        const ws = new WebSocket(`wss://share.garmin.com/signalr/connect?${connectParams}`);

        const users: Map<number, MapShareUser> = new Map();
        let invocation = 0;
        let filterSent = false;
        let done = false;
        let settleTimer: ReturnType<typeof setTimeout> | undefined;

        const finish = (err?: Error): void => {
            if (done) return;
            done = true;
            clearTimeout(hardTimer);
            if (settleTimer) clearTimeout(settleTimer);
            try {
                ws.close();
            } catch (closeErr) {
                console.error(`${shareId}: ${closeErr}`);
            }
            if (err) reject(err);
            else resolve(Array.from(users.values()));
        };

        const hardTimer = setTimeout(() => {
            if (filterSent) {
                finish();
            } else {
                finish(new Error(`${shareId}: timed out waiting for MapShare user list - the share may be invalid, disabled or password protected`));
            }
        }, opts.timeout ?? 30000);

        // Resolve once the server has gone quiet after the filter was applied
        const settle = (): void => {
            if (settleTimer) clearTimeout(settleTimer);
            settleTimer = setTimeout(() => finish(), opts.settle ?? 5000);
        };

        const upsert = (user: MapShareUser): void => {
            if (!user || typeof user.Id !== 'number') return;
            if (!Array.isArray(user.Locations)) user.Locations = [];

            // The server pushes several UpdateUser messages per user - never
            // let a later push with an empty Locations array clobber positions
            const existing = users.get(user.Id);
            if (existing && existing.Locations.length) {
                const seen = new Set(user.Locations.map((loc) => `${loc.D}-${loc.M}`));
                user.Locations.push(...existing.Locations.filter((loc) => !seen.has(`${loc.D}-${loc.M}`)));
            }

            users.set(user.Id, user);
        };

        const sendFilter = (): void => {
            if (filterSent || done) return;
            filterSent = true;

            ws.send(JSON.stringify({
                H: 'maphub',
                M: 'updateFilter',
                A: [{
                    UUID: crypto.randomUUID(),
                    StartTime: opts.startTime.toISOString(),
                    EndTime: opts.endTime.toISOString(),
                    LastXSeconds: null,
                    ChosenGroup: null,
                    MessageTypes: [5, 3, 6, 7, 9, 10],
                    Zoom: 12,
                    MBR: null,
                    FiltersExpanded: false,
                    BookmarkId: null,
                    DateSelection: null,
                    TypeFilterRestriction: null,
                    TypeFilterRestrictions: 15,
                    FilterType: null,
                    MapExpandedUsers: [],
                    UserListExpandedUsers: [],
                    CheckedUsers: Array.from(users.keys()),
                    ExpandedGroups: [],
                    HiddenMessages: [],
                    HiddenTracks: []
                }],
                I: invocation++
            }));

            settle();
        };

        ws.onopen = () => {
            // Complete the classic ASP.NET SignalR handshake - data pushes begin
            // as soon as the transport connects so a failure here is non-fatal
            fetch(`${GARMIN_BASE}/signalr/start?${connectParams}`, { headers: httpHeaders }).catch((err) => {
                console.error(`${shareId}: SignalR start failed: ${err}`);
            });
        };

        ws.onerror = () => {
            finish(new Error(`${shareId}: MapShare websocket connection failed`));
        };

        ws.onclose = () => {
            if (filterSent) finish();
            else finish(new Error(`${shareId}: MapShare websocket closed before the user list was received`));
        };

        ws.onmessage = (msg) => {
            let body: HubEnvelope;
            try {
                body = JSON.parse(String(msg.data));
            } catch (parseErr) {
                console.error(`${shareId}: ${parseErr}`);
                return;
            }

            for (const hubmsg of body.M ?? []) {
                // Classic SignalR matches hub methods case-insensitively
                const method = (hubmsg.M ?? '').toLowerCase();

                if (method === 'pingback') {
                    ws.send(JSON.stringify({ H: 'maphub', M: 'Pong', A: [], I: invocation++ }));
                } else if (method === 'addusers') {
                    for (const user of (hubmsg.A ? hubmsg.A[0] as MapShareUser[] : [])) {
                        upsert(user);
                    }
                    sendFilter();
                } else if (method === 'adduser' || method === 'updateuser' || method === 'addtrackpoints') {
                    if (hubmsg.A && hubmsg.A[0]) upsert(hubmsg.A[0] as MapShareUser);
                    if (filterSent) settle();
                }
            }
        };
    });
}

const Input = Type.Object({
    'INREACH_MAP_SHARES': Type.Array(Type.Object({
        ShareId: Type.String({ description: 'Garmin Inreach Share ID or URL' }),
        CallSign: Type.Optional(Type.String({ description: 'Human Readable Name of the Operator - Used as the callsign in TAK when the Share contains a single device, otherwise the Garmin Display Name is used' })),
        Password: Type.Optional(Type.String({ description: 'Optional: Garmin Inreach MapShare Password - NOTE: Password protected shares are not currently supported by the Garmin MapShare API' }))
    }, {
        description: 'Inreach Share IDs to pull data from',
        display: 'table',
    })),
    'DEBUG': Type.Boolean({
        default: false,
        description: 'Print debug info in logs'
    })
})

export default class Task extends ETL {
    static name = 'etl-inreach'
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return Input;
            } else {
                return Type.Object({
                    inreachId: Type.String(),
                    inreachName: Type.String(),
                    inreachDeviceId: Type.Optional(Type.String()),
                    inreachShareId: Type.String(),
                    inreachEmergency: Type.Boolean(),
                    inreachText: Type.Optional(Type.String()),
                    inreachBattery: Type.Optional(Type.String()),
                    inreachReceive: Type.String({ format: 'date-time' }),
                })
            }
        } else {
            return Type.Object({});
        }
    }

    async control(): Promise<void> {
        const env = await this.env(Input);

        if (!env.INREACH_MAP_SHARES) throw new Error('No INREACH_MAP_SHARES Provided');
        if (!Array.isArray(env.INREACH_MAP_SHARES)) throw new Error('INREACH_MAP_SHARES must be an array');

        const errors: string[] = [];

        const obtains: Array<Promise<Static<typeof Feature.InputFeature>[]>> = [];
        for (const share of env.INREACH_MAP_SHARES) {
            obtains.push((async (share: Share): Promise<Static<typeof Feature.InputFeature>[]> => {
                try {
                    if (share.ShareId.startsWith('https://')) {
                        share.ShareId = new URL(share.ShareId).pathname.replace(/^\/|\/$/g, '');
                    } else if (share.ShareId.startsWith('share.garmin.com')) {
                        share.ShareId = share.ShareId.replace('share.garmin.com/', '').replace(/\/$/, '');
                    }
                    if (share.Password) {
                        console.warn(`warn - ${share.ShareId}: password protected shares are not supported by the Garmin MapShare API - attempting anonymous access`);
                    }

                    console.log(`ok - requesting https://share.garmin.com/${share.ShareId} ${share.CallSign ? `(${share.CallSign})` : ''}`);

                    const endTime = new Date();
                    const startTime = new Date(endTime.getTime() - HISTORY_WINDOW_MS);

                    const users = await fetchMapShare(share.ShareId, { startTime, endTime });

                    const features: Static<typeof Feature.InputFeature>[] = [];

                    for (const user of users) {
                        const positions = user.Locations.filter((loc) => {
                            return typeof loc.L === 'number' && typeof loc.N === 'number' && loc.D;
                        });

                        if (!positions.length) continue;

                        const latest = positions.reduce((a, b) => {
                            return new Date(a.D) >= new Date(b.D) ? a : b;
                        });

                        const callsign = (users.length === 1 && share.CallSign)
                            ? share.CallSign
                            : user.MapDisplayName
                                || [user.FirstName, user.LastName].filter(Boolean).join(' ')
                                || `${share.ShareId}-${user.Id}`;

                        const metadata: Record<string, string | boolean> = {
                            inreachId: String(user.Id),
                            inreachName: user.MapDisplayName,
                            inreachShareId: share.ShareId,
                            inreachEmergency: user.InSOS === true,
                            inreachReceive: new Date(latest.D).toISOString(),
                        };

                        if (user.AssignedDeviceId !== null && user.AssignedDeviceId !== undefined) {
                            metadata.inreachDeviceId = String(user.AssignedDeviceId);
                        }

                        if (latest.X) {
                            metadata.inreachText = latest.X;
                        }

                        if (latest.P !== null && latest.P !== undefined) {
                            metadata.inreachBattery = latest.P === 0 ? 'Normal' : 'Low';
                        }

                        const id = `inreach-${user.Id}`;
                        const feat: Static<typeof Feature.InputFeature> = {
                            id,
                            type: 'Feature',
                            properties: {
                                callsign,
                                speed: latest.G * 0.277778, // km/h => m/s
                                time: new Date(latest.D).toISOString(),
                                start: new Date(latest.D).toISOString(),
                                links: [{
                                    uid: id,
                                    relation: 'r-u',
                                    mime: 'text/html',
                                    url: `https://share.garmin.com/${share.ShareId}`,
                                    remarks: 'Garmin Portal'
                                }],
                                metadata
                            },
                            geometry: {
                                type: 'Point',
                                coordinates: [latest.N, latest.L, latest.A]
                            }
                        };

                        // A speed of 0 marks the course as invalid
                        if (latest.G > 0) {
                            feat.properties.course = latest.C * 22.5;
                        }

                        features.push(feat);
                    }

                    console.log(`ok - ${share.ShareId} has ${users.length} users & ${features.length} locations in the last ${HISTORY_WINDOW_MS / 60000} minutes`);

                    return features;
                } catch (err) {
                    const msg = `${share.CallSign || share.ShareId}: ${err instanceof Error ? err.message : String(err)}`;
                    console.error(`FEED ERROR: ${msg}`);
                    errors.push(msg);
                    return [];
                }
            })(share))
        }

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        }

        for (const res of await Promise.all(obtains)) {
            if (!res || !res.length) continue;
            fc.features.push(...res);
        }

        await this.submit(fc);

        // Submit whatever succeeded first, then surface any feed failures so the
        // layer turns red and CloudTAK admins are alerted
        if (errors.length) {
            throw new Error(`${errors.length} of ${env.INREACH_MAP_SHARES.length} MapShare feeds failed: ${errors.join('; ')}`);
        }
    }
}

await local(await Task.init(import.meta.url), import.meta.url);

export async function handler(event: Event = {}, context?: object) {
    return await internal(await Task.init(import.meta.url, {
        logging: {
            event: process.env.DEBUG ? true : false,
        }
    }), event, context);
}
