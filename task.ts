import { Feature } from '@tak-ps/node-cot';
import type { Static, TSchema } from '@sinclair/typebox';
import { Type } from '@sinclair/typebox';
import xml2js from 'xml2js';
import ETL, { DataFlowType, SchemaType, handler as internal, local, InvocationType } from '@tak-ps/etl';
import type { Event } from '@tak-ps/etl';

export interface Share {
    ShareId: string;
    CallSign?: string;
    Password?: string;
}

const Input = Type.Object({
    'INREACH_MAP_SHARES': Type.Array(Type.Object({
        ShareId: Type.String({ description: 'Garmin Inreach Share ID or URL' }),
        CallSign: Type.Optional(Type.String({ description: 'Human Readable Name of the Operator - Used as the callsign in TAK' })),
        Password: Type.Optional(Type.String({ description: 'Optional: Garmin Inreach MapShare Password' }))
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
                    inreachDeviceType: Type.String(),
                    inreachIMEI: Type.Optional(Type.String()),
                    inreachIncidentId: Type.Optional(Type.String()),
                    inreachValidFix: Type.Optional(Type.String()),
                    inreachText: Type.Optional(Type.String()),
                    inreachEvent: Type.Optional(Type.String()),
                    inreachDeviceId: Type.String(),
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

        const obtains: Array<Promise<Static<typeof Feature.InputFeature>[]>> = [];
        for (const share of env.INREACH_MAP_SHARES) {
            obtains.push((async (share: Share): Promise<Static<typeof Feature.InputFeature>[]> => {
                try {
                    if (share.ShareId.startsWith('https://')) {
                        share.ShareId = new URL(share.ShareId).pathname.replace(/^\//, '');
                    } else if (share.ShareId.startsWith('share.garmin.com')) {
                        share.ShareId = share.ShareId.replace('share.garmin.com/', '');
                    }
                    if (!share.CallSign) share.CallSign = share.ShareId;
                    console.log(`ok - requesting https://share.garmin.com/${share.ShareId} ${share.CallSign ? `(${share.CallSign})` : ''}`);

                    const url = new URL(`/Feed/Share/${share.ShareId}`, 'https://share.garmin.com')

                    const d1 = new Date();
                    d1.setMinutes(d1.getMinutes() - 30);
                    url.searchParams.append('d1', d1.toISOString());

                    const headers = new Headers();
                    if (share.Password) {
                        headers.append('Authorization', 'Basic ' + Buffer.from(":" + share.Password).toString('base64'));
                    }

                    const kmlres = await fetch(url, { headers });
                    const body = await kmlres.text();

                    const featuresmap: Map<string, Static<typeof Feature.InputFeature>> = new Map();
                    const features: Static<typeof Feature.InputFeature>[] = [];

                    if (!body.trim()) return features;

                    const xml = await xml2js.parseStringPromise(body);
                    if (!xml.kml || !xml.kml.Document) throw new Error('XML Parse Error: Document not found');
                    if (!xml.kml.Document[0] || !xml.kml.Document[0].Folder || !xml.kml.Document[0].Folder[0]) return;

                    console.log(`ok - ${share.ShareId} has ${xml.kml.Document[0].Folder[0].Placemark.length} locations`);
                    for (const placemark of xml.kml.Document[0].Folder[0].Placemark) {
                        if (!placemark.Point || !placemark.Point[0]) continue;

                        const coords = placemark.Point[0].coordinates[0].split(',').map((ele: string) => {
                            return parseFloat(ele);
                        });

                        const extended: Record<string, string> = {};
                        for (const ext of placemark.ExtendedData[0].Data) {
                            extended[ext.$.name] = ext.value[0];
                        }

                        const id = `inreach-${extended['IMEI']}`;
                        const feat: Static<typeof Feature.InputFeature> = {
                            id,
                            type: 'Feature',
                            properties: {
                                course: Number(extended['Course'].replace(/\s.*/, '')),
                                speed: Number(extended['Velocity'].replace(/\s.*/, '')) * 0.277778, //km/h => m/s
                                callsign: share.CallSign,
                                time: new Date(placemark.TimeStamp[0].when[0]).toISOString(),
                                start: new Date(placemark.TimeStamp[0].when[0]).toISOString(),
                                links: [{
                                    uid: id,
                                    relation: 'r-u',
                                    mime: 'text/html',
                                    url: `https://share.garmin.com/${share.ShareId}`,
                                    remarks: 'Garmin Portal'

                                }],
                                metadata: {
                                    inreachId: extended['Id'],
                                    inreachName: extended['Name'],
                                    inreachDeviceType: extended['Device Type'],
                                    inreachIMEI: extended['IMEI'],
                                    inreachIncidentId: extended['Incident Id'],
                                    inreachValidFix: extended['Valid GPS Fix'],
                                    inreachText: extended['Text'],
                                    inreachEvent: extended['Event'],
                                    inreachDeviceId: extended['Device Identifier'],
                                    inreachReceive: new Date(placemark.TimeStamp[0].when[0]).toISOString(),
                                }
                            },
                            geometry: {
                                type: 'Point',
                                coordinates: coords
                            }
                        };

                        if (featuresmap.has(String(feat.id))) {
                            const existing = featuresmap.get(String(feat.id));

                            if (new Date(feat.properties.time) > new Date(existing.properties.time)) {
                                featuresmap.set(String(feat.id), feat);
                            }
                        } else {
                            featuresmap.set(String(feat.id), feat);
                        }
                    }

                    features.push(...Array.from(featuresmap.values()))

                    return features;
                } catch (err) {
                    console.error(`FEED: ${share.CallSign}: ${err}`);
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
