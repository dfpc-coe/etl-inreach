import moment from 'moment';
import { FeatureCollection, Feature, Geometry } from 'geojson';
import { Static, Type, TSchema } from '@sinclair/typebox';
import xml2js from 'xml2js';
import ETL, { Event, SchemaType, handler as internal, local, env } from '@tak-ps/etl';

export interface Share {
    ShareId: string;
    CallSign?: string;
    Password?: string;
}

export default class Task extends ETL {
    async schema(type: SchemaType = SchemaType.Input): Promise<TSchema> {
        if (type === SchemaType.Input) {
            return Type.Object({
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
                    description: 'Print ADSBX results in logs'
                })
            })
        } else {
            return Type.Object({
                inreachId: Type.String(),
                inreachName: Type.String(),
                inreachDeviceType: Type.String(),
                inreachIMEI: Type.String(),
                inreachIncidentId: Type.String(),
                inreachValidFix: Type.String(),
                inreachText: Type.String(),
                inreachEvent: Type.String(),
                inreachDeviceId: Type.String(),
                inreachReceive: Type.String({ format: 'date-time' }),
            })
        }
    }

    async control(): Promise<void> {
        const layer = await this.fetchLayer();

        if (!layer.environment.INREACH_MAP_SHARES) throw new Error('No INREACH_MAP_SHARES Provided');
        if (!Array.isArray(layer.environment.INREACH_MAP_SHARES)) throw new Error('INREACH_MAP_SHARES must be an array');

        const obtains: Array<Promise<Feature[]>> = [];
        for (const share of layer.environment.INREACH_MAP_SHARES) {
            obtains.push((async (share: Share): Promise<Feature[]> => {
                try {
                    if (share.ShareId.startsWith('https://')) {
                        share.ShareId = new URL(share.ShareId).pathname.replace(/^\//, '');
                    } else if (share.ShareId.startsWith('share.garmin.com')) {
                        share.ShareId = share.ShareId.replace('share.garmin.com/', '');
                    }
                } catch (err) {
                    console.error(err);
                }

                if (!share.CallSign) share.CallSign = share.ShareId;
                console.log(`ok - requesting ${share.ShareId} ${share.CallSign ? `(${share.CallSign})` : ''}`);

                const url = new URL(`/feed/Share/${share.ShareId}`, 'https://explore.garmin.com')
                url.searchParams.append('d1', moment().subtract(30, 'minutes').utc().format());

                const kmlres = await fetch(url);
                const body = await kmlres.text();

                const featuresmap: Map<string, Feature> = new Map();
                const features: Feature[] = [];

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

                    const feat: Feature<Geometry, { [name: string]: any; }> = {
                        id: `inreach-${share.CallSign}`,
                        type: 'Feature',
                        properties: {
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
                            course: Number(extended['Course'].replace(/\s.*/, '')),
                            speed: Number(extended['Velocity'].replace(/\s.*/, '')) * 0.277778, //km/h => m/s
                            callsign: share.CallSign,
                            time: new Date(placemark.TimeStamp[0].when[0]),
                            start: new Date(placemark.TimeStamp[0].when[0])
                        },
                        geometry: {
                            type: 'Point',
                            coordinates: coords
                        }
                    };

                    if (featuresmap.has(String(feat.id))) {
                        const existing = featuresmap.get(String(feat.id));

                        if (moment(feat.properties.time).isAfter(existing.properties.time)) {
                            featuresmap.set(String(feat.id), feat);
                        }
                    } else {
                        featuresmap.set(String(feat.id), feat);
                    }
                }

                features.push(...Array.from(featuresmap.values()))

                return features;
            })(share))
        }

        const fc: FeatureCollection = {
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

env(import.meta.url)
await local(new Task(), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(), event);
}
