import type { InputFeatureCollection, InputFeature } from '@tak-ps/etl';
import type { Static, TSchema } from '@sinclair/typebox';
import { Type } from '@sinclair/typebox';
import xml2js from 'xml2js';
import ETL, { DataFlowType, SchemaType, handler as internal, local, InvocationType } from '@tak-ps/etl';
import type { Event } from '@tak-ps/etl';

export interface Share {
    ShareId: string;
    CallSign?: string;
    Password?: string;
    CoTType?: string;
}

const EphemeralSchema = Type.Object({
    deviceStates: Type.Record(Type.String(), Type.Object({
        currentLat: Type.Number(),
        currentLon: Type.Number(),
        lastUpdate: Type.String({ format: 'date-time' }),
        currentAngle: Type.Optional(Type.Number()),
        stepCount: Type.Number({ default: 0 }),
        wasEmergency: Type.Optional(Type.Boolean({ default: false }))
    }))
});

const Input = Type.Object({
    'INREACH_MAP_SHARES': Type.Array(Type.Object({
        ShareId: Type.String({ description: 'Garmin Inreach Share ID or URL' }),
        CallSign: Type.Optional(Type.String({ description: 'Human Readable Name of the Operator - Used as the callsign in TAK' })),
        Password: Type.Optional(Type.String({ description: 'Optional: Garmin Inreach MapShare Password' })),
        CoTType: Type.Optional(Type.String({ description: 'CoT type override', default: 'a-f-G' }))
    }, {
        description: 'Inreach Share IDs to pull data from',
        display: 'table',
    })),
    'TEST_MODE': Type.Boolean({
        default: false,
        description: 'Enable test mode with simulated devices'
    }),
    'TEST_DEVICES': Type.Array(Type.Object({
        IMEI: Type.String({ description: 'Simulated device IMEI' }),
        Name: Type.String({ description: 'Device operator name' }),
        DeviceType: Type.String({ 
            description: 'Device model',
            default: 'inReach Mini',
            enum: ['inReach Mini', 'inReach Explorer', 'inReach SE+', 'inReach Messenger']
        }),
        StartLat: Type.Number({ description: 'Starting latitude' }),
        StartLon: Type.Number({ description: 'Starting longitude' }),
        MovementPattern: Type.String({
            description: 'Movement simulation pattern',
            default: 'stationary',
            enum: ['stationary', 'random_walk', 'circular', 'linear_path']
        }),
        Speed: Type.Number({ 
            description: 'Movement speed in km/h',
            default: 5
        }),
        EmergencyMode: Type.Boolean({
            description: 'Simulate emergency activation',
            default: false
        }),
        MessageInterval: Type.Number({
            description: 'Minutes between simulated messages',
            default: 10
        }),
        CoTType: Type.Optional(Type.String({ description: 'CoT type override', default: 'a-f-G' }))
    }), {
        default: [],
        description: 'Test devices configuration'
    }),
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
                    inreachEmergency: Type.Optional(Type.String()),
                    inreachDeviceId: Type.String(),
                    inreachReceive: Type.String({ format: 'date-time' }),
                })
            }
        } else {
            return Type.Object({});
        }
    }

    private generateTestKML(device: { IMEI: string; Name: string; DeviceType: string; StartLat: number; StartLon: number; MovementPattern: string; Speed: number; EmergencyMode: boolean; MessageInterval: number; CoTType?: string }, deviceState: Static<typeof EphemeralSchema>['deviceStates'][string]): string {
        const now = new Date();
        const messageId = Math.floor(Math.random() * 999999999);
        
        let lat = deviceState.currentLat;
        let lon = deviceState.currentLon;
        let velocity = 0;
        let course = 0;
        
        const lastUpdate = new Date(deviceState.lastUpdate);
        const minutesElapsed = (now.getTime() - lastUpdate.getTime()) / 60000;
        const distanceKm = (device.Speed * minutesElapsed) / 60;
        const distanceDeg = distanceKm / 111;
        
        switch (device.MovementPattern) {
            case 'random_walk': {
                if (minutesElapsed > 0) {
                    const angle = Math.random() * 2 * Math.PI;
                    lat += Math.sin(angle) * distanceDeg;
                    lon += Math.cos(angle) * distanceDeg;
                    course = (angle * 180 / Math.PI) % 360;
                }
                velocity = device.Speed;
                break;
            }
            case 'circular': {
                const radius = 0.001;
                deviceState.currentAngle = (deviceState.currentAngle || 0) + (minutesElapsed * 0.1);
                lat = device.StartLat + Math.sin(deviceState.currentAngle) * radius;
                lon = device.StartLon + Math.cos(deviceState.currentAngle) * radius;
                velocity = device.Speed;
                course = (deviceState.currentAngle * 180 / Math.PI + 90) % 360;
                break;
            }
            case 'linear_path': {
                deviceState.stepCount += minutesElapsed;
                lat = device.StartLat + deviceState.stepCount * 0.0001;
                lon = device.StartLon + deviceState.stepCount * 0.0001;
                velocity = device.Speed;
                course = 45;
                break;
            }
            default: {
                lat += (Math.random() - 0.5) * 0.00001;
                lon += (Math.random() - 0.5) * 0.00001;
                velocity = 0;
                course = 0;
            }
        }
        
        deviceState.currentLat = lat;
        deviceState.currentLon = lon;
        deviceState.lastUpdate = now.toISOString();
        
        const elevation = 100 + Math.random() * 50;
        const styleId = device.EmergencyMode ? 'style_emergency' : 'style_test';
        
        return `<?xml version="1.0" encoding="utf-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <name>Test KML Export ${now.toISOString()}</name>
    <Style id="style_emergency">
      <IconStyle>
        <Icon>
          <href>http://maps.google.com/mapfiles/kml/shapes/caution.png</href>
        </Icon>
      </IconStyle>
    </Style>
    <Style id="style_test">
      <IconStyle>
        <color>ffff5500</color>
        <Icon>
          <href>http://maps.google.com/mapfiles/kml/paddle/wht-blank.png</href>
        </Icon>
      </IconStyle>
    </Style>
    <Folder>
      <name>${device.Name}</name>
      <Placemark>
        <name>${device.Name}</name>
        <TimeStamp>
          <when>${now.toISOString()}</when>
        </TimeStamp>
        <styleUrl>#${styleId}</styleUrl>
        <ExtendedData>
          <Data name="Id">
            <value>${messageId}</value>
          </Data>
          <Data name="Name">
            <value>${device.Name}</value>
          </Data>
          <Data name="Map Display Name">
            <value>${device.Name}</value>
          </Data>
          <Data name="Device Type">
            <value>${device.DeviceType}</value>
          </Data>
          <Data name="IMEI">
            <value>${device.IMEI}</value>
          </Data>
          <Data name="Incident Id">
            <value></value>
          </Data>
          <Data name="Latitude">
            <value>${lat.toFixed(6)}</value>
          </Data>
          <Data name="Longitude">
            <value>${lon.toFixed(6)}</value>
          </Data>
          <Data name="Elevation">
            <value>${elevation.toFixed(2)} m from MSL</value>
          </Data>
          <Data name="Velocity">
            <value>${velocity.toFixed(1)} km/h</value>
          </Data>
          <Data name="Course">
            <value>${course.toFixed(2)} ° True</value>
          </Data>
          <Data name="Valid GPS Fix">
            <value>True</value>
          </Data>
          <Data name="In Emergency">
            <value>${device.EmergencyMode ? 'True' : 'False'}</value>
          </Data>
          <Data name="Text">
            <value>${device.EmergencyMode ? 'EMERGENCY - HELP NEEDED' : ''}</value>
          </Data>
          <Data name="Event">
            <value>${device.EmergencyMode ? 'SOS activated' : 'Tracking interval received.'}</value>
          </Data>
          <Data name="Time UTC">
            <value>${now.toISOString()}</value>
          </Data>
          <Data name="Device Identifier">
            <value></value>
          </Data>
          <Data name="SpatialRefSystem">
            <value>WGS84</value>
          </Data>
        </ExtendedData>
        <Point>
          <coordinates>${lon.toFixed(6)},${lat.toFixed(6)},${elevation.toFixed(2)}</coordinates>
        </Point>
      </Placemark>
    </Folder>
  </Document>
</kml>`;
    }

    async control(): Promise<void> {
        const env = await this.env(Input);

        if (env.TEST_MODE) {
            if (!env.TEST_DEVICES || env.TEST_DEVICES.length === 0) {
                console.log('TEST_MODE enabled but no TEST_DEVICES configured');
                await this.submit({ type: 'FeatureCollection', features: [] });
                return;
            }
            
            const ephemeral = await this.ephemeral(EphemeralSchema);
            if (!ephemeral.deviceStates) ephemeral.deviceStates = {};
            console.log(`TEST_MODE: Loaded ephemeral state for ${Object.keys(ephemeral.deviceStates).length} devices`);
            
            const now = new Date();
            const activeDevices = env.TEST_DEVICES.filter(device => {
                const minutesSinceEpoch = Math.floor(now.getTime() / 60000);
                return minutesSinceEpoch % device.MessageInterval === 0;
            });
            
            if (activeDevices.length === 0) {
                return;
            }
            
            console.log(`TEST_MODE: Simulating ${activeDevices.length} of ${env.TEST_DEVICES.length} devices`);
            const fc: Static<typeof InputFeatureCollection> = { type: 'FeatureCollection', features: [] };
            
            for (const device of activeDevices) {
                const deviceKey = device.IMEI;
                const isNewDevice = !ephemeral.deviceStates[deviceKey];
                
                if (isNewDevice) {
                    ephemeral.deviceStates[deviceKey] = {
                        currentLat: device.StartLat,
                        currentLon: device.StartLon,
                        lastUpdate: now.toISOString(),
                        stepCount: 0,
                        wasEmergency: false
                    };
                    console.log(`TEST_MODE: Initialized new device ${device.Name} (${deviceKey}) at ${device.StartLat.toFixed(6)}, ${device.StartLon.toFixed(6)}`);
                } else {
                    const state = ephemeral.deviceStates[deviceKey];
                    const lastUpdate = new Date(state.lastUpdate);
                    const minutesElapsed = (now.getTime() - lastUpdate.getTime()) / 60000;
                    console.log(`TEST_MODE: Device ${device.Name} (${deviceKey}) - Last: ${state.currentLat.toFixed(6)}, ${state.currentLon.toFixed(6)} (${minutesElapsed.toFixed(1)}min ago)`);
                }
                
                const testKML = this.generateTestKML(device, ephemeral.deviceStates[deviceKey]);
                const newState = ephemeral.deviceStates[deviceKey];
                console.log(`TEST_MODE: Device ${device.Name} moved to ${newState.currentLat.toFixed(6)}, ${newState.currentLon.toFixed(6)} (${device.MovementPattern})`);
                
                const features = await this.processKML(testKML, { ShareId: device.IMEI, CallSign: device.Name, CoTType: device.CoTType });
                features.forEach(feature => {
                    feature.properties.remarks += '\n\nNote: Simulated Device';
                });
                fc.features.push(...features);
            }
            
            await this.processAlerts(fc.features, ephemeral);
            
            await this.setEphemeral(ephemeral);
            console.log(`TEST_MODE: Saved ephemeral state for ${Object.keys(ephemeral.deviceStates).length} devices`);
            
            const emergencyCount = fc.features.filter(f => f.properties.type === 'b-a-o-tbl').length;
            const cancelCount = fc.features.filter(f => f.properties.type === 'b-a-o-can').length;
            console.log(`TEST_MODE: Generated ${fc.features.length} features with ${emergencyCount} emergency alerts and ${cancelCount} cancel alerts`);
            await this.submit(fc);
            return;
        }

        if (!env.INREACH_MAP_SHARES) throw new Error('No INREACH_MAP_SHARES Provided');
        if (!Array.isArray(env.INREACH_MAP_SHARES)) throw new Error('INREACH_MAP_SHARES must be an array');

        const obtains: Array<Promise<Static<typeof InputFeature>[]>> = [];
        for (const share of env.INREACH_MAP_SHARES) {
            obtains.push((async (share: Share): Promise<Static<typeof InputFeature>[]> => {
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

                    // Add retry logic with exponential backoff
                    let kmlres;
                    let body;
                    for (let attempt = 0; attempt < 3; attempt++) {
                        try {
                            kmlres = await fetch(url, { headers });
                            if (!kmlres.ok) {
                                throw new Error(`HTTP ${kmlres.status}: ${kmlres.statusText}`);
                            }
                            body = await kmlres.text();
                            break;
                        } catch (error) {
                            if (attempt === 2) throw error;
                            console.log(`Attempt ${attempt + 1} failed for ${share.ShareId}, retrying...`);
                            await new Promise(resolve => setTimeout(resolve, 1000 * (attempt + 1)));
                        }
                    }



                    return await this.processKML(body, share);
                } catch (err) {
                    console.error(`FEED: ${share.CallSign}: ${err.message || err}`);
                    return [];
                }
            })(share))
        }

        const fc: Static<typeof InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        }

        const results = await Promise.all(obtains);
        let totalFeatures = 0;
        
        for (const res of results) {
            if (!res || !res.length) continue;
            fc.features.push(...res);
            totalFeatures += res.length;
        }
        
        const ephemeral = await this.ephemeral(EphemeralSchema);
        if (!ephemeral.deviceStates) ephemeral.deviceStates = {};
        
        await this.processAlerts(fc.features, ephemeral);
        
        await this.setEphemeral(ephemeral);
        
        const emergencyCount = fc.features.filter(f => f.properties.type === 'b-a-o-tbl').length;
        const cancelCount = fc.features.filter(f => f.properties.type === 'b-a-o-can').length;
        console.log(`ok - submitting ${totalFeatures} total features with ${emergencyCount} emergency alerts and ${cancelCount} cancel alerts`);
        await this.submit(fc);
    }

    private async processKML(body: string, share: Share): Promise<Static<typeof InputFeature>[]> {
        const featuresmap: Map<string, Static<typeof InputFeature>> = new Map();
        const features: Static<typeof InputFeature>[] = [];

        if (!body.trim()) return features;

        let xml;
        try {
            xml = await xml2js.parseStringPromise(body);
        } catch (error) {
            throw new Error(`XML Parse Error: ${error.message}`);
        }
        
        const kmlRoot = xml.kml || xml['kml'] || Object.values(xml)[0];
        if (!kmlRoot || !kmlRoot.Document) throw new Error('XML Parse Error: Document not found');
        if (!kmlRoot.Document[0] || !kmlRoot.Document[0].Folder || !kmlRoot.Document[0].Folder[0]) return features;
        if (!kmlRoot.Document[0].Folder[0].Placemark) return features;
        
        const placemarks = kmlRoot.Document[0].Folder[0].Placemark;

        console.log(`ok - ${share.ShareId} has ${placemarks.length} locations`);
        for (const placemark of placemarks) {
            if (!placemark.Point || !placemark.Point[0]) continue;

            const coordsStr = placemark.Point[0].coordinates[0];
            if (!coordsStr || typeof coordsStr !== 'string') {
                console.warn(`Missing coordinates for ${share.ShareId}`);
                continue;
            }
            
            const coords = coordsStr.split(',').map((ele: string) => {
                const val = parseFloat(ele.trim());
                return isNaN(val) ? null : val;
            }).filter(val => val !== null);
            
            if (coords.length < 2 || coords[1] < -90 || coords[1] > 90 || coords[0] < -180 || coords[0] > 180) {
                console.warn(`Invalid coordinates for ${share.ShareId}: ${coordsStr}`);
                continue;
            }

            const extended: Record<string, string> = {};
            if (placemark.ExtendedData && placemark.ExtendedData[0] && placemark.ExtendedData[0].Data) {
                for (const ext of placemark.ExtendedData[0].Data) {
                    extended[ext.$.name] = ext.value && ext.value[0] ? ext.value[0] : '';
                }
            }
            


            let timestamp;
            try {
                if (!placemark.TimeStamp || !placemark.TimeStamp[0] || !placemark.TimeStamp[0].when || !placemark.TimeStamp[0].when[0]) {
                    console.warn(`Missing timestamp for ${share.ShareId}`);
                    continue;
                }
                timestamp = new Date(placemark.TimeStamp[0].when[0]);
                if (isNaN(timestamp.getTime())) {
                    console.warn(`Invalid timestamp for ${share.ShareId}: ${placemark.TimeStamp[0].when[0]}`);
                    continue;
                }
            } catch (error) {
                console.warn(`Timestamp parse error for ${share.ShareId}: ${error.message}`);
                continue;
            }

            if (!extended['IMEI']) {
                console.warn(`Missing IMEI for ${share.ShareId}`);
                continue;
            }
            
            const id = `inreach-${extended['IMEI']}`;
            
            const remarks = [
                `Time: ${extended['Time UTC'] || 'Unknown'}`,
                `Name: ${extended['Name'] || 'Unknown'}`,
                `Map Display Name: ${extended['Map Display Name'] || 'Unknown'}`,
                `Device: ${extended['Device Type'] || 'Unknown'}`,
                `Elevation: ${extended['Elevation'] || 'Unknown'}`,
                `Velocity: ${extended['Velocity'] || 'Unknown'}`,
                `Course: ${extended['Course'] || 'Unknown'}`,
                `Event: ${extended['Event'] || 'Unknown'}`,
                `GPS Fix: ${extended['Valid GPS Fix'] || 'Unknown'}`,
                `Emergency: ${extended['In Emergency'] || 'False'}`,
                extended['Text'] ? `Message: ${extended['Text']}` : null,
                extended['Incident Id'] ? `Incident: ${extended['Incident Id']}` : null
            ].filter(Boolean).join('\n');
            
            const feat: Static<typeof InputFeature> = {
                id,
                type: 'Feature',
                properties: {
                    type: share.CoTType || 'a-f-G',
                    course: Number(extended['Course']?.replace(/\s.*/, '') || 0),
                    speed: Number(extended['Velocity']?.replace(/\s.*/, '') || 0) * 0.277778,
                    callsign: share.CallSign,
                    time: timestamp.toISOString(),
                    start: timestamp.toISOString(),
                    remarks,
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
                        inreachEmergency: extended['In Emergency'],
                        inreachDeviceId: extended['Device Identifier'],
                        inreachReceive: timestamp.toISOString(),
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

        features.push(...Array.from(featuresmap.values()));
        console.log(`ok - ${share.ShareId} processed ${featuresmap.size} locations`);
        return features;
    }

    private async processAlerts(features: Static<typeof InputFeature>[], ephemeral: Static<typeof EphemeralSchema>): Promise<void> {
        const alertFeatures: Static<typeof InputFeature>[] = [];
        
        for (const feature of features) {
            const deviceKey = String(feature.properties.metadata.inreachIMEI);
            if (!deviceKey) continue;
            
            const isCurrentlyEmergency = feature.properties.metadata.inreachEmergency === 'True';
            const wasEmergency = ephemeral.deviceStates[deviceKey]?.wasEmergency || false;
            
            // Create separate emergency alert CoT events
            if (!wasEmergency && isCurrentlyEmergency) {
                console.log(`ALERT: Emergency started for ${feature.properties.callsign}`);
                alertFeatures.push(this.createEmergencyAlert(feature, 'b-a-o-tbl'));
            } else if (wasEmergency && !isCurrentlyEmergency) {
                console.log(`CANCEL: Emergency ended for ${feature.properties.callsign}`);
                alertFeatures.push(this.createEmergencyAlert(feature, 'b-a-o-can'));
            }
            
            // Update emergency state
            const coords = feature.geometry.coordinates as number[];
            if (!ephemeral.deviceStates[deviceKey]) {
                ephemeral.deviceStates[deviceKey] = {
                    currentLat: coords[1],
                    currentLon: coords[0],
                    lastUpdate: feature.properties.time,
                    stepCount: 0,
                    wasEmergency: isCurrentlyEmergency
                };
            } else {
                ephemeral.deviceStates[deviceKey].wasEmergency = isCurrentlyEmergency;
            }
        }
        
        // Add alert features to the main features array
        features.push(...alertFeatures);
    }
    
    private createEmergencyAlert(deviceFeature: Static<typeof InputFeature>, alertType: string): Static<typeof InputFeature> {
        const alertId = `${deviceFeature.id}-9-1-1`;
        const isCancel = alertType === 'b-a-o-can';
        
        return {
            id: alertId,
            type: 'Feature',
            properties: {
                type: alertType,
                callsign: `${deviceFeature.properties.callsign}-Alert`,
                time: deviceFeature.properties.time,
                start: deviceFeature.properties.time,
                remarks: isCancel ? 'Emergency cancelled' : `Emergency alert from ${deviceFeature.properties.callsign}`,
                links: [{
                    uid: String(deviceFeature.id),
                    relation: 'p-p',
                    type: deviceFeature.properties.type
                }],
                metadata: {
                    emergency: {
                        type: isCancel ? 'Cancel Alert' : '911 Alert',
                        cancel: isCancel ? 'true' : undefined
                    },
                    contact: {
                        callsign: `${deviceFeature.properties.callsign}-Alert`
                    }
                }
            },
            geometry: deviceFeature.geometry
        };
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
