import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { z } from 'zod';

// Same contract for every SDK MQTT consumer. No application/profile selectors.
export const MQTT_HANDOVER_STATE_MAX_BYTES = 1024 * 1024;
const text = z.string().min(1).max(65535).refine(v => !v.includes('\0'));
const scalar = z.union([z.number().finite(), z.string().max(65535)]);
const value = z.union([scalar, z.array(scalar).max(4096)]);
export const mqttHandoverOfferSchema = z.array(z.strictObject({
  instanceName: z.string().min(1).max(256),
  compatibility: z.string().regex(/^[a-f0-9]{64}$/),
})).max(64).refine(offer => new Set(offer.map(p => p.instanceName)).size === offer.length, 'Duplicate MQTT proxy');
export const mqttProxyHandoverStateSchema = z.strictObject({
  instanceName: z.string().min(1).max(256),
  compatibility: z.string().regex(/^[a-f0-9]{64}$/),
  values: z.array(z.strictObject({ topic: text, value, uom: z.string().max(256), timestamp: z.string().datetime() })).max(10000),
  sequences: z.array(z.tuple([text, z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER)])).max(10000),
  cursor: z.strictObject({ batchSize: z.number().int().min(0).max(100), referenceHash: z.string().regex(/^[a-f0-9]{64}$/) }),
}).superRefine((state, ctx) => {
  for (const keys of [state.values.map(v => v.topic), state.sequences.map(v => v[0])])
    if (new Set(keys).size !== keys.length) ctx.addIssue({ code: 'custom', message: 'Duplicate MQTT state key' });
});
export const mqttHandoverStateSchema = z.strictObject({
  protocol: z.literal(1),
  proxies: z.array(mqttProxyHandoverStateSchema).max(64),
}).superRefine((state, ctx) => {
  if (new Set(state.proxies.map(p => p.instanceName)).size !== state.proxies.length)
    ctx.addIssue({ code: 'custom', message: 'Duplicate MQTT proxy' });
});
export type MqttProxyHandoverState = z.infer<typeof mqttProxyHandoverStateSchema>;
export type MqttHandoverState = z.infer<typeof mqttHandoverStateSchema>;
export function parseMqttHandoverState(input: unknown): MqttHandoverState {
  const json = JSON.stringify(input);
  if (!json || Buffer.byteLength(json) > MQTT_HANDOVER_STATE_MAX_BYTES) throw Error('MQTT handover state exceeds its bound');
  return mqttHandoverStateSchema.parse(input);
}
export function mqttHandoverStateDigest(state: MqttHandoverState): string {
  return createHash('sha256').update(JSON.stringify(parseMqttHandoverState(state))).digest('hex');
}
export const mqttHandoverCapabilitySchema = z.strictObject({ protocolVersion: z.literal(2) });
export function readMqttHandoverCapability(packageRoot: string): 2 | undefined {
  const file = path.join(packageRoot, 'package.json');
  let stat: fs.Stats;
  try { stat = fs.lstatSync(file); } catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined; throw error; }
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 1048576) throw Error('Unsafe MQTT handover descriptor');
  const descriptor = JSON.parse(fs.readFileSync(file, 'utf8'))?.unsDatahub?.mqttHandover;
  if (descriptor === undefined) return undefined;
  mqttHandoverCapabilitySchema.parse(descriptor);
  return 2;
}
