import { afterEach, describe, expect, it, vi } from 'vitest';
import { createHash } from 'node:crypto';
import { HandoverManager } from '../packages/uns-core/src/uns/handover-manager.js';
import { ThrottledSubscriber } from '../packages/uns-core/src/uns-mqtt/throttled-queue.js';
import { parseMqttHandoverState, mqttHandoverStateDigest } from '../packages/uns-core/src/runtime/mqtt-handover-state.js';
import { ACTIVE_TIMEOUT } from '../packages/uns-core/src/uns/process-config.js';

const hash = (items: unknown[]) => createHash('sha256').update(JSON.stringify(items)).digest('hex');
const snapshot = (name = 'counter') => ({ instanceName: name, compatibility: hash(["semantic-v1"]), values: [{ topic: 'plant/counter-delta', value: 100, uom: 'kWh', timestamp: '2026-01-01T00:00:00.000Z' }], sequences: [['plant/counter', 7]], cursor: { batchSize: 0, referenceHash: hash([]) } });
const state = () => parseMqttHandoverState({ protocol: 1, proxies: [snapshot()] });
const event = (body: object, peer = 'old') => ({ message: JSON.stringify(body), packet: { properties: { responseTopic: 'reply', userProperties: { processId: peer } } } }) as any;
const finish = () => { const s = state(); return { type: 'handover_fin', protocol: 2, handoverId: 'move-1', state: s, stateDigest: mqttHandoverStateDigest(s) }; };
function target() {
  vi.useFakeTimers();
  const calls: string[] = [];
  const proxy = { instanceName: 'counter', validateHandoverState: vi.fn(), prepareHandoverResume: vi.fn(async () => { calls.push('prepare'); }), importHandoverState: vi.fn(() => { calls.push('import'); }), setPublisherActive: vi.fn(() => { calls.push('publisher'); }), activateHandoverResume: vi.fn(async () => { calls.push('input'); }), setSubscriberActive: vi.fn() };
  const publish = vi.fn(async () => undefined);
  const m = new HandoverManager('any-application', 'new', { publish } as any, [proxy] as any, true, true, false, 'move-1', undefined, true) as any;
  m.requestingHandover = true; m.expectedSourceId = 'old';
  return { m, proxy, publish, calls };
}
afterEach(() => { vi.clearAllTimers(); vi.useRealTimers(); });
describe('generic stateful MQTT handover', () => {
  it.each(['missing', 'contract', 'broker', 'proxy'])('keeps the source active before an incompatible %s offer', async kind => {
    vi.useFakeTimers();
    const proxy = { getHandoverCompatibility: () => ({instanceName:'counter', compatibility:snapshot().compatibility}), setSubscriberPassiveAndDrainQueue: vi.fn() };
    const m = new HandoverManager('generic-app', 'old', {publish:vi.fn()} as any, [proxy] as any, false, true, false, undefined, undefined, true) as any;
    m.active = true;
    const offer = kind === 'missing' ? undefined : [{instanceName:kind === 'proxy' ? 'other' : 'counter', compatibility:kind === 'proxy' ? snapshot().compatibility : hash([kind])}];
    await m.handleHandover(event({type:'handover_request', protocol:2, handoverId:'move-1', offer}, 'new'));
    expect(m.active).toBe(true); expect(m.sourcePeerId).toBeUndefined(); expect(proxy.setSubscriberPassiveAndDrainQueue).not.toHaveBeenCalled();
  });
  it.each([false, true])('records a source receipt only after successful drain (failure=%s)', async fail => {
    vi.useFakeTimers(); const calls: string[] = [];
    const proxy = { stop: async () => { calls.push('stop'); if (fail) throw Error('blocked'); } };
    const m = new HandoverManager('generic-app', 'old', { publish: vi.fn() } as any, [proxy] as any, false, true, false, undefined, { drain: async () => { calls.push('drain'); } }, true) as any;
    m.sourcePreparation = Promise.resolve(); m.acceptedHandoverId = 'move-1'; m.stateDigest = finish().stateDigest;
    m.exitSource = vi.fn((code: number) => calls.push('exit:' + code));
    const received: unknown[] = []; const onReceipt = (receipt: unknown) => { received.push(receipt); calls.push('receipt'); };
    process.on('uns-kit:mqtt-handover-drained' as any, onReceipt);
    try { await m.finishSourceHandover(); } finally { process.removeListener('uns-kit:mqtt-handover-drained' as any, onReceipt); }
    expect(calls).toEqual(fail ? ['drain', 'stop', 'exit:1'] : ['drain', 'stop', 'receipt', 'exit:0']);
    expect(received).toEqual(fail ? [] : [{ protocol: 2, handoverId: 'move-1', stateDigest: finish().stateDigest, pendingRecords: 0, outputs: 1 }]);
  });
  it('restores state before accepting input and re-acks lost replies without resetting the baseline', async () => {
    const { m, proxy, publish, calls } = target();
    await m.handleHandover(event(finish()));
    await m.handleHandover(event(finish()));
    expect(calls).toEqual(['prepare', 'import', 'publisher', 'input']);
    expect(proxy.importHandoverState).toHaveBeenCalledOnce();
    expect(publish).toHaveBeenCalledTimes(2);
    expect(JSON.parse(publish.mock.calls[0][1])).toMatchObject({ type: 'handover_ack', protocol: 2, stateDigest: finish().stateDigest });
  });
  it.each(['peer', 'attempt', 'protocol', 'digest', 'proxy'])('does not activate for mismatched %s', async (kind) => {
    const { m, proxy, publish } = target(); const msg = finish();
    if (kind === 'attempt') msg.handoverId = 'other';
    if (kind === 'protocol') msg.protocol = 1;
    if (kind === 'digest') msg.stateDigest = 'bad';
    if (kind === 'proxy') { msg.state.proxies[0].instanceName = 'other'; msg.stateDigest = mqttHandoverStateDigest(msg.state); }
    await m.handleHandover(event(msg, kind === 'peer' ? 'other' : 'old'));
    expect(proxy.activateHandoverResume).not.toHaveBeenCalled(); expect(publish).not.toHaveBeenCalled();
  });
  it('never self-promotes a reviewed successor after a discovery timeout', async () => {
    const { proxy } = target(); await vi.advanceTimersByTimeAsync(ACTIVE_TIMEOUT + 1);
    expect(proxy.setPublisherActive).not.toHaveBeenCalled(); expect(proxy.setSubscriberActive).not.toHaveBeenCalled();
  });
  it('blocks every input if one cursor cannot be reconciled', async () => {
    const { m, proxy, publish } = target(); proxy.prepareHandoverResume.mockRejectedValueOnce(Error('ambiguous'));
    await m.handleHandover(event(finish()));
    expect(proxy.importHandoverState).not.toHaveBeenCalled(); expect(proxy.activateHandoverResume).not.toHaveBeenCalled(); expect(publish).not.toHaveBeenCalled();
  });
  it('rejects duplicate keys, invalid times and oversized snapshots', () => {
    const a = state(); a.proxies[0].values.push(a.proxies[0].values[0]); expect(() => parseMqttHandoverState(a)).toThrow();
    const b = state(); b.proxies[0].values[0].timestamp = 'not-a-date'; expect(() => parseMqttHandoverState(b)).toThrow();
    expect(() => parseMqttHandoverState({ data: 'a'.repeat(1048577) })).toThrow(/bound/);
  });
  it('finishes accepted application work and broker flush before publishing calculation state', async () => {
    vi.useFakeTimers(); const order: string[] = []; let complete!: () => void;
    const pending = new Promise<void>(r => { complete = r; });
    const proxy = { instanceName: 'counter', getHandoverCompatibility: () => ({instanceName:'counter', compatibility:snapshot().compatibility}), setSubscriberPassiveAndDrainQueue: async () => { order.push('freeze'); return snapshot().cursor; }, flush: async () => { order.push('flush'); }, exportHandoverState: () => { order.push('snapshot'); return snapshot(); }, stop: vi.fn() };
    const publish = vi.fn(async () => { order.push('fin'); });
    const m = new HandoverManager('unrelated-sensor', 'old', { publish } as any, [proxy] as any, false, true, false, undefined, { drain: async () => { await pending; order.push('app'); } }, true) as any;
    m.active = true;
    const preparation = m.handleHandover(event({ type: 'handover_request', protocol: 2, handoverId: 'move-1', offer: [{instanceName:'counter', compatibility:snapshot().compatibility}] }, 'new'));
    await Promise.resolve(); expect(publish).not.toHaveBeenCalled(); complete(); await preparation;
    expect(order).toEqual(['freeze', 'app', 'flush', 'snapshot', 'fin']); expect(proxy.stop).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1000); expect(publish).toHaveBeenCalledTimes(2);
  });
});
describe('strict input boundary', () => {
  it('includes topic in the boundary and refuses missing or ambiguous overlap without consuming input', async () => {
    const seen: string[] = [];
    const q = new ThrottledSubscriber(0, async (topic, message) => { seen.push(topic + message); }, false, undefined, 'sensor', false);
    q.enqueue('a', 'same'); q.enqueue('b', 'same'); q.enqueue('b', 'next');
    q.prepareResume({ batchSize: 1, referenceHash: hash([{ topic: 'b', message: 'same' }]) }); q.activatePreparedResume();
    await new Promise(r => setTimeout(r, 10)); expect(seen).toEqual(['bnext']);
    const blocked = new ThrottledSubscriber(0, async () => {}, false, undefined, 'other', false);
    blocked.enqueue('a', 'same'); blocked.enqueue('a', 'same');
    expect(() => blocked.prepareResume({ batchSize: 1, referenceHash: hash([{ topic: 'a', message: 'same' }]) })).toThrow(/ambiguous/);
    expect(() => blocked.prepareResume({ batchSize: 1, referenceHash: hash([{ topic: 'absent', message: 'same' }]) })).toThrow(/missing/);
    expect(blocked.getState()).toBe(false); expect(() => blocked.activatePreparedResume()).toThrow();
  });
});
