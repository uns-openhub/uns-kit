import { describe, expect, it } from 'vitest';
import { ProcessWork } from '../packages/uns-core/src/runtime/process-work.js';
import { createApplicationHandoverContract, cloneApplicationHandoverState, validateApplicationHandoverContract } from '../packages/uns-core/src/runtime/application-handover-state.js';

describe('application work boundary', () => {
  it('waits for accepted work after background admission has closed', async () => {
    let finish!: () => void;
    const accepted = new ProcessWork();
    const work = accepted.track(new Promise<void>(resolve => { finish = resolve; }));
    accepted.pause();
    let drained = false;
    const drain = accepted.drain().then(() => { drained = true; });
    await Promise.resolve();
    expect(drained).toBe(false);
    expect(await accepted.run(async () => 'unexpected')).toBeUndefined();
    finish(); await work; await drain;
    expect(drained).toBe(true);
  });
  it('latches a rejected publish even after reconnect and repeated drain', async () => {
    const accepted = new ProcessWork();
    await expect(accepted.track(Promise.reject(Error('failure')))).rejects.toThrow();
    await accepted.track(Promise.resolve());
    await expect(accepted.drain()).rejects.toThrow('unconfirmed');
    await expect(accepted.drain()).rejects.toThrow('unconfirmed');
  });
  it('does not start a poller on a standby owner', async () => {
    let calls = 0;
    const accepted = new ProcessWork(() => false);
    await accepted.run(async () => { calls++; });
    expect(calls).toBe(0);
  });
});

describe('bounded application RAM state', () => {
  it('copies an unrelated aggregation and workflow state without sharing references', () => {
    for (const state of [{ buffer: [{ at: 123, value: 0 }] }, { session: 'a', conditions: [true, null], uom: 'kWh' }]) {
      const copy = cloneApplicationHandoverState(state);
      expect(copy).toEqual(state); expect(copy).not.toBe(state);
    }
  });
  it('rejects executable, cyclic, oversized and non-finite state', () => {
    const cyclic: any = {}; cyclic.self = cyclic;
    for (const state of [cyclic, { n: NaN }, { value: undefined }, new Date(), { data: 'x'.repeat(800000) }, { run: () => 1 }])
      expect(() => cloneApplicationHandoverState(state)).toThrow();
  });
  it('validates the declared semantic contract before runtime connections', () => {
    expect(() => validateApplicationHandoverContract({ contract: '', exportState: () => ({}), validateState() {}, importState() {} })).toThrow();
  });
});

// Startup must not spend its ownership timeout before the application is prepared.
import { UnsProxyProcess } from '../packages/uns-core/src/uns/uns-proxy-process.js';
import { MQTT_DRAIN_HANDLER } from '../packages/uns-core/src/runtime/mqtt-drain.js';
import { vi } from 'vitest';

describe('deferred process readiness', () => {
  it('initializes one complete owner only after handlers and participants are registered', async () => {
    const runtime = Object.create(UnsProxyProcess.prototype) as any;
    Object.assign(runtime, { handoverReady: false, statefulHandover: true,
      waitForProcessConnection: async () => undefined, markReady: vi.fn(),
      initHandoverManager: vi.fn(() => { runtime.handoverManager = {}; }) });
    await runtime.prepareHandover('handover', true);
    expect(runtime.initHandoverManager).not.toHaveBeenCalled();
    const registry = globalThis as any;
    const prior = registry[MQTT_DRAIN_HANDLER];
    delete registry[MQTT_DRAIN_HANDLER];
    try {
      runtime.enableMigrationDrain();
      expect(runtime.initHandoverManager).toHaveBeenCalledExactlyOnceWith('handover', true, undefined);
      expect(runtime.markReady).toHaveBeenCalledOnce();
    } finally {
      delete registry[MQTT_DRAIN_HANDLER];
      if (prior) registry[MQTT_DRAIN_HANDLER] = prior;
    }
  });
  it('rejects conflicting per-proxy ownership settings before opening an owner', async () => {
    const runtime = Object.create(UnsProxyProcess.prototype) as any;
    Object.assign(runtime, { handoverReady: false, waitForProcessConnection: async () => undefined });
    await runtime.prepareHandover('handover', true);
    await expect(runtime.prepareHandover('force', false)).rejects.toThrow('same handover configuration');
  });
});


describe('business-resource compatibility', () => {
  it('is independent of object property order and refuses a changed aggregation window', () => {
    const a = createApplicationHandoverContract('window-v1', { unit: 'kWh', windowMs: 500 });
    expect(a).toBe(createApplicationHandoverContract('window-v1', { windowMs: 500, unit: 'kWh' }));
    expect(a).not.toBe(createApplicationHandoverContract('window-v1', { unit: 'kWh', windowMs: 1000 }));
    expect(a.length).toBeLessThanOrEqual(128);
  });
});
