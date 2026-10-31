import { describe, expect, it, vi, afterEach } from 'vitest';
import { HandoverManager } from '../packages/uns-core/src/uns/handover-manager.js';
import { mqttHandoverStateDigest } from '../packages/uns-core/src/runtime/mqtt-handover-state.js';

afterEach(() => vi.useRealTimers());
const event = (body: object, peer = 'target') => ({ message: JSON.stringify(body), packet: {
  retain: false, properties: { responseTopic: 'reply', userProperties: { processId: peer } },
} }) as any;

function fixture(contract = 'aggregate-v1') {
  const publish = vi.fn(async () => undefined), drain = vi.fn(async () => undefined);
  let data = { rows: [1, 2, 3], window: 123 };
  const state = { contract, exportState: () => data, validateState: (v: any) => { if (!Array.isArray(v?.rows)) throw Error('bad state'); },
    importState: vi.fn((v: any) => { data = v; }) };
  const manager = new HandoverManager('test', 'source', { publish } as any, [], true, true, false, 'move-1', { drain }, true, state);
  return { manager: manager as any, publish, drain, state };
}

describe('application state uses the common handover', () => {
  it('rejects incompatible application semantics before releasing the source', async () => {
    vi.useFakeTimers();
    const { manager, drain } = fixture(); manager.active = true;
    await manager.handleHandover(event({ type: 'handover_request', protocol: 2, handoverId: 'move-1', offer: [], applicationContract: 'other-v1' }));
    expect(manager.active).toBe(true); expect(drain).not.toHaveBeenCalled();
  });
  it('freezes accepted work before exporting the state and replays the same receipt', async () => {
    vi.useFakeTimers();
    const { manager, publish, drain } = fixture(); manager.active = true;
    const request = event({ type: 'handover_request', protocol: 2, handoverId: 'move-1', offer: [], applicationContract: 'aggregate-v1' });
    await manager.handleHandover(request);
    const first = JSON.parse(publish.mock.calls[0]![1] as string);
    expect(drain).toHaveBeenCalledOnce();
    expect(first.state.application.data).toEqual({ rows: [1, 2, 3], window: 123 });
    await manager.handleHandover(request);
    expect(JSON.parse(publish.mock.calls[1]![1] as string).stateDigest).toBe(first.stateDigest);
    expect(drain).toHaveBeenCalledOnce();
    clearInterval(manager.finRetry);
  });
  it('validates then restores RAM before activating the successor', async () => {
    vi.useFakeTimers();
    const { manager, state } = fixture();
    const activate = vi.fn(async () => { expect(state.importState).toHaveBeenCalledOnce(); });
    manager.shutdownHooks.onActivate = activate;
    const snapshot = { protocol: 1 as const, proxies: [], application: { contract: 'aggregate-v1', data: { rows: [4], window: 456 } } };
    await manager.acceptPreparedState({ state: snapshot, stateDigest: mqttHandoverStateDigest(snapshot) });
    expect(manager.active).toBe(true); expect(activate).toHaveBeenCalledOnce();
  });
  it('waits for target application restoration before opening ownership', async () => {
    vi.useFakeTimers();
    const { manager, state } = fixture();
    let restored!: () => void;
    const pending = new Promise<void>(resolve => { restored = resolve; });
    state.importState.mockImplementationOnce(async () => { await pending; });
    const snapshot = { protocol: 1 as const, proxies: [], application: { contract: 'aggregate-v1', data: { rows: [4], window: 456 } } };
    const accepting = manager.acceptPreparedState({ state: snapshot, stateDigest: mqttHandoverStateDigest(snapshot) });
    await Promise.resolve(); await Promise.resolve();
    expect(manager.active).toBe(false);
    restored(); await accepting;
    expect(manager.active).toBe(true); expect(state.importState).toHaveBeenCalledOnce();
  });
  it('closes application admission if activation fails after validated import', async () => {
    vi.useFakeTimers();
    const { manager } = fixture();
    const release = vi.fn();
    manager.shutdownHooks.onActivate = async () => { throw Error('unavailable'); };
    manager.shutdownHooks.onRelease = release;
    const snapshot = { protocol: 1 as const, proxies: [], application: { contract: 'aggregate-v1', data: { rows: [], window: 0 } } };
    await expect(manager.acceptPreparedState({ state: snapshot, stateDigest: mqttHandoverStateDigest(snapshot) })).rejects.toThrow('remains passive');
    expect(manager.active).toBe(false); expect(release).toHaveBeenCalledOnce();
  });
  it('closes application admission when input activation fails after application preparation', async () => {
    vi.useFakeTimers();
    const { manager } = fixture();
    const release = vi.fn(); manager.shutdownHooks.onRelease = release;
    const proxy = { instanceName: 'input', validateHandoverState: vi.fn(), prepareHandoverResume: vi.fn(async () => undefined),
      importHandoverState: vi.fn(), setPublisherActive: vi.fn(), setPublisherPassive: vi.fn(),
      setSubscriberPassive: vi.fn(async () => undefined), activateHandoverResume: vi.fn(async () => { throw Error('worker failed'); }) };
    manager.unsMqttProxies = [proxy];
    const snapshot = { protocol: 1 as const, application: { contract: 'aggregate-v1', data: { rows: [], window: 0 } },
      proxies: [{ instanceName: 'input', compatibility: 'a'.repeat(64), values: [], sequences: [], cursor: { batchSize: 0, referenceHash: 'b'.repeat(64) } }] };
    await expect(manager.acceptPreparedState({ state: snapshot, stateDigest: mqttHandoverStateDigest(snapshot) })).rejects.toThrow('input activation failed');
    expect(manager.active).toBe(false); expect(manager.acceptedStateDigest).toBeUndefined();
    expect(release).toHaveBeenCalledOnce(); expect(proxy.setPublisherPassive).toHaveBeenCalledOnce();
    expect(proxy.setSubscriberPassive).toHaveBeenCalledWith(true);
  });
  it('leaves a successor passive when application state is invalid', async () => {
    vi.useFakeTimers();
    const { manager, state } = fixture();
    const snapshot = { protocol: 1 as const, proxies: [], application: { contract: 'aggregate-v1', data: { invalid: true } } };
    await expect(manager.acceptPreparedState({ state: snapshot, stateDigest: mqttHandoverStateDigest(snapshot) })).rejects.toThrow();
    expect(manager.active).toBe(false); expect(state.importState).not.toHaveBeenCalled();
  });
});
