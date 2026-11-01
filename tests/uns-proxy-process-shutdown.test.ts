import { describe, expect, it, vi } from "vitest";

import { UnsProxyProcess } from "../packages/uns-core/src/uns/uns-proxy-process.js";
import { MQTT_DRAIN_HANDLER, MQTT_COOPERATIVE_STOP_TOPIC } from "../packages/uns-core/src/runtime/mqtt-drain.js";

type ProxyStop = { stop: ReturnType<typeof vi.fn> };

function createProcessForShutdown(proxyStops: ProxyStop[], processMqttStop = vi.fn(async () => undefined)) {
  const statusMonitorStop = vi.fn();
  const publishInactiveStatus = vi.fn(async () => undefined);
  const process = Object.create(UnsProxyProcess.prototype) as any;
  process.processName = "test-process";
  process.shutdownPromise = null;
  process.statusMonitor = { stop: statusMonitorStop, publishInactiveStatus };
  process.mqttInputHandler = undefined;
  process.unsMqttProxies = proxyStops;
  process.unsApiProxies = [];
  process.lifecycleParticipants = [];
  process.processMqttProxy = {
    event: { off: vi.fn() },
    stop: processMqttStop,
  };
  return {
    process: process as UnsProxyProcess,
    processMqttStop,
    statusMonitorStop,
    publishInactiveStatus,
  };
}

describe("UnsProxyProcess shutdown", () => {
  it("removes its own signal owner after successful manual shutdown",async()=>{
    const {process:owned}=createProcessForShutdown([]);
    owned.installSignalHandlers({retainOnFailure:true});
    const handler=(owned as any).signalStopHandler;
    expect(process.listeners("SIGINT")).toContain(handler);
    await owned.shutdown();
    expect(process.listeners("SIGINT")).not.toContain(handler);
    expect(process.listeners("SIGTERM")).not.toContain(handler);
  });
  it("binds cooperative host stop to this launch and permits retry without forced exit", async()=>{
    const owned=Object.create(UnsProxyProcess.prototype)as any;
    owned.processName="independent-probe";owned.handoverReady=true;
    owned.drainForMigration=vi.fn().mockRejectedValueOnce(Error("output full")).mockResolvedValueOnce({protocol:1,state:"drained",pendingRecords:0,outputs:1});
    const prior=process.env.UNS_HANDOVER_LAUNCH_ID;process.env.UNS_HANDOVER_LAUNCH_ID="launch-one";
    const registry=globalThis as any;
    try{
      owned.enableMigrationDrain();
      owned.cooperativeStopHandler({type:"process:msg",data:{type:MQTT_COOPERATIVE_STOP_TOPIC,protocol:1,launchId:"wrong"}});
      expect(owned.drainForMigration).not.toHaveBeenCalled();
      const request={type:"process:msg",data:{type:MQTT_COOPERATIVE_STOP_TOPIC,protocol:1,launchId:"launch-one"}};
      owned.cooperativeStopHandler(request);await Promise.resolve();await Promise.resolve();
      expect(owned.drainForMigration).toHaveBeenCalledTimes(1);
      owned.cooperativeStopHandler(request);await Promise.resolve();await Promise.resolve();
      expect(owned.drainForMigration).toHaveBeenCalledTimes(2);
    }finally{
      process.off("message",owned.cooperativeStopHandler);delete registry[MQTT_DRAIN_HANDLER];
      if(prior===undefined)delete process.env.UNS_HANDOVER_LAUNCH_ID;else process.env.UNS_HANDOVER_LAUNCH_ID=prior;
    }
  });
  it("retains failed signal cleanup, coalesces concurrent signals and allows a later retry", async () => {
    const owned = Object.create(UnsProxyProcess.prototype) as any;
    owned.processName = "unrelated-service";
    let reject!: (error: Error) => void;
    owned.drainForMigration = vi.fn().mockImplementationOnce(() => new Promise((_, no) => { reject = no; }))
      .mockResolvedValueOnce({ protocol: 1, state: "drained", pendingRecords: 0, outputs: 1 });
    const callbacks: Record<string, () => void> = {};
    const on = vi.spyOn(process, "on").mockImplementation(((event: string, callback: () => void) => { callbacks[event] = callback; return process; }) as any);
    const exit = vi.spyOn(process, "exit").mockImplementation((() => {}) as any);
    try {
      owned.installSignalHandlers({ retainOnFailure: true });
      callbacks.SIGINT(); callbacks.SIGTERM();
      expect(owned.drainForMigration).toHaveBeenCalledTimes(1);
      reject(Error("broker still offline")); await Promise.resolve(); await Promise.resolve();
      expect(exit).not.toHaveBeenCalled();
      callbacks.SIGTERM();
      await vi.waitFor(() => expect(exit).toHaveBeenCalledWith(0));
      expect(owned.drainForMigration).toHaveBeenCalledTimes(2);
    } finally { on.mockRestore(); exit.mockRestore(); }
  });
  it("attempts every cleanup and propagates child drain failures", async () => {
    const drainFailure = new Error("publisher drain timed out");
    const firstProxy = {
      stop: vi.fn(async () => {
        throw drainFailure;
      }),
    };
    const secondProxy = { stop: vi.fn(async () => undefined) };
    const { process, processMqttStop, statusMonitorStop, publishInactiveStatus } = createProcessForShutdown([firstProxy, secondProxy]);

    const shutdownError = await process.shutdown().catch((error) => error);

    expect(shutdownError).toBeInstanceOf(AggregateError);
    expect((shutdownError as AggregateError).errors).toContain(drainFailure);
    expect(firstProxy.stop).toHaveBeenCalledOnce();
    expect(secondProxy.stop).toHaveBeenCalledOnce();
    expect(statusMonitorStop).toHaveBeenCalledOnce();
    expect(publishInactiveStatus).toHaveBeenCalledOnce();
    expect(processMqttStop).toHaveBeenCalledOnce();
    expect(publishInactiveStatus.mock.invocationCallOrder[0]).toBeLessThan(processMqttStop.mock.invocationCallOrder[0]);
  });

  it("is idempotent and runs cleanup only once", async () => {
    let finishStop!: () => void;
    const proxyStop = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          finishStop = resolve;
        }),
    );
    const { process, processMqttStop } = createProcessForShutdown([{ stop: proxyStop }]);

    (process as any).active = true;
    expect(process.isActive()).toBe(true);
    const firstShutdown = process.shutdown();
    expect(process.isActive()).toBe(false);
    const secondShutdown = process.shutdown();

    expect(secondShutdown).toBe(firstShutdown);
    await vi.waitFor(() => expect(proxyStop).toHaveBeenCalledOnce());
    finishStop();
    await expect(firstShutdown).resolves.toBeUndefined();
    await expect(process.shutdown()).resolves.toBeUndefined();
    expect(proxyStop).toHaveBeenCalledOnce();
    expect(processMqttStop).toHaveBeenCalledOnce();
  });
});
