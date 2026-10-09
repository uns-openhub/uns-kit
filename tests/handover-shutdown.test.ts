import { afterEach, describe, expect, it, vi } from "vitest";

import { MqttTopicBuilder } from "../packages/uns-core/src/uns-mqtt/mqtt-topic-builder.js";
import { HandoverManager } from "../packages/uns-core/src/uns/handover-manager.js";
import { runHandoverShutdown, validateHandoverShutdown } from "../packages/uns-core/src/uns/handover-shutdown.js";
import { ACTIVE_TIMEOUT, PACKAGE_INFO } from "../packages/uns-core/src/uns/process-config.js";
import { UnsProxyProcess } from "../packages/uns-core/src/uns/uns-proxy-process.js";

function deferred() {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
const topic = `uns-infra/${MqttTopicBuilder.sanitizeTopicPart(PACKAGE_INFO.name)}/${MqttTopicBuilder.sanitizeTopicPart(PACKAGE_INFO.version)}/test/handover`;
function event(type: string, peer = "target", handoverId = "migration") {
  return {
    topic,
    message: JSON.stringify({ type, handoverId }),
    packet: {
      properties: {
        responseTopic: "uns-infra/target/next/test/handover",
        userProperties: { processId: peer, processName: "test" },
      },
    },
  };
}
async function source(drain: () => Promise<void>, stop = vi.fn(async () => undefined), onRelease = vi.fn(), timeoutMs = 100) {
  vi.useFakeTimers();
  const publish = vi.fn(async () => undefined);
  const proxy = {
    instanceName: "input",
    stop,
    setPublisherActive: vi.fn(),
    setSubscriberActive: vi.fn(),
    setSubscriberPassiveAndDrainQueue: vi.fn(async () => ({ batchSize: 1, referenceHash: "ref", instanceName: "input" })),
  };
  const manager = new HandoverManager("test", "source", { publish } as any, [proxy] as any, false, true, false, undefined, {
    drain,
    onRelease,
    timeoutMs,
  });
  const exit = vi.spyOn(process, "exit").mockImplementation((() => undefined) as any);
  await vi.advanceTimersByTimeAsync(ACTIVE_TIMEOUT);
  return { manager, exit, publish, proxy, onRelease };
}

describe("application handover drain", () => {
  afterEach(() => {
    vi.clearAllTimers();
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("releases background admission before subscriber drain and waits for app work after acknowledgement", async () => {
    const app = deferred();
    const drain = vi.fn(() => app.promise);
    const { manager, exit, onRelease, proxy } = await source(drain);
    await manager.handleMqttMessage(event("handover_request"));
    expect(onRelease).toHaveBeenCalledOnce();
    expect(onRelease.mock.invocationCallOrder[0]).toBeLessThan(proxy.setSubscriberPassiveAndDrainQueue.mock.invocationCallOrder[0]);
    expect(drain).not.toHaveBeenCalled();
    const ack = manager.handleMqttMessage(event("handover_ack"));
    await vi.advanceTimersByTimeAsync(0);
    expect(drain).toHaveBeenCalledOnce();
    expect(exit).not.toHaveBeenCalled();
    app.resolve();
    await ack;
    expect(exit).toHaveBeenCalledWith(0);
  });

  it("coalesces duplicate requests and acknowledgements", async () => {
    const app = deferred();
    const drain = vi.fn(() => app.promise);
    const { manager, exit, proxy, onRelease } = await source(drain);
    await Promise.all([manager.handleMqttMessage(event("handover_request")), manager.handleMqttMessage(event("handover_request"))]);
    const first = manager.handleMqttMessage(event("handover_ack"));
    const second = manager.handleMqttMessage(event("handover_ack"));
    await vi.advanceTimersByTimeAsync(0);
    expect(drain).toHaveBeenCalledOnce();
    expect(onRelease).toHaveBeenCalledOnce();
    expect(proxy.stop).toHaveBeenCalledOnce();
    app.resolve();
    await Promise.all([first, second]);
    expect(exit).toHaveBeenCalledOnce();
  });

  it("ignores unsolicited, wrong-peer and wrong-migration acknowledgements", async () => {
    const drain = vi.fn(async () => undefined);
    const { manager, exit } = await source(drain);
    await manager.handleMqttMessage(event("handover_ack"));
    await manager.handleMqttMessage(event("handover_request"));
    await manager.handleMqttMessage(event("handover_ack", "other"));
    await manager.handleMqttMessage(event("handover_ack", "target", "other"));
    expect(drain).not.toHaveBeenCalled();
    expect(exit).not.toHaveBeenCalled();
    await manager.handleMqttMessage(event("handover_ack"));
    expect(exit).toHaveBeenCalledWith(0);
  });

  it("waits for a pending source proxy stop before running app drain", async () => {
    const mqtt = deferred();
    const drain = vi.fn(async () => undefined);
    const { manager, exit } = await source(
      drain,
      vi.fn(() => mqtt.promise),
    );
    const request = manager.handleMqttMessage(event("handover_request"));
    await vi.advanceTimersByTimeAsync(0);
    const ack = manager.handleMqttMessage(event("handover_ack"));
    await vi.advanceTimersByTimeAsync(0);
    expect(drain).not.toHaveBeenCalled();
    expect(exit).not.toHaveBeenCalled();
    mqtt.resolve();
    await Promise.all([request, ack]);
    expect(drain).toHaveBeenCalledOnce();
    expect(exit).toHaveBeenCalledWith(0);
  });

  it("exits nonzero on rejected drain without passing error details to the protocol", async () => {
    const { manager, exit, publish } = await source(async () => {
      throw new Error("secret raw topic or credential");
    });
    await manager.handleMqttMessage(event("handover_request"));
    await manager.handleMqttMessage(event("handover_ack"));
    expect(exit).toHaveBeenCalledWith(1);
    expect(JSON.stringify(publish.mock.calls)).not.toContain("secret");
  });

  it("exits nonzero at the deadline and does not report success on late completion", async () => {
    const app = deferred();
    const { manager, exit } = await source(() => app.promise);
    await manager.handleMqttMessage(event("handover_request"));
    const ack = manager.handleMqttMessage(event("handover_ack"));
    await vi.advanceTimersByTimeAsync(99);
    expect(exit).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    await ack;
    expect(exit).toHaveBeenCalledExactlyOnceWith(1);
    app.resolve();
    await vi.advanceTimersByTimeAsync(0);
    expect(exit).toHaveBeenCalledOnce();
  });

  it("does not reactivate a released source on a foreign completion message", async () => {
    const { manager, proxy } = await source(async () => undefined);
    await manager.handleMqttMessage(event("handover_request"));
    proxy.setPublisherActive.mockClear();
    proxy.setSubscriberActive.mockClear();
    await manager.handleMqttMessage(event("handover_fin"));
    await vi.advanceTimersByTimeAsync(ACTIVE_TIMEOUT);
    expect(proxy.setPublisherActive).not.toHaveBeenCalled();
    expect(proxy.setSubscriberActive).not.toHaveBeenCalled();
  });

  it("fails source preparation before publishing completion when onRelease throws", async () => {
    const { manager, publish, exit } = await source(
      async () => undefined,
      vi.fn(async () => undefined),
      vi.fn(() => {
        throw new Error("sensitive");
      }),
    );
    await manager.handleMqttMessage(event("handover_request"));
    expect(publish).not.toHaveBeenCalled();
    expect(exit).toHaveBeenCalledWith(1);
  });

  it("rejects invalid hooks before opening process MQTT connections", () => {
    for (const timeoutMs of [0, -1, NaN, 1.5, 300001]) {
      expect(() => validateHandoverShutdown({ drain: async () => undefined, timeoutMs })).toThrow();
      expect(
        () => new UnsProxyProcess("mqtt://unused", { processName: "test", handoverShutdown: { drain: async () => undefined, timeoutMs } }),
      ).toThrow();
    }
    expect(() => validateHandoverShutdown({ drain: undefined } as any)).toThrow();
    expect(() => validateHandoverShutdown({ drain: async () => undefined, onRelease: 1 } as any)).toThrow();
  });

  it("handles synchronous hook throws as a failed drain", async () => {
    expect(
      await runHandoverShutdown(() => {
        throw new Error("sensitive");
      }),
    ).toBe("failed");
  });
  it("bounds pending source proxy stop after ACK as well as the application hook", async () => {
    const mqtt = deferred();
    const drain = vi.fn(async () => undefined);
    const { manager, exit } = await source(
      drain,
      vi.fn(() => mqtt.promise),
    );
    const request = manager.handleMqttMessage(event("handover_request"));
    await vi.advanceTimersByTimeAsync(0);
    const ack = manager.handleMqttMessage(event("handover_ack"));
    await vi.advanceTimersByTimeAsync(100);
    await ack;
    expect(exit).toHaveBeenCalledExactlyOnceWith(1);
    expect(drain).not.toHaveBeenCalled();
    mqtt.resolve();
    await request;
  });

  it("fails once when proxy stop rejects while an acknowledgement is pending", async () => {
    const mqtt = deferred();
    const drain = vi.fn(async () => undefined);
    const { manager, exit } = await source(
      drain,
      vi.fn(() => mqtt.promise),
    );
    const request = manager.handleMqttMessage(event("handover_request"));
    await vi.advanceTimersByTimeAsync(0);
    const ack = manager.handleMqttMessage(event("handover_ack"));
    mqtt.reject(new Error("sensitive proxy failure"));
    await Promise.all([request, ack]);
    expect(exit).toHaveBeenCalledExactlyOnceWith(1);
    expect(drain).not.toHaveBeenCalled();
  });

  it("preserves legacy ACK compatibility when no migration id was supplied", async () => {
    const { manager, exit } = await source(async () => undefined);
    const request = event("handover_request");
    request.message = JSON.stringify({ type: "handover_request" });
    const ack = event("handover_ack");
    ack.message = JSON.stringify({ type: "handover_ack" });
    await manager.handleMqttMessage(request);
    await manager.handleMqttMessage(ack);
    expect(exit).toHaveBeenCalledExactlyOnceWith(0);
  });

  it("ignores handover requests when this process is still passive", async () => {
    const { manager, onRelease } = await source(async () => undefined);
    (manager as any).active = false;
    await manager.handleMqttMessage(event("handover_request"));
    expect(onRelease).not.toHaveBeenCalled();
  });

  it("forwards the configured hooks from UnsProxyProcess to its manager", async () => {
    vi.useFakeTimers();
    const drain = vi.fn(async () => undefined);
    const onRelease = vi.fn();
    const runtime = Object.create(UnsProxyProcess.prototype) as any;
    Object.assign(runtime, {
      processName: "test",
      processId: "source",
      unsMqttProxies: [],
      handoverShutdown: { drain, onRelease },
      processMqttProxy: { publish: vi.fn(async () => undefined), event: { on: vi.fn() } },
    });
    runtime.initHandoverManager("handover", true);
    await vi.advanceTimersByTimeAsync(ACTIVE_TIMEOUT);
    const exit = vi.spyOn(process, "exit").mockImplementation((() => undefined) as any);
    await runtime.handoverManager.handleMqttMessage(event("handover_request"));
    expect(runtime.active).toBe(false);
    await runtime.handoverManager.handleMqttMessage(event("handover_ack"));
    expect(onRelease).toHaveBeenCalledOnce();
    expect(drain).toHaveBeenCalledOnce();
    expect(exit).toHaveBeenCalledWith(0);
  });
});
