import { beforeEach, describe, expect, it, vi } from "vitest";

const { parentPortMock, MqttProxyMock } = vi.hoisted(() => {
  class MiniEmitter {
    private listeners = new Map<string, Array<(...args: any[]) => void>>();

    on(eventName: string, handler: (...args: any[]) => void): this {
      const handlers = this.listeners.get(eventName) ?? [];
      handlers.push(handler);
      this.listeners.set(eventName, handlers);
      return this;
    }

    emit(eventName: string, ...args: any[]): boolean {
      const handlers = this.listeners.get(eventName) ?? [];
      for (const handler of handlers) {
        handler(...args);
      }
      return handlers.length > 0;
    }

    removeAllListeners(): this {
      this.listeners.clear();
      return this;
    }
  }

  const parentPort = new MiniEmitter() as MiniEmitter & {
    postMessage: ReturnType<typeof vi.fn>;
  };
  parentPort.postMessage = vi.fn();

  class MockMqttProxy {
    public event = new MiniEmitter();
    public start = vi.fn();
    public publish = vi.fn(async () => undefined);
    public subscribeAsync = vi.fn();
    public unsubscribeAsync = vi.fn();
    public pauseSubscriptions = vi.fn(async () => undefined);
  }

  return {
    parentPortMock: parentPort,
    MqttProxyMock: MockMqttProxy,
  };
});

vi.mock("worker_threads", () => ({
  parentPort: parentPortMock,
}));

vi.mock("../packages/uns-core/src/uns-mqtt/mqtt-proxy.js", () => ({
  default: MqttProxyMock,
}));

import { MqttWorker } from "../packages/uns-core/src/uns-mqtt/mqtt-worker.js";

describe("MqttWorker publish acceptance", () => {
  it("keeps input in flight until its matching application acknowledgement", async () => {
    const worker = new MqttWorker({ mqttHost: "localhost", instanceNameWithSuffix: "unrelated-sensor",
      publisherActive: true, subscriberActive: true, managedInput: true });
    const subscriber = (worker as any).subscriber;
    subscriber.enqueue("local/in", "one"); subscriber.enqueue("local/in", "two");
    const inputs = () => parentPortMock.postMessage.mock.calls.map(c => c[0]).filter(m => m.command === "input");
    await vi.waitFor(() => expect(inputs()).toHaveLength(1));
    expect(subscriber.lastProcessedItems).toHaveLength(0);
    parentPortMock.emit("message", { command: "inputComplete", id: "unrelated", status: "success" });
    expect(inputs()).toHaveLength(1);
    parentPortMock.emit("message", { command: "inputComplete", id: inputs()[0].id, status: "success" });
    await vi.waitFor(() => expect(inputs()).toHaveLength(2));
    expect(subscriber.lastProcessedItems.map((x: any) => x.message)).toEqual(["one"]);
    parentPortMock.emit("message", { command: "inputComplete", id: inputs()[1].id, status: "error" });
    await vi.waitFor(() => expect(subscriber.inputFailed).toBe(true));
    expect(subscriber.queue.map((x: any) => x.message)).toEqual(["two"]);
    expect(() => subscriber.becomeActive({ batchSize: 0 })).toThrow("cannot be retried");
    parentPortMock.emit("message", { command: "setSubscriberActive", batchSize: 0 });
    expect(parentPortMock.postMessage).toHaveBeenCalledWith({ command: "publishQueueControlError", error: "MQTT input activation blocked; retained input requires review" });
    expect(subscriber.queue.map((x: any) => x.message)).toEqual(["two"]);
    await expect(subscriber.drainOwnedInput()).rejects.toThrow("retained input");
  });

  it("ordinary stop drains accepted owned input but never activates a passive shadow", async () => {
    for (const active of [true, false]) {
      parentPortMock.removeAllListeners(); parentPortMock.postMessage.mockReset();
      const worker = new MqttWorker({ mqttHost: "localhost", instanceNameWithSuffix: "other-pipeline",
        publisherActive: active, subscriberActive: active, managedInput: true });
      const subscriber = (worker as any).subscriber;
      subscriber.enqueue("local/in", "retained");
      parentPortMock.emit("message", { command: "drainSubscriberInput", id: "stop" });
      await Promise.resolve();
      if (active) {
        expect(parentPortMock.postMessage.mock.calls.some(c => c[0].command === "publishQueueReply")).toBe(false);
        const input = parentPortMock.postMessage.mock.calls.find(c => c[0].command === "input")![0];
        parentPortMock.emit("message", { command: "inputComplete", id: input.id, status: "success" });
      }
      await vi.waitFor(() => expect(parentPortMock.postMessage).toHaveBeenCalledWith({ command: "publishQueueReply", id: "stop", status: "success" }));
      expect(subscriber.queue).toHaveLength(active ? 0 : 1);
      expect(subscriber.getState()).toBe(false);
    }
  });
  beforeEach(() => {
    parentPortMock.removeAllListeners();
    parentPortMock.postMessage.mockReset();
  });

  it("posts enqueueAccepted only after local queue acceptance succeeds", async () => {
    const worker = new MqttWorker({
      mqttHost: "localhost",
      instanceNameWithSuffix: "test-worker",
      publisherActive: true,
      subscriberActive: true,
    });

    (worker as any).publisher = {
      enqueueOrThrow: vi.fn(() => Promise.resolve()),
      becomeActive: vi.fn(),
      becomePassive: vi.fn(),
      getState: vi.fn(() => true),
    };

    parentPortMock.emit("message", {
      command: "enqueue",
      id: "accepted-1",
      topic: "raw/data",
      message: "payload",
    });

    await Promise.resolve();

    expect(parentPortMock.postMessage.mock.calls).toEqual([
      [{ command: "enqueueAccepted", id: "accepted-1", status: "accepted" }],
      [{ command: "publishResult", id: "accepted-1", status: "success", topic: "raw/data" }],
    ]);
  });

  it("reports queue-full without posting enqueueAccepted success first", async () => {
    const worker = new MqttWorker({
      mqttHost: "localhost",
      instanceNameWithSuffix: "test-worker",
      publisherActive: true,
      subscriberActive: true,
    });

    (worker as any).publisher = {
      enqueueOrThrow: vi.fn(() => {
        throw new Error("test-worker - Publisher queue is full (2).");
      }),
      becomeActive: vi.fn(),
      becomePassive: vi.fn(),
      getState: vi.fn(() => true),
    };

    parentPortMock.emit("message", {
      command: "enqueue",
      id: "rejected-1",
      topic: "raw/data",
      message: "payload",
    });

    expect(parentPortMock.postMessage.mock.calls).toEqual([
      [{
        command: "enqueueAccepted",
        id: "rejected-1",
        status: "error",
        error: "test-worker - Publisher queue is full (2).",
      }],
    ]);
  });
});
