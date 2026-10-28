import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { FakeWorker } = vi.hoisted(() => {
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

  class MockWorker extends MiniEmitter {
    static instances: MockWorker[] = [];
    public readonly postedMessages: any[] = [];
    public readonly terminate = vi.fn(async () => 0);

    constructor(_filename: string, _options: unknown) {
      super();
      MockWorker.instances.push(this);
    }

    postMessage(message: any): void {
      this.postedMessages.push(message);
    }
  }

  return { FakeWorker: MockWorker };
});

vi.mock("worker_threads", () => ({
  Worker: FakeWorker,
}));

import UnsMqttProxy, { MessageMode } from "../packages/uns-core/src/uns-mqtt/uns-mqtt-proxy.js";
import type { IMqttAttributeMessage, IMqttPublishRequest, ITopicObject } from "../packages/uns-core/src/uns/uns-interfaces.js";

const getLastWorker = (): InstanceType<typeof FakeWorker> => {
  const worker = FakeWorker.instances.at(-1);
  if (!worker) {
    throw new Error("Expected fake worker instance to exist.");
  }
  return worker;
};

describe("UnsMqttProxy publish drain contract", () => {
  afterEach(async () => {
    const workers = [...FakeWorker.instances];
    FakeWorker.instances.length = 0;
    for (const worker of workers) {
      worker.removeAllListeners();
    }
  });

  beforeEach(() => {
    FakeWorker.instances.length = 0;
  });

  it("exposes broker subscription readiness and clears it on disconnect or worker exit",async()=>{
    const proxy=new UnsMqttProxy("localhost","readiness","one"),worker=getLastWorker();
    expect(proxy.getInputReadiness()).toBe(false);expect(proxy.getConnectionReadiness()).toBe(false);
    worker.emit("message",{command:"mqttProxyStatus",event:"mqtt-connected",value:1});expect(proxy.getConnectionReadiness()).toBe(true);
    worker.emit("message",{command:"mqttProxyStatus",event:"mqtt-subscriptions-ready",value:1});expect(proxy.getInputReadiness()).toBe(true);
    worker.emit("message",{command:"mqttProxyStatus",event:"mqtt-subscriptions-ready",value:0});expect(proxy.getInputReadiness()).toBe(false);
    worker.emit("message",{command:"mqttProxyStatus",event:"mqtt-subscriptions-ready",value:1});worker.emit("exit",1);expect(proxy.getInputReadiness()).toBe(false);expect(proxy.getConnectionReadiness()).toBe(false);
    await proxy.stop({drain:false});
  });

  it("rejects late raw and structured publication after the migration admission fence", async () => {
    const proxy = new UnsMqttProxy("localhost", "inspection", "one");
    const worker = getLastWorker();
    proxy.sealPublishes();
    await expect(proxy.publishMessage("synthetic/late", "value")).rejects.toThrow("sealed");
    await expect(proxy.publishMqttMessage(null)).rejects.toThrow("sealed");
    expect(worker.postedMessages.some(message => message.command === "enqueue")).toBe(false);
    await proxy.stop({ drain: false });
  });

  it("rejects publishMessage immediately when the worker reports queue-full at enqueue time", async () => {
    const proxy = new UnsMqttProxy("localhost", "proc", "instance", {
      maxPendingPublishes: 2,
    });
    const worker = getLastWorker();

    const publishPromise = proxy.publishMessage("raw/data", "third");
    const enqueue = worker.postedMessages.at(-1);

    worker.emit("message", {
      command: "enqueueAccepted",
      id: enqueue.id,
      status: "error",
      error: "proc-instance - Publisher queue is full (2).",
    });

    await expect(publishPromise).rejects.toThrow("Publisher queue is full");
    await proxy.stop({ drain: false });
  });

  it("bounds pending publish admission before messages enter the worker channel", async () => {
    const proxy = new UnsMqttProxy("localhost", "proc", "instance", {
      maxPendingPublishes: 2,
    });
    const worker = getLastWorker();

    const first = proxy.publishMessage("raw/data", "first");
    const second = proxy.publishMessage("raw/data", "second");
    const third = proxy.publishMessage("raw/data", "third");

    await expect(third).rejects.toThrow("Publisher queue is full (2)");
    expect(worker.postedMessages.filter((message) => message.command === "enqueue")).toHaveLength(2);

    const firstRejection = expect(first).rejects.toThrow("UnsProxy has been stopped");
    const secondRejection = expect(second).rejects.toThrow("UnsProxy has been stopped");
    await proxy.stop({ drain: false });
    await Promise.all([firstRejection, secondRejection]);
  });

  it("flush waits for delayed async publish completions", async () => {
    const proxy = new UnsMqttProxy("localhost", "proc", "instance");
    const worker = getLastWorker();

    const publishPromise = proxy.publishMessage("raw/data", "payload");
    const enqueue = worker.postedMessages.at(-1);
    worker.emit("message", { command: "enqueueAccepted", id: enqueue.id, status: "accepted" });
    await publishPromise;

    const flushPromise = proxy.flush(1000);
    let settled = false;
    void flushPromise.then(() => {
      settled = true;
    });
    await Promise.resolve();
    expect(settled).toBe(false);

    worker.emit("message", { command: "publishResult", id: enqueue.id, status: "success", topic: "raw/data" });
    await expect(flushPromise).resolves.toBeUndefined();
    await proxy.stop({ drain: false });
  });

  it("stop drains accepted publishes before terminating the worker by default", async () => {
    const proxy = new UnsMqttProxy("localhost", "proc", "instance");
    const worker = getLastWorker();

    const publishPromise = proxy.publishMessage("raw/data", "payload");
    const enqueue = worker.postedMessages.at(-1);
    worker.emit("message", { command: "enqueueAccepted", id: enqueue.id, status: "accepted" });
    await publishPromise;

    const stopPromise = proxy.stop();
    await Promise.resolve();
    expect(worker.terminate).not.toHaveBeenCalled();

    worker.emit("message", { command: "publishResult", id: enqueue.id, status: "success", topic: "raw/data" });
    await expect(stopPromise).resolves.toBeUndefined();
    expect(worker.terminate).toHaveBeenCalledOnce();
  });

  it("is idempotent and terminates the worker only once", async () => {
    const proxy = new UnsMqttProxy("localhost", "proc", "instance");
    const worker = getLastWorker();

    const firstStop = proxy.stop({ drain: false });
    const secondStop = proxy.stop();

    expect(secondStop).toBe(firstStop);
    await expect(firstStop).resolves.toBeUndefined();
    await expect(proxy.stop()).resolves.toBeUndefined();
    expect(worker.terminate).toHaveBeenCalledOnce();
  });

  it("rejects new publishes as soon as stop starts", async () => {
    const proxy = new UnsMqttProxy("localhost", "proc", "instance");
    const worker = getLastWorker();
    let finishTerminate!: (exitCode: number) => void;
    worker.terminate.mockImplementationOnce(() => new Promise<number>((resolve) => {
      finishTerminate = resolve;
    }));

    const stopPromise = proxy.stop({ drain: false });
    await vi.waitFor(() => expect(worker.terminate).toHaveBeenCalledOnce());
    const enqueueCount = worker.postedMessages.length;

    await expect(proxy.publishMessage("raw/data", "too-late")).rejects.toThrow("MQTT proxy is stopping");
    expect(worker.postedMessages).toHaveLength(enqueueCount);

    finishTerminate(1);
    await expect(stopPromise).resolves.toBeUndefined();
  });

  it("does not treat the exit from intentional worker termination as a failure", async () => {
    const proxy = new UnsMqttProxy("localhost", "proc", "instance");
    const worker = getLastWorker();
    worker.terminate.mockImplementationOnce(async () => {
      worker.emit("exit", 1);
      return 1;
    });

    await expect(proxy.stop({ drain: false })).resolves.toBeUndefined();
    await expect(proxy.flush()).resolves.toBeUndefined();
  });

  it("emits async proxy error events for broker publish failure after acceptance and still drains", async () => {
    const proxy = new UnsMqttProxy("localhost", "proc", "instance");
    const worker = getLastWorker();
    const errorHandler = vi.fn();
    proxy.event.on("error", errorHandler);

    const publishPromise = proxy.publishMessage("raw/data", "payload");
    const enqueue = worker.postedMessages.at(-1);
    worker.emit("message", { command: "enqueueAccepted", id: enqueue.id, status: "accepted" });
    await publishPromise;

    const flushPromise = proxy.flush(1000);
    worker.emit("message", {
      command: "publishResult",
      id: enqueue.id,
      status: "error",
      topic: "raw/data",
      error: "broker rejected publish",
    });

    await expect(flushPromise).resolves.toBeUndefined();
    expect(errorHandler).toHaveBeenCalledWith({ code: 0, message: "broker rejected publish" });
    await proxy.stop({ drain: false });
  });

  it("does not hang flush when the worker exits while publishes are still pending", async () => {
    const proxy = new UnsMqttProxy("localhost", "proc", "instance");
    const worker = getLastWorker();

    const publishPromise = proxy.publishMessage("raw/data", "payload");
    const enqueue = worker.postedMessages.at(-1);
    worker.emit("message", { command: "enqueueAccepted", id: enqueue.id, status: "accepted" });
    await publishPromise;

    const flushPromise = proxy.flush(1000);
    worker.emit("exit", 1);

    await expect(flushPromise).rejects.toThrow("MQTT worker exited unexpectedly with code 1");
    await proxy.stop({ drain: false }).catch(() => undefined);
  });

  it("propagates handover drain failures instead of leaving the handover pending", async () => {
    const proxy = new UnsMqttProxy("localhost", "proc", "instance");
    vi.spyOn(proxy, "setSubscriberPassive").mockResolvedValue({
      command: "handover_subscriber",
      batchSize: 0,
      referenceHash: "",
      instanceName: "instance",
    });
    vi.spyOn(proxy, "flush").mockRejectedValue(new Error("handover drain failed"));

    await expect(proxy.setSubscriberPassiveAndDrainQueue()).rejects.toThrow("handover drain failed");
    await proxy.stop({ drain: false });
  });
});

describe("generated numeric delta semantics", () => {
  beforeEach(() => { FakeWorker.instances.length = 0; });

  const setup = () => {
    const proxy = new UnsMqttProxy("localhost", "delta-test", "output");
    const worker = getLastWorker();
    const originalPost = worker.postMessage.bind(worker);
    worker.postMessage = message => {
      originalPost(message);
      if (message.command === "enqueue") queueMicrotask(() => {
        worker.emit("message", { command: "enqueueAccepted", id: message.id, status: "accepted" });
        worker.emit("message", { command: "publishResult", id: message.id, status: "success" });
      });
    };
    let metadata: ITopicObject[] = [];
    proxy.event.on("unsProxyProducedTopics", event => { metadata = event.producedTopics; });
    return {
      proxy,
      metadata: () => metadata,
      packets: () => worker.postedMessages.filter(m => m.command === "enqueue").map(m => ({ topic: m.topic, packet: JSON.parse(m.message) })),
    };
  };

  const reading = (value: number | string, seconds = 0): IMqttPublishRequest & { attributes: IMqttAttributeMessage[] } => ({
    topic: "plant/site/", asset: "meter", objectType: "energy-meter", objectId: "main",
    attributes: [{
      attribute: "energy-total", attributeNeedsPersistence: true,
      valueType: typeof value === "number" ? "number" : "string",
      presentationKind: "counter", defaultAggregation: "last", counterResetPolicy: "new-value",
      validityMode: "interval", expectedIntervalMs: 60000,
      data: { time: new Date(Date.UTC(2026, 9, 13, 0, 0, seconds)).toISOString(), value, uom: "kWh", dataGroup: "meters" },
    }],
  });

  it.each([MessageMode.Both, MessageMode.Delta])("separates delta metadata and preserves calculated values in %s mode", async mode => {
    const test = setup();
    try {
      await test.proxy.publishMqttMessage(reading(100), mode);
      await test.proxy.publishMqttMessage(reading(117, 5), mode);
      const delta = test.metadata().find(m => m.attribute === "energy-total-delta");
      expect(delta).toMatchObject({ presentationKind: "delta", defaultAggregation: "sum", valueType: "number", attributeNeedsPersistence: true, validityMode: "interval", expectedIntervalMs: 60000, dataGroup: "meters" });
      expect(delta).not.toHaveProperty("counterResetPolicy");
      const packets = test.packets();
      const values = packets.filter(p => p.topic.endsWith("-delta")).map(p => p.packet.message.data.value);
      expect(values).toEqual([17]);
      const packet = packets.find(p => p.topic.endsWith("-delta"))!.packet;
      expect(packet.message.data).toMatchObject({ time: "2026-10-13T00:00:05.000Z", value: 17, uom: "kWh", dataGroup: "meters" });
      expect(packet.interval).toBe(5000);
      if (mode === MessageMode.Both) {
        expect(test.metadata().find(m => m.attribute === "energy-total")).toMatchObject({ presentationKind: "counter", defaultAggregation: "last", counterResetPolicy: "new-value" });
        expect(packets.filter(p => !p.topic.endsWith("-delta")).map(p => p.packet.message.data.value)).toEqual([100, 117]);
      } else {
        expect(test.metadata()).toHaveLength(1);
      }
    } finally { await test.proxy.stop({ drain: false }); }
  });

  it("preserves zero increments, legacy zero-reading suppression and resets", async () => {
    const test = setup();
    try {
      for (const [i, value] of [100, 100, 0, 7, 3].entries()) {
        await test.proxy.publishMqttMessage(reading(value, i * 5), MessageMode.Both);
      }
      expect(test.packets().filter(p => p.topic.endsWith("-delta")).map(p => p.packet.message.data.value)).toEqual([0, 7, 3]);
      expect(test.packets().filter(p => !p.topic.endsWith("-delta")).map(p => p.packet.message.data.value)).toEqual([100, 100, 0, 7, 3]);
    } finally { await test.proxy.stop({ drain: false }); }
  });

  it("does not change a raw interval publication or invent a delta path", async () => {
    const test = setup();
    try {
      const interval = reading(13);
      interval.attributes[0]!.presentationKind = "chart";
      interval.attributes[0]!.defaultAggregation = "sum";
      await test.proxy.publishMqttMessage(interval, MessageMode.Raw);
      expect(test.metadata()).toHaveLength(1);
      expect(test.metadata()[0]).toMatchObject({ presentationKind: "chart", defaultAggregation: "sum" });
      expect(test.packets()[0]!.packet.message.data.value).toBe(13);
    } finally { await test.proxy.stop({ drain: false }); }
  });

  it("does not impose sum/delta presentation on non-numeric data", async () => {
    const test = setup();
    try {
      await test.proxy.publishMqttMessage(reading("ready"), MessageMode.Both);
      expect(test.metadata().find(m => m.attribute === "energy-total-delta")).toMatchObject({ valueType: "string", presentationKind: "counter", defaultAggregation: "last" });
    } finally { await test.proxy.stop({ drain: false }); }
  });
});


describe("UnsMqttProxy opt-in output queue controls", () => {
  beforeEach(() => { FakeWorker.instances.length = 0; });
  afterEach(() => { for (const w of FakeWorker.instances) w.removeAllListeners(); FakeWorker.instances.length = 0; });
  const proxyWithQueue = () => new UnsMqttProxy("localhost", "camera-service", "output", {
    publishQueue: { mode: "durable", directory: "/private/tmp/queue-controls", maxRecords: 2, maxBytes: 1024 },
  });
  it("flush asks the worker to drain recovered work even with no new local admissions", async () => {
    const proxy = proxyWithQueue(), worker = getLastWorker();
    const flushed = proxy.flush(1000), request = worker.postedMessages.at(-1);
    expect(request.command).toBe("flushPublishes");
    worker.emit("message", { command: "publishQueueReply", id: request.id, status: "success" });
    await flushed;
  });
  it("does not claim recovered records drained when the worker reports a retained backlog", async () => {
    const proxy = proxyWithQueue(), worker = getLastWorker();
    const flushed = proxy.flush(1000), request = worker.postedMessages.at(-1);
    worker.emit("message", { command: "publishQueueReply", id: request.id, status: "error", error: "pending work retained" });
    await expect(flushed).rejects.toThrow("retained");
  });
  it("preserves its worker when a close cannot persist buffered work, and permits retry", async () => {
    const proxy = proxyWithQueue(), worker = getLastWorker();
    const first = proxy.stop({ drain: false, timeoutMs: 1000 });
    await vi.waitFor(() => expect(worker.postedMessages.at(-1)?.command).toBe("closePublisher"));
    worker.emit("message", { command: "publishQueueReply", id: worker.postedMessages.at(-1).id, status: "error", error: "disk full; worker retained" });
    await expect(first).rejects.toThrow("worker retained"); expect(worker.terminate).not.toHaveBeenCalled();
    const second = proxy.stop({ drain: false, timeoutMs: 1000 });
    await vi.waitFor(() => expect(worker.postedMessages.filter(m => m.command === "closePublisher")).toHaveLength(2));
    worker.emit("message", { command: "publishQueueReply", id: worker.postedMessages.at(-1).id, status: "success" });
    await second; expect(worker.terminate).toHaveBeenCalledOnce();
  });
  it("bounds pre-worker admission when no legacy maxPendingPublishes was selected", async () => {
    const proxy = proxyWithQueue(), worker = getLastWorker();
    const one = proxy.publishMessage("a", "1"), two = proxy.publishMessage("a", "2");
    const rejectedOne = expect(one).rejects.toThrow(), rejectedTwo = expect(two).rejects.toThrow();
    await expect(proxy.publishMessage("a", "3")).rejects.toThrow("full");
    worker.emit("error", new Error("test worker exited")); await Promise.all([rejectedOne,rejectedTwo]);
  });
  it("reports only cached operator-safe output queue metadata", () => {
    const proxy = proxyWithQueue(), worker = getLastWorker(); expect(proxy.getPublishQueueStatus()).toBeUndefined();
    const status = { mode: "durable", state: "pending", pendingRecords: 2, pendingBytes: 30, memoryRecords: 0,
      diskRecords: 2, inFlight: false, retries: 0, delivered: 0, lastError: null };
    worker.emit("message", { command: "publishQueueStatus", status }); expect(proxy.getPublishQueueStatus()).toEqual(status);
  });
});
