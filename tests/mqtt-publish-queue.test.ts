import { chmodSync, mkdtempSync, realpathSync, rmSync, symlinkSync } from "node:fs";
import { createRequire } from "node:module";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import { mqttChannelParameters, resolveMqttChannel } from "../packages/uns-core/src/uns-config/mqtt-channel.js";
import { mqttChannelSchema, mqttOutputChannelSchema } from "../packages/uns-core/src/uns-config/uns-core-schema.js";
import { mqttPublishQueueSchema } from "../packages/uns-core/src/uns-mqtt/publish-queue-contract.js";
import { MqttPublishQueue } from "../packages/uns-core/src/uns-mqtt/publish-queue.js";

const roots: string[] = [];
const queues: MqttPublishQueue[] = [];
const directory = () => {
  const root = realpathSync(mkdtempSync(path.join(os.tmpdir(), "uns-mqtt-outbox-")));
  roots.push(root);
  return root;
};
const fixture = (
  options: Record<string, unknown> = {},
  send: (value: any) => Promise<void> = async () => {},
  owner = "unrelated/meter-a",
  active = false,
) => {
  const dir = directory();
  const config = { mode: "durable" as const, directory: dir, retryIntervalMs: 10, minFreeBytes: 0, ...options };
  const queue = new MqttPublishQueue(config, owner, send, active);
  queues.push(queue);
  return { dir, queue, config };
};
const enqueue = (q: MqttPublishQueue, id: string, message = id) => {
  const delivered = q.enqueueOrThrow("factory/line/temperature", message, id);
  // These fixtures intentionally leave pending work for restart/overflow tests.
  void delivered.catch(() => {});
  return delivered;
};
afterEach(async () => {
  for (const q of queues.splice(0)) await q.close();
  for (const r of roots.splice(0)) rmSync(r, { recursive: true, force: true });
});

describe("optional MQTT output persistence", () => {
  it("does not add persistence defaults to ordinary channels", () => {
    expect(mqttChannelParameters(resolveMqttChannel({ host: "localhost" }))).toEqual({});
    const selected = { host: "localhost", publishQueue: { mode: "durable", directory: "/local/output" } };
    expect(mqttChannelSchema.safeParse(selected).success).toBe(false);
    expect(mqttOutputChannelSchema.safeParse(selected).success).toBe(true);
    expect(mqttPublishQueueSchema.safeParse({ mode: "durable", directory: "/local/state", mystery: true }).success).toBe(false);
  });
  it("carries explicitly selected output queue through standard channel resolution", () => {
    const config = { mode: "buffered" as const, directory: "/local/output" };
    expect(resolveMqttChannel({ host: "localhost", publishQueue: config }).publishQueue).toBeUndefined();
    expect(mqttChannelParameters(resolveMqttChannel({ host: "localhost" }, { publishQueue: config })).publishQueue).toEqual(config);
  });
  it("commits durable acceptance before return and retains pending work over reopen", async () => {
    const { queue, dir, config } = fixture();
    enqueue(queue, "first");
    expect(queue.snapshot()).toMatchObject({ pendingRecords: 1, diskRecords: 1, memoryRecords: 0 });
    await queue.close();
    const sent: string[] = [];
    const next = new MqttPublishQueue(
      config,
      "unrelated/meter-a",
      async (r) => {
        sent.push(r.message);
      },
      true,
    );
    queues.push(next);
    await next.waitForIdle(1000);
    expect(sent).toEqual(["first"]);
    expect(next.snapshot().pendingRecords).toBe(0);
  });
  it("buffers healthy work in RAM and waits for the destination before delivery completion", async () => {
    let confirm!: () => void;
    const { queue } = fixture(
      { mode: "buffered" },
      () =>
        new Promise<void>((resolve) => {
          confirm = resolve;
        }),
    );
    const delivered = enqueue(queue, "buffered");
    expect(queue.snapshot()).toMatchObject({ memoryRecords: 1, diskRecords: 0 });
    queue.becomeActive();
    await Promise.resolve();
    let completed = false;
    void delivered.then(() => {
      completed = true;
    });
    await Promise.resolve();
    expect(completed).toBe(false);
    confirm();
    await delivered;
    await queue.waitForIdle(1000);
    expect(queue.snapshot().delivered).toBe(1);
  });
  it("spills all RAM work after observed output failure and replays it in original order", async () => {
    let online = false;
    const sent: string[] = [];
    const { queue } = fixture({ mode: "buffered" }, async (r) => {
      if (!online) throw Error("offline");
      sent.push(r.message);
    });
    enqueue(queue, "a");
    enqueue(queue, "b");
    queue.becomeActive();
    await new Promise((resolve) => setTimeout(resolve, 2));
    expect(queue.snapshot()).toMatchObject({ memoryRecords: 0, diskRecords: 2, pendingRecords: 2 });
    online = true;
    await queue.waitForIdle(1000);
    expect(sent).toEqual(["a", "b"]);
  });
  it("spills memory overflow without overtaking earlier buffered messages", async () => {
    const sent: string[] = [];
    const { queue } = fixture({ mode: "buffered", memoryRecords: 1 }, async (r) => {
      sent.push(r.id);
    });
    enqueue(queue, "old-ram");
    enqueue(queue, "next-disk");
    expect(queue.snapshot()).toMatchObject({ memoryRecords: 1, diskRecords: 1 });
    queue.becomeActive();
    await queue.waitForIdle(1000);
    expect(sent).toEqual(["old-ram", "next-disk"]);
  });
  it("refuses count overflow without deleting previously accepted work", () => {
    const { queue } = fixture({ maxRecords: 1 });
    enqueue(queue, "retained");
    expect(() => enqueue(queue, "rejected")).toThrow("capacity");
    expect(queue.snapshot().pendingRecords).toBe(1);
  });
  it("refuses byte and individual-message overflow", () => {
    const { queue } = fixture({ maxBytes: 320, maxMessageBytes: 320 });
    enqueue(queue, "one", "x");
    expect(() => enqueue(queue, "two", "x")).toThrow("capacity");
    expect(() => enqueue(queue, "large", "x".repeat(300))).toThrow("maxMessageBytes");
  });
  it("refuses QoS zero, ephemeral MQTT properties, wildcards and duplicate admissions", () => {
    const { queue } = fixture();
    expect(() => queue.enqueueOrThrow("a", "x", "zero", { qos: 0 })).toThrow("QoS");
    expect(() => queue.enqueueOrThrow("a", "x", "expiry", { properties: { messageExpiryInterval: 1 } })).toThrow("publish options");
    expect(() => queue.enqueueOrThrow("a/#", "x", "wild")).toThrow("envelope");
    enqueue(queue, "same");
    expect(() => enqueue(queue, "same")).toThrow("Duplicate");
  });
  it("preserves work when destination acknowledged but its response was lost", async () => {
    const seen: string[] = [];
    let first = true;
    const { queue } = fixture({}, async (r) => {
      seen.push(r.id);
      if (first) {
        first = false;
        throw Error("ACK lost after delivery");
      }
    });
    const completed = enqueue(queue, "ambiguous");
    queue.becomeActive();
    await completed;
    await queue.waitForIdle(1000);
    expect(seen).toEqual(["ambiguous", "ambiguous"]);
    expect(queue.snapshot()).toMatchObject({ retries: 1, delivered: 1, pendingRecords: 0 });
  });
  it("rejects drain timeout and keeps its durable record", async () => {
    const { queue } = fixture();
    enqueue(queue, "pending");
    await expect(queue.waitForIdle(10)).rejects.toThrow("retained");
    expect(queue.snapshot().diskRecords).toBe(1);
  });
  it("excludes a second writer and a different destination identity", async () => {
    const { queue, config } = fixture();
    enqueue(queue, "original");
    expect(() => new MqttPublishQueue(config, "unrelated/meter-a", async () => {}, false)).toThrow();
    await queue.close();
    expect(() => new MqttPublishQueue(config, "other-destination", async () => {}, false)).toThrow("identity");
    const next = new MqttPublishQueue(config, "unrelated/meter-a", async () => {}, false);
    queues.push(next);
    expect(next.snapshot().pendingRecords).toBe(1);
  });
  it("supports two unrelated services and directories without shared ownership or data", async () => {
    const one: string[] = [],
      two: string[] = [];
    const a = fixture(
      {},
      async (r) => {
        one.push(r.message);
      },
      "vision/camera-7",
    );
    const b = fixture(
      {},
      async (r) => {
        two.push(r.message);
      },
      "meter/flow-4",
    );
    enqueue(a.queue, "image", "image-metadata");
    enqueue(b.queue, "meter", "flow-value");
    a.queue.becomeActive();
    b.queue.becomeActive();
    await Promise.all([a.queue.waitForIdle(1000), b.queue.waitForIdle(1000)]);
    expect(one).toEqual(["image-metadata"]);
    expect(two).toEqual(["flow-value"]);
  });
  it("refuses a symlinked directory and leaves original contents unchanged", () => {
    const original = directory(),
      parent = directory(),
      link = path.join(parent, "symlink");
    symlinkSync(original, link);
    expect(() => new MqttPublishQueue({ mode: "durable", directory: link }, "owner", async () => {})).toThrow("private");
  });
  it("blocks corrupted disk payloads rather than publishing or deleting them", async () => {
    const { queue, config, dir } = fixture();
    enqueue(queue, "retained");
    await queue.close();
    const { DatabaseSync } = createRequire(import.meta.url)("node:sqlite");
    const db = new DatabaseSync(path.join(dir, "mqtt-outbox.sqlite"));
    db.exec("UPDATE pending SET record='invalid'");
    db.close();
    let sent = false;
    const next = new MqttPublishQueue(
      config,
      "unrelated/meter-a",
      async () => {
        sent = true;
      },
      true,
    );
    queues.push(next);
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(sent).toBe(false);
    expect(next.snapshot()).toMatchObject({ state: "blocked", pendingRecords: 1 });
    await expect(next.waitForIdle(100)).rejects.toThrow("integrity");
  });
  it("refuses legacy skip hashes without discarding pending output", () => {
    const { queue } = fixture();
    enqueue(queue, "retained");
    expect(() => queue.becomeActive({ batchSize: 1, referenceHash: "old-hash" })).toThrow("handover");
    expect(queue.snapshot().pendingRecords).toBe(1);
  });
  it("a full SQLite file rejects new admission and keeps prior accepted records", () => {
    const { queue } = fixture({ maxDiskBytes: 1048576, maxMessageBytes: 700000 });
    enqueue(queue, "first", "x".repeat(200000));
    expect(() => enqueue(queue, "second", "y".repeat(600000))).toThrow();
    expect(queue.snapshot().pendingRecords).toBe(1);
  });
  it("bounds a WAL pinned by an external reader, then resumes after the reader releases it", async () => {
    const { queue, dir } = fixture({ maxDiskBytes: 1048576 }, async () => {}, "pinned-reader", true);
    const { DatabaseSync } = createRequire(import.meta.url)("node:sqlite");
    const reader = new DatabaseSync(path.join(dir, "mqtt-outbox.sqlite"), { readOnly: true });
    reader.exec("BEGIN");
    reader.prepare("SELECT COUNT(*) FROM pending").get();
    let failure: Error | undefined;
    let delivered = 0;
    try {
      for (let i = 0; i < 200; i++) {
        try {
          await enqueue(queue, "bounded-" + i);
          delivered++;
        } catch (error) {
          failure = error as Error;
          break;
        }
      }
      expect(failure?.message).toContain("physical disk budget");
      expect(delivered).toBeGreaterThan(0);
      expect(queue.snapshot().pendingRecords).toBe(0);
    } finally {
      reader.exec("ROLLBACK");
      reader.close();
    }
    await enqueue(queue, "after-reader-release");
    await queue.waitForIdle(1000);
    expect(queue.snapshot().pendingRecords).toBe(0);
  });
});
