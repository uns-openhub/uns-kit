import { existsSync, mkdirSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { drainMqttForMigration, readMqttDrainCapability } from "../packages/uns-core/src/runtime/mqtt-drain.js";
import { managedMqttOutputBinding } from "../packages/uns-core/src/runtime/mqtt-output-state-contract.js";
import { assertDisabledPublishQueueEmpty } from "../packages/uns-core/src/uns-mqtt/disabled-publish-queue.js";
import { MqttPublishQueue } from "../packages/uns-core/src/uns-mqtt/publish-queue.js";

const roots: string[] = [], queues: MqttPublishQueue[] = [];
const fixture = (name: string) => {
  const root = realpathSync(mkdtempSync(path.join(os.tmpdir(), "uns-clean-move-"))); roots.push(root);
  return { mode: "durable" as const, directory: path.join(root, name), minFreeBytes: 0, retryIntervalMs: 10 };
};
afterEach(async () => { for (const q of queues.splice(0)) await q.close(); for (const r of roots.splice(0)) rmSync(r, { recursive: true, force: true }); });
describe("explicit disabled persistence and generic migration drain", () => {
  it("keeps the declaration optional for ordinary runtimes and rejects unsupported protocols", () => {
    const root = fixture("descriptor").directory;
    expect(readMqttDrainCapability(root)).toBeUndefined();
    mkdirSync(root);
    const file = path.join(root, "package.json");
    writeFileSync(file, JSON.stringify({ name: "inspection-service" }));
    expect(readMqttDrainCapability(root)).toBeUndefined();
    writeFileSync(file, JSON.stringify({ unsDatahub: { mqttDrain: { protocolVersion: 2 } } }));
    expect(() => readMqttDrainCapability(root)).toThrow();
    writeFileSync(file, JSON.stringify({ unsDatahub: { mqttDrain: { protocolVersion: 1 } } }));
    expect(readMqttDrainCapability(root)).toBe(1);
  });
  it("starts disabled before the mount exists without making directories", () => {
    const config = { ...fixture("not-created"), storage: "runtime" as const, enabled: false };
    expect(() => assertDisabledPublishQueueEmpty(config, "unrelated-owner")).not.toThrow();
    expect(managedMqttOutputBinding({ output: { publishQueue: config } }, undefined)).toBeUndefined();
    expect(existsSync(config.directory)).toBe(false);
  });
  it("refuses a live owner and retained pending output when persistence is disabled", async () => {
    const config = fixture("retained");
    const q = new MqttPublishQueue(config, "sensor-a", async () => {}, false); queues.push(q);
    const completion = q.enqueueOrThrow("plant/a", "retained", "one"); void completion.catch(() => {});
    expect(() => assertDisabledPublishQueueEmpty({ ...config, enabled: false }, "sensor-a")).toThrow();
    await q.close();
    expect(() => assertDisabledPublishQueueEmpty({ ...config, enabled: false }, "sensor-a")).toThrow(/not empty/);
    const next = new MqttPublishQueue(config, "sensor-a", async () => {}, true); queues.push(next);
    await next.waitForIdle(1000); await next.close();
    expect(() => assertDisabledPublishQueueEmpty({ ...config, enabled: false }, "sensor-a")).not.toThrow();
    expect(() => assertDisabledPublishQueueEmpty({ ...config, enabled: false }, "different-destination")).toThrow(/identity/);
  });
  it("quiesces input and accepted work before confirmed output, then seals admission", async () => {
    const order: string[] = [];
    let empty = false;
    const receipt = await drainMqttForMigration({
      quiesce: async () => { order.push("input-stopped"); },
      finishAccepted: async () => { order.push("application-finished"); },
      outputs: [{ flush: async () => { order.push("broker-confirmed"); empty = true; }, sealPublishes: () => order.push("sealed"), getPublishQueueStatus: () => undefined }],
    });
    expect(empty).toBe(true);
    expect(order).toEqual(["input-stopped", "application-finished", "broker-confirmed", "sealed"]);
    expect(receipt).toEqual({ protocol: 1, state: "drained", pendingRecords: 0, outputs: 1 });
  });
  it("does not certify RAM or disk work as empty after a failed flush", async () => {
    let sealed = false;
    await expect(drainMqttForMigration({ quiesce: async () => {}, finishAccepted: async () => {}, outputs: [{
      flush: async () => { throw Error("broker offline"); }, sealPublishes: () => { sealed = true; }, getPublishQueueStatus: () => undefined,
    }] })).rejects.toThrow("broker offline");
    expect(sealed).toBe(false);
  });
  it("keeps migration blocked when application work failed before output admission", async () => {
    let flushed = false, sealed = false;
    await expect(drainMqttForMigration({
      quiesce: async () => {},
      finishAccepted: async () => { throw Error("accepted transform failed"); },
      outputs: [{ flush: async () => { flushed = true; }, sealPublishes: () => { sealed = true; }, getPublishQueueStatus: () => undefined }],
    })).rejects.toThrow("accepted transform failed");
    expect(flushed).toBe(false); expect(sealed).toBe(false);
  });
  it("refuses a successful flush whose retained status still contains rows", async () => {
    const q = new MqttPublishQueue(fixture("inconsistent"), "inspection", async () => {}, false); queues.push(q);
    void q.enqueueOrThrow("synthetic/inspection", "value", "one").catch(() => {});
    let sealed = false;
    await expect(drainMqttForMigration({ quiesce: async () => {}, finishAccepted: async () => {}, outputs: [{
      flush: async () => {}, sealPublishes: () => { sealed = true; }, getPublishQueueStatus: () => q.snapshot(),
    }] })).rejects.toThrow("not confirmed empty");
    expect(sealed).toBe(false); expect(q.snapshot().pendingRecords).toBe(1);
  });
  it("cannot seal late work after the reviewed deadline has expired", async () => {
    let finish!: () => void, sealed = false;
    const drain = drainMqttForMigration({ timeoutMs: 10, quiesce: async () => {}, finishAccepted: () => new Promise<void>(r => { finish = r; }), outputs: [{
      flush: async () => {}, sealPublishes: () => { sealed = true; }, getPublishQueueStatus: () => undefined,
    }] });
    await expect(drain).rejects.toThrow(/timed out/); finish();
    await new Promise(r => setTimeout(r, 10)); expect(sealed).toBe(false);
  });
  it("drains retained disk work for unrelated application identities using the same mechanism", async () => {
    for (const name of ["energy-group", "vision-inspections"]) {
      const config = fixture(name), delivered: string[] = [];
      const old = new MqttPublishQueue(config, name, async () => { throw Error("offline"); }, false); queues.push(old);
      void old.enqueueOrThrow("synthetic/" + name, "original-value", "retained").catch(() => {}); await old.close();
      const next = new MqttPublishQueue(config, name, async r => { delivered.push(r.message); }, true); queues.push(next);
      let sealed = false;
      await drainMqttForMigration({ quiesce: async () => {}, finishAccepted: async () => {}, outputs: [{
        flush: timeout => next.waitForIdle(timeout), sealPublishes: () => { sealed = true; }, getPublishQueueStatus: () => next.snapshot(),
      }] });
      expect(sealed).toBe(true); expect(delivered).toEqual(["original-value"]); await next.close();
      expect(() => assertDisabledPublishQueueEmpty({ ...config, enabled: false }, name)).not.toThrow();
    }
  });
});
