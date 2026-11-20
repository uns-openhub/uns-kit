import { spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { once } from "node:events";
import fs from "node:fs";
import { createRequire } from "node:module";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it, vi } from "vitest";

import { managedMqttOutputNamespaceIdentity } from "../packages/uns-core/src/runtime/mqtt-output-state-contract.js";
import {
  QUEUED_HANDOVER_STATE,
  QUEUED_HANDOVER_TOPIC,
  type QueueObservation,
  type QueuedHandoverAdapter,
  type QueuedHandoverBinding,
  type QueuedHandoverEvidence,
  type QueuedHandoverOutput,
  QueuedHandoverRuntime,
  queuedHandoverEvidenceDigest,
  queuedHandoverEvidenceSchema,
  queuedHandoverResponseSchema,
  queuedHandoverTargetLaunch,
  readQueuedHandoverState,
} from "../packages/uns-core/src/runtime/queued-handover.js";
import { MqttPublishQueue } from "../packages/uns-core/src/uns-mqtt/publish-queue.js";
import UnsMqttProxy from "../packages/uns-core/src/uns-mqtt/uns-mqtt-proxy.js";

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
function temp() {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "kit-queued-handover-")));
  roots.push(root);
  return root;
}
function deferred() {
  let resolve: () => void = () => {};
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { resolve, promise };
}
function digest(value: string) {
  return createHash("sha256").update(value).digest("hex");
}
function request(launchId: string, action: string, binding?: QueuedHandoverBinding, previous?: QueuedHandoverEvidence) {
  return {
    protocol: 1,
    type: QUEUED_HANDOVER_TOPIC,
    requestId: randomUUID(),
    launchId,
    action,
    ...(binding ? { binding } : {}),
    ...(previous ? { previous } : {}),
  };
}
async function execute(runtime: QueuedHandoverRuntime, ...args: Parameters<typeof request>) {
  const response = await runtime.respond(request(...args));
  if (!response?.ok) throw Error(response?.reason ?? "Invalid test request");
  return response.status;
}

function fixture() {
  const root = temp();
  const source = path.join(root, "source");
  const target = path.join(root, "target");
  fs.mkdirSync(source);
  fs.mkdirSync(target);
  const now = Date.now();
  const binding = {
    protocol: 1,
    operationId: randomUUID(),
    reviewedChecksum: digest("review"),
    sourceLaunchId: "source-one",
    targetLaunchId: "target-two",
    sourceQueueIdentity: digest("source-queue"),
    targetQueueIdentity: digest("target-queue"),
    expiresAt: now + 60_000,
  } satisfies QueuedHandoverBinding;
  let effectsHeld = true;
  let sourcePending = 1;
  let capacityAvailable = true;
  let observedAt = now;
  const checkpoint = digest("durable-checkpoint");
  const sourceDrain = deferred();
  const outputs: QueuedHandoverOutput[] = [];
  const records = new DatabaseSync(path.join(root, "durable-ingress.sqlite"));
  records.exec("PRAGMA synchronous=FULL; CREATE TABLE work(seq INTEGER PRIMARY KEY, payload TEXT); INSERT INTO work VALUES(1, 'old')");
  const observed: string[] = [];
  const adapter = (identity: string): QueuedHandoverAdapter => ({
    observeApplication: () => [
      {
        id: "fixture",
        purpose: "Archive acknowledgement",
        unit: "rows",
        pending: identity === binding.sourceQueueIdentity ? sourcePending : 0,
        inFlight: 0,
        quality: "exact",
        observedAt,
      },
    ],
    queueIdentity: () => identity,
    inputBoundary: {
      quiesceSource: async () => checkpoint,
      openTarget: async (proof) => {
        expect(proof).toBe(checkpoint);
        if (!effectsHeld) throw Error("target external effects opened");
        for (const row of records.prepare("SELECT payload FROM work WHERE seq > 1 ORDER BY seq").all()) observed.push(String(row.payload));
        return proof;
      },
    },
    assertTargetEffectsHeld: () => {
      if (!effectsHeld) throw Error("target external effects opened");
    },
    releaseTargetEffects: async () => {
      effectsHeld = false;
    },
    targetCapacityAvailable: () => capacityAvailable,
    recoverSourceInput: async (proof) => {
      expect(proof).toBe(checkpoint);
      return proof;
    },
    drainApplication: async () => {
      await sourceDrain.promise;
      sourcePending = 0;
    },
  });
  const sourceOptions = {
    directory: source,
    launchId: binding.sourceLaunchId,
    adapter: adapter(binding.sourceQueueIdentity),
    outputs: () => outputs,
    now: () => now,
  };
  const targetOptions = {
    directory: target,
    launchId: binding.targetLaunchId,
    adapter: adapter(binding.targetQueueIdentity),
    outputs: () => [],
    now: () => now,
  };
  return {
    sourceOptions,
    targetOptions,
    root,
    binding,
    observed,
    outputs,
    sourceDrain,
    records,
    held: () => effectsHeld,
    capacity: (value: boolean) => {
      capacityAvailable = value;
    },
    stale: () => {
      observedAt -= 120_000;
    },
  };
}

describe("launch-correlated queued handover", () => {
  it.each([false, true])("binds QoS 0 loss acceptance and gap endpoints when release fault=%s", async (injectReleaseFault) => {
    const f = fixture();
    const now = f.binding.expiresAt - 60_000;
    let clock = now;
    let closed = false;
    let permitted = false;
    let effectsHeld = true;
    let spillFailed = false;
    const inputBoundary = {
      kind: "mqtt-qos0-gap-v1" as const,
      quiesceSource: async () => {
        closed = true;
        return { closedAt: clock };
      },
      openTarget: async () => ({ openedAt: clock }),
    };
    const accepted = { ...f.binding, acceptedInputLoss: true } as const;
    const source = new QueuedHandoverRuntime({ ...f.sourceOptions, now: () => clock, adapter: { ...f.sourceOptions.adapter, inputBoundary } });
    const target = new QueuedHandoverRuntime({
      ...f.targetOptions,
      now: () => clock,
      target: accepted,
      adapter: {
        ...f.targetOptions.adapter,
        inputBoundary,
        assertTargetEffectsHeld: () => {
          expect(effectsHeld).toBe(true);
        },
        assertTargetReleaseReady: () => {
          if (spillFailed) throw Error("Durable capture failed");
        },
        releaseTargetEffects: async () => {
          spillFailed = injectReleaseFault;
          effectsHeld = injectReleaseFault;
        },
      },
      targetReleased: () => {
        permitted = true;
      },
    });
    try {
      const durable = new QueuedHandoverRuntime(f.sourceOptions);
      await expect(durable.respond(request(accepted.sourceLaunchId, "freeze-source", accepted))).resolves.toMatchObject({
        ok: false,
        reason: "input-loss-acceptance-mismatch",
      });
      await expect(source.respond(request(accepted.sourceLaunchId, "freeze-source", f.binding))).resolves.toMatchObject({
        ok: false,
        reason: "input-loss-acceptance-mismatch",
      });
      expect(closed).toBe(false);
      const held = await target.status();
      expect(held.inputBoundary).toBe("mqtt-qos0-gap-v1");
      const frozen = (await execute(source, accepted.sourceLaunchId, "freeze-source", accepted)).evidence!;
      expect(frozen).toMatchObject({ acceptedInputLoss: true, closedAt: now, phase: "source-frozen" });
      expect(frozen.openedAt).toBeUndefined();
      expect(queuedHandoverEvidenceSchema.safeParse({ ...frozen, acceptedInputLoss: undefined }).success).toBe(false);
      clock += 500;
      const wrong = { ...frozen, closedAt: now - 1 };
      await expect(target.respond(request(accepted.targetLaunchId, "activate-target", accepted, wrong))).resolves.toMatchObject({
        ok: false,
        reason: "invalid-evidence",
      });
      const captured = (await execute(target, accepted.targetLaunchId, "activate-target", accepted, frozen)).evidence!;
      expect(captured).toMatchObject({ acceptedInputLoss: true, closedAt: now, openedAt: clock, phase: "target-capturing" });
      f.sourceDrain.resolve();
      await execute(source, accepted.sourceLaunchId, "drain-source", accepted, captured);
      await vi.waitFor(async () => expect((await source.status()).phase).toBe("source-drained"));
      const drained = (await source.status()).evidence!;
      expect(drained).toMatchObject({ acceptedInputLoss: true, closedAt: now, openedAt: clock, phase: "source-drained" });
      if (injectReleaseFault) {
        await expect(target.respond(request(accepted.targetLaunchId, "release-target", accepted, drained))).resolves.toMatchObject({
          ok: false,
          reason: "outcome-unknown",
        });
        expect(permitted).toBe(false);
        expect((await target.status()).evidence?.phase).toBe("target-capturing");
      } else {
        const released = await execute(target, accepted.targetLaunchId, "release-target", accepted, drained);
        expect(released.evidence).toMatchObject({ acceptedInputLoss: true, closedAt: now, openedAt: clock, phase: "target-released" });
        expect(effectsHeld).toBe(false);
        expect(permitted).toBe(true);
      }
    } finally {
      f.records.close();
    }
  });

  it("durably reconciles late QoS 0 closure and opening without dispatching either action twice", async () => {
    const f = fixture();
    const binding = { ...f.binding, acceptedInputLoss: true } as const;
    const close = deferred();
    const open = deferred();
    const now = binding.expiresAt - 60_000;
    let clock = now;
    let sourceActive = false;
    const inputBoundary = {
      kind: "mqtt-qos0-gap-v1" as const,
      quiesceSource: vi.fn(async () => {
        await close.promise;
        return { closedAt: clock };
      }),
      openTarget: vi.fn(async () => {
        await open.promise;
        return { openedAt: clock };
      }),
    };
    const source = new QueuedHandoverRuntime({
      ...f.sourceOptions,
      adapter: { ...f.sourceOptions.adapter, inputBoundary },
      now: () => clock,
      responseBudgetMs: 5,
      sourceInputActive: () => sourceActive,
    });
    const target = new QueuedHandoverRuntime({
      ...f.targetOptions,
      target: binding,
      adapter: { ...f.targetOptions.adapter, inputBoundary },
      now: () => clock,
      responseBudgetMs: 5,
    });
    try {
      const freeze = () => execute(source, binding.sourceLaunchId, "freeze-source", binding);
      expect(await source.status()).toMatchObject({ phase: "ready", reason: "input-boundary-unproven" });
      await expect(freeze()).rejects.toThrow("input-boundary-unproven");
      expect(inputBoundary.quiesceSource).not.toHaveBeenCalled();
      sourceActive = true;
      expect(await freeze()).toMatchObject({ phase: "blocked", reason: "operation-pending", binding });
      expect(await freeze()).toMatchObject({ phase: "blocked", reason: "operation-pending" });
      expect(inputBoundary.quiesceSource).toHaveBeenCalledTimes(1);
      expect(readQueuedHandoverState(f.sourceOptions.directory)).toMatchObject({ pending: "freeze-source", evidence: [] });
      clock += 1;
      close.resolve();
      await vi.waitFor(async () => expect((await source.status()).phase).toBe("source-frozen"));
      const frozen = (await freeze()).evidence!;
      expect(frozen).toMatchObject({ closedAt: now + 1, phase: "source-frozen" });
      expect(inputBoundary.quiesceSource).toHaveBeenCalledTimes(1);

      const activate = () => execute(target, binding.targetLaunchId, "activate-target", binding, frozen);
      expect(await activate()).toMatchObject({ phase: "blocked", reason: "operation-pending", binding });
      expect(await activate()).toMatchObject({ phase: "blocked", reason: "operation-pending" });
      expect(inputBoundary.openTarget).toHaveBeenCalledTimes(1);
      expect(readQueuedHandoverState(f.targetOptions.directory)).toMatchObject({ pending: "activate-target", evidence: [] });
      clock += 1;
      open.resolve();
      await vi.waitFor(async () => expect((await target.status()).phase).toBe("target-capturing"));
      const capture = (await activate()).evidence!;
      expect(readQueuedHandoverState(f.targetOptions.directory)).toMatchObject({ pending: null, evidence: [capture] });
      expect(capture).toMatchObject({ closedAt: now + 1, openedAt: now + 2 });
      expect(inputBoundary.openTarget).toHaveBeenCalledTimes(1);
    } finally {
      close.resolve();
      open.resolve();
      f.records.close();
    }
  });

  it("requires fresh exact empty successor lanes before capture and preserves the proof once new rows arrive", async () => {
    const f = fixture();
    const { binding } = f;
    const sample = (id: string): QueueObservation => ({
      id,
      purpose: "Durable work",
      unit: "records",
      pending: 0,
      inFlight: 0,
      quality: "exact",
      observedAt: Date.now(),
    });
    const firstLane = sample("source-input");
    const secondLane = sample("destination-output");
    let queues = [firstLane, secondLane];
    let openCalls = 0;
    const target = new QueuedHandoverRuntime({
      ...f.targetOptions,
      target: binding,
      adapter: {
        ...f.targetOptions.adapter,
        observeApplication: () => queues,
        inputBoundary: {
          ...f.targetOptions.adapter.inputBoundary!,
          openTarget: async (proof) => {
            openCalls++;
            queues = [{ ...firstLane, pending: 2 }, secondLane];
            return proof;
          },
        },
      },
    });
    try {
      expect(await target.status()).toMatchObject({ phase: "held", queues: [{ id: firstLane.id }, { id: secondLane.id }] });
      const source = new QueuedHandoverRuntime(f.sourceOptions);
      const frozen = (await execute(source, binding.sourceLaunchId, "freeze-source", binding)).evidence!;
      const activate = () => target.respond(request(binding.targetLaunchId, "activate-target", binding, frozen));
      for (const [observations, reason] of [
        [[{ ...firstLane, pending: 1 }, secondLane], "target-not-empty"],
        [[firstLane, { ...secondLane, inFlight: 1 }], "target-not-empty"],
        [[], "progress-unknown"],
        [[firstLane], "progress-unknown"],
        [[firstLane, { ...secondLane, quality: "lower-bound" as const }], "progress-unknown"],
        [[firstLane, { ...secondLane, observedAt: secondLane.observedAt - 60_000 }], "progress-unknown"],
      ] as const) {
        queues = [...observations];
        expect(await activate()).toMatchObject({ ok: false, reason });
        expect(await target.status()).toMatchObject({ phase: "held", reason });
        expect(readQueuedHandoverState(f.targetOptions.directory)).toMatchObject({ pending: null, evidence: [] });
      }
      expect(openCalls).toBe(0);
      queues = [firstLane, secondLane];
      const accepted = (await execute(target, binding.targetLaunchId, "activate-target", binding, frozen)).evidence!;
      const retry = await execute(target, binding.targetLaunchId, "activate-target", binding, frozen);
      expect(retry.evidence).toEqual(accepted);
      expect(retry.queues[0]).toMatchObject({ pending: 2, quality: "exact" });
      expect(openCalls).toBe(1);
    } finally {
      f.records.close();
    }
  });

  it("moves the same released successor launch into a second reviewed operation after persisting freeze intent", async () => {
    const f = fixture();
    const first = f.binding;
    const second: QueuedHandoverBinding = {
      ...first,
      operationId: randomUUID(),
      reviewedChecksum: digest("review-second"),
      sourceLaunchId: first.targetLaunchId,
      sourceQueueIdentity: first.targetQueueIdentity,
      targetLaunchId: "third-physical-launch",
      targetQueueIdentity: digest("third-queue"),
    };
    const sampleGate = deferred();
    const startedSample = deferred();
    const fences: string[] = [];
    let delayNextObservation = false;
    let running = false;
    let quiesced = 0;
    let thirdHeld = true;
    const observe = async () => {
      if (delayNextObservation) {
        delayNextObservation = false;
        startedSample.resolve();
        await sampleGate.promise;
      }
      return undefined;
    };
    const firstSource = new QueuedHandoverRuntime(f.sourceOptions);
    const successor = new QueuedHandoverRuntime({
      ...f.targetOptions,
      target: first,
      outputs: () => [
        {
          instanceName: "output",
          observeQueuedHandoverOutput: observe,
          flush: async () => {},
          sealPublishes: () => {},
          releaseQueuedHandoverOutput: async () => {},
        },
      ],
      adapter: {
        ...f.targetOptions.adapter,
        inputBoundary: {
          ...f.targetOptions.adapter.inputBoundary!,
          quiesceSource: async (binding) => {
            expect(binding).toEqual(second);
            expect(readQueuedHandoverState(f.targetOptions.directory)).toMatchObject({
              binding: second,
              role: "source",
              pending: "freeze-source",
              evidence: [],
            });
            quiesced++;
            return digest("second-input-boundary");
          },
        },
      },
      fenceLegacyHandover: (role) => {
        if (role === "source") expect(running).toBe(true);
        fences.push(role);
      },
      targetReleased: () => {
        running = true;
      },
    });
    try {
      const initial = (await execute(firstSource, first.sourceLaunchId, "freeze-source", first)).evidence!;
      const captured = (await execute(successor, first.targetLaunchId, "activate-target", first, initial)).evidence!;
      await expect(successor.respond(request(second.sourceLaunchId, "freeze-source", second))).resolves.toMatchObject({
        ok: false,
        reason: "request-conflict",
      });
      f.sourceDrain.resolve();
      await execute(firstSource, first.sourceLaunchId, "drain-source", first, captured);
      await vi.waitFor(async () => expect((await firstSource.status()).phase).toBe("source-drained"));
      const firstDrained = (await firstSource.status()).evidence!;
      const released = (await execute(successor, first.targetLaunchId, "release-target", first, firstDrained)).evidence!;
      await expect(
        successor.respond(request(first.targetLaunchId, "release-target", first, { ...firstDrained, inputBoundaryDigest: digest("stale-receipt") })),
      ).resolves.toMatchObject({ ok: false, reason: "invalid-evidence" });
      await expect(
        successor.respond(request(second.sourceLaunchId, "freeze-source", { ...second, operationId: first.operationId })),
      ).resolves.toMatchObject({ ok: false, reason: "request-conflict" });
      await expect(successor.respond(request(second.sourceLaunchId, "freeze-source", { ...second, expiresAt: 1 }))).resolves.toMatchObject({
        ok: false,
        reason: "request-conflict",
      });
      await expect(
        successor.respond(request(second.sourceLaunchId, "freeze-source", { ...second, sourceQueueIdentity: digest("different-queue") })),
      ).resolves.toMatchObject({ ok: false, reason: "request-conflict" });
      expect(quiesced).toBe(0);
      const ledger = path.join(f.targetOptions.directory, QUEUED_HANDOVER_STATE);
      const terminal = readQueuedHandoverState(f.targetOptions.directory)!;
      fs.writeFileSync(ledger, JSON.stringify({ ...terminal, pending: "release-target" }));
      await expect(successor.respond(request(second.sourceLaunchId, "freeze-source", second))).resolves.toMatchObject({
        ok: false,
        reason: "outcome-unknown",
      });
      fs.writeFileSync(ledger, JSON.stringify(terminal));

      delayNextObservation = true;
      const secondFreeze = execute(successor, second.sourceLaunchId, "freeze-source", second);
      await startedSample.promise;
      expect(readQueuedHandoverState(f.targetOptions.directory)).toEqual(terminal);
      expect((await execute(successor, first.targetLaunchId, "release-target", first, firstDrained)).evidence).toEqual(released);
      sampleGate.resolve();
      const secondFrozen = (await secondFreeze).evidence!;
      expect(secondFrozen).toMatchObject({ operationId: second.operationId, phase: "source-frozen", previousDigest: null });
      await expect(successor.respond(request(first.targetLaunchId, "release-target", first, firstDrained))).resolves.toMatchObject({
        ok: false,
        reason: "request-conflict",
      });
      const directory = path.join(f.root, "third-runtime");
      fs.mkdirSync(directory);
      const third = new QueuedHandoverRuntime({
        ...f.targetOptions,
        directory,
        launchId: second.targetLaunchId,
        target: second,
        adapter: {
          ...f.targetOptions.adapter,
          inputBoundary: { quiesceSource: async () => digest("unused"), openTarget: async (proof) => proof },
          queueIdentity: () => second.targetQueueIdentity,
          assertTargetEffectsHeld: () => {
            expect(thirdHeld).toBe(true);
          },
          releaseTargetEffects: async () => {
            thirdHeld = false;
          },
        },
      });
      const thirdCapture = (await execute(third, second.targetLaunchId, "activate-target", second, secondFrozen)).evidence!;
      await execute(successor, second.sourceLaunchId, "drain-source", second, thirdCapture);
      await vi.waitFor(async () => expect((await successor.status()).phase).toBe("source-drained"));
      expect((await execute(third, second.targetLaunchId, "release-target", second, (await successor.status()).evidence!)).phase).toBe(
        "target-released",
      );
      expect(fences).toEqual(["target", "source"]);
      expect(quiesced).toBe(1);
    } finally {
      sampleGate.resolve();
      f.records.close();
    }
  });

  it("uses one SDK outbox with a replay proof and no application queue, drain, or effect callbacks", async () => {
    const f = fixture();
    const { binding } = f;
    const checkpoint = digest("SDK-only-ingress-checkpoint");
    let held = true;
    let sealed = false;
    const source = new QueuedHandoverRuntime({
      ...f.sourceOptions,
      adapter: { inputBoundary: { quiesceSource: async () => checkpoint, openTarget: async (proof) => proof } },
      managedOutput: {
        identity: () => binding.sourceQueueIdentity,
        targetCapacityAvailable: () => true,
        assertTargetEffectsHeld: () => {},
      },
      outputs: () => [
        {
          instanceName: "source-output",
          observeQueuedHandoverOutput: async () => ({
            id: "output",
            purpose: "Broker acknowledgement",
            unit: "mqtt-records",
            quality: "exact",
            pending: 0,
            inFlight: 0,
            observedAt: Date.now(),
          }),
          flush: async () => {
            expect(sealed).toBe(true);
          },
          sealPublishes: () => {
            sealed = true;
          },
          releaseQueuedHandoverOutput: async () => {},
        },
      ],
    });
    const target = new QueuedHandoverRuntime({
      ...f.targetOptions,
      target: binding,
      adapter: { inputBoundary: { quiesceSource: async () => checkpoint, openTarget: async (proof) => proof } },
      managedOutput: {
        identity: () => binding.targetQueueIdentity,
        targetCapacityAvailable: () => true,
        assertTargetEffectsHeld: () => {
          expect(held).toBe(true);
        },
      },
      outputs: () => [
        {
          instanceName: "target-output",
          observeQueuedHandoverOutput: async () => ({
            id: "output",
            purpose: "Broker acknowledgement",
            unit: "mqtt-records",
            pending: 0,
            inFlight: 0,
            quality: "exact",
            observedAt: Date.now(),
          }),
          flush: async () => {},
          sealPublishes: () => {},
          releaseQueuedHandoverOutput: async () => {
            held = false;
          },
        },
      ],
    });
    try {
      const frozen = (await execute(source, binding.sourceLaunchId, "freeze-source", binding)).evidence!;
      const handled = (await execute(target, binding.targetLaunchId, "activate-target", binding, frozen)).evidence!;
      await execute(source, binding.sourceLaunchId, "drain-source", binding, handled);
      expect(held).toBe(true);
      await vi.waitFor(async () => expect((await source.status()).phase).toBe("source-drained"));
      const released = await execute(target, binding.targetLaunchId, "release-target", binding, (await source.status()).evidence!);
      expect(released.phase).toBe("target-released");
      expect(held).toBe(false);
    } finally {
      f.records.close();
    }
  });

  it("pins immutable managed mount and physical owner namespace independently of application names", () => {
    const rootId = randomUUID();
    const identity = managedMqttOutputNamespaceIdentity(rootId, "meter/old");
    expect(managedMqttOutputNamespaceIdentity(rootId, "meter/old")).toBe(identity);
    expect(managedMqttOutputNamespaceIdentity(rootId, "meter/new")).not.toBe(identity);
    expect(managedMqttOutputNamespaceIdentity(randomUUID(), "meter/old")).not.toBe(identity);
    expect(() => managedMqttOutputNamespaceIdentity(rootId, "../old")).toThrow(/namespace/);
  });
  it("captures records appended during cutover, waits for application and SQLite MQTT queues and authorizes a clean successor restart", async () => {
    const f = fixture();
    const { binding } = f;
    const sourcePublish = deferred();
    const queue = new MqttPublishQueue(
      { mode: "durable", directory: path.join(f.root, "mqtt-outbox"), minFreeBytes: 0 },
      "source-independent-outbox",
      () => sourcePublish.promise,
      true,
    );
    const delivery = queue.enqueueOrThrow("fixture/output", "old-before-new", randomUUID(), { qos: 1 });
    let sealed = false;
    f.outputs.push({
      instanceName: "fixture-output",
      observeQueuedHandoverOutput: async () => ({
        id: "sdk-outbox",
        purpose: "Broker acknowledgement",
        unit: "mqtt-records",
        pending: queue.snapshot().pendingRecords,
        inFlight: Number(queue.snapshot().inFlight),
        quality: "exact",
        observedAt: Date.now(),
      }),
      flush: (ms) => queue.waitForIdle(ms),
      sealPublishes: () => {
        sealed = true;
      },
      releaseQueuedHandoverOutput: async () => {},
    });
    try {
      const source = new QueuedHandoverRuntime(f.sourceOptions);
      const target = new QueuedHandoverRuntime({ ...f.targetOptions, target: binding });
      expect((await target.status()).phase).toBe("held");
      const frozen = (await execute(source, binding.sourceLaunchId, "freeze-source", binding)).evidence!;
      f.records.prepare("INSERT INTO work VALUES(2, ?)").run("between-close-and-capture");
      f.capacity(false);
      await expect(target.respond(request(binding.targetLaunchId, "activate-target", binding, frozen))).resolves.toMatchObject({
        ok: false,
        reason: "target-capacity-blocked",
      });
      f.capacity(true);
      const capturing = (await execute(target, binding.targetLaunchId, "activate-target", binding, frozen)).evidence!;
      expect(f.observed).toEqual(["between-close-and-capture"]);
      expect(f.held()).toBe(true);
      await expect(target.respond(request(binding.targetLaunchId, "release-target", binding, frozen))).resolves.toMatchObject({
        ok: false,
        reason: "invalid-evidence",
      });
      expect((await execute(source, binding.sourceLaunchId, "drain-source", binding, capturing)).reason).toBe("operation-pending");
      f.sourceDrain.resolve();
      await vi.waitFor(() => expect(sealed).toBe(true));
      const progress = await source.status();
      expect(progress.queues).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ owner: "application", pending: 0 }),
          expect.objectContaining({ owner: "sdk", pending: 1, inFlight: 1 }),
        ]),
      );
      expect(progress.evidence?.phase).toBe("source-frozen");
      expect(f.held()).toBe(true);
      sourcePublish.resolve();
      await delivery;
      await vi.waitFor(async () => expect((await source.status()).phase).toBe("source-drained"));
      const drained = (await source.status()).evidence!;
      expect(drained.previousDigest).toBe(queuedHandoverEvidenceDigest(capturing));
      expect(readQueuedHandoverState(f.sourceOptions.directory)?.evidence.at(-1)).toEqual(drained);
      f.capacity(false);
      const released = (await execute(target, binding.targetLaunchId, "release-target", binding, drained)).evidence!;
      expect(f.held()).toBe(false);
      expect((await execute(target, binding.targetLaunchId, "release-target", binding, drained)).evidence).toEqual(released);
      expect(() => new QueuedHandoverRuntime({ ...f.sourceOptions, launchId: "ordinary-source" })).toThrow("fenced");
      const restarted = new QueuedHandoverRuntime({ ...f.targetOptions, launchId: "ordinary-successor" });
      expect(await restarted.status()).toMatchObject({ phase: "ready", queueIdentity: binding.targetQueueIdentity });
    } finally {
      sourcePublish.resolve();
      await queue.close();
      f.records.close();
    }
  });

  it("leaves SDK publication available until a later application drain pass finishes", async () => {
    const f = fixture();
    const { binding } = f;
    let passes = 0;
    let pending = 2;
    let sealed = false;
    const published: string[] = [];
    const outbox = new MqttPublishQueue(
      { mode: "durable", directory: path.join(f.root, "mqtt-outbox"), minFreeBytes: 0 },
      "bounded-application-drain",
      async (record) => {
        published.push(record.message);
      },
      true,
    );
    f.outputs.push({
      instanceName: "fixture-output",
      observeQueuedHandoverOutput: async () => ({
        id: "sdk-outbox",
        purpose: "Broker acknowledgement",
        unit: "mqtt-records",
        pending: outbox.snapshot().pendingRecords,
        inFlight: Number(outbox.snapshot().inFlight),
        quality: "exact",
        observedAt: Date.now(),
      }),
      flush: (ms) => outbox.waitForIdle(ms),
      sealPublishes: () => {
        sealed = true;
      },
      releaseQueuedHandoverOutput: async () => {},
    });
    const source = new QueuedHandoverRuntime({
      ...f.sourceOptions,
      adapter: {
        ...f.sourceOptions.adapter,
        observeApplication: () => [
          { id: "application", purpose: "Transform source", unit: "rows", quality: "exact", pending, inFlight: 0, observedAt: Date.now() },
        ],
        drainApplication: async () => {
          expect(sealed).toBe(false);
          await outbox.enqueueOrThrow("fixture/output", `old-${++passes}`, randomUUID(), { qos: 1 });
          pending--;
        },
      },
    });
    const target = new QueuedHandoverRuntime({ ...f.targetOptions, target: binding });
    try {
      const frozen = (await execute(source, binding.sourceLaunchId, "freeze-source", binding)).evidence!;
      const capture = (await execute(target, binding.targetLaunchId, "activate-target", binding, frozen)).evidence!;
      const drain = () => execute(source, binding.sourceLaunchId, "drain-source", binding, capture);
      await drain();
      await vi.waitFor(async () => expect((await source.status()).reason).toBe("drain-blocked"));
      expect(readQueuedHandoverState(f.sourceOptions.directory)).toMatchObject({ pending: null, evidence: [frozen] });
      expect(published).toEqual(["old-1"]);
      expect(sealed).toBe(false);
      await drain();
      await vi.waitFor(async () => expect((await source.status()).phase).toBe("source-drained"));
      expect(published).toEqual(["old-1", "old-2"]);
      expect(sealed).toBe(true);
    } finally {
      await outbox.close();
      f.records.close();
    }
  });

  it("retains a frozen source after interrupted drain and demands verified drain-only recovery on a fresh launch", async () => {
    const f = fixture();
    const { binding } = f;
    try {
      const source = new QueuedHandoverRuntime({
        ...f.sourceOptions,
        adapter: { ...f.sourceOptions.adapter, drainApplication: () => new Promise(() => {}) },
      });
      const target = new QueuedHandoverRuntime({ ...f.targetOptions, target: binding });
      const frozen = (await execute(source, binding.sourceLaunchId, "freeze-source", binding)).evidence!;
      const captures = (await execute(target, binding.targetLaunchId, "activate-target", binding, frozen)).evidence!;
      await execute(source, binding.sourceLaunchId, "drain-source", binding, captures);
      expect(readQueuedHandoverState(f.sourceOptions.directory)?.pending).toBe("drain-source");
      const restoredOptions = {
        ...f.sourceOptions,
        launchId: "replacement-launch",
        adapter: { ...f.sourceOptions.adapter, observeApplication: () => [], drainApplication: async () => {} },
        recovery: { protocol: 1, operationId: binding.operationId, launchId: "replacement-launch", sourceFrozenDigest: digest("wrong") } as const,
      };
      expect(() => new QueuedHandoverRuntime(restoredOptions)).toThrow(/does not match/);
      const recovered = new QueuedHandoverRuntime({
        ...restoredOptions,
        adapter: f.sourceOptions.adapter,
        recovery: { ...restoredOptions.recovery, sourceFrozenDigest: queuedHandoverEvidenceDigest(frozen) },
      });
      const drain = await recovered.respond(request(binding.sourceLaunchId, "drain-source", binding, captures));
      expect(drain).toMatchObject({ ok: false, reason: "stale-launch" });
      await expect(recovered.respond(request("replacement-launch", "freeze-source", binding))).resolves.toMatchObject({
        ok: false,
        reason: "stale-launch",
      });
      f.sourceDrain.resolve();
      await execute(recovered, "replacement-launch", "drain-source", binding, captures);
      await vi.waitFor(async () => expect((await recovered.status()).phase).toBe("source-drained"));
    } finally {
      f.records.close();
    }
  });

  it("keeps observation-only, stale, wrong-chain and mixed launch data untrusted", async () => {
    const f = fixture();
    const { binding } = f;
    try {
      const optional = new QueuedHandoverRuntime({ ...f.sourceOptions, adapter: { observeApplication: f.sourceOptions.adapter.observeApplication } });
      expect(await optional.status()).toMatchObject({ phase: "ready", inputBoundary: "none", reason: "input-boundary-unproven" });
      await expect(optional.respond(request(binding.sourceLaunchId, "freeze-source", binding))).resolves.toMatchObject({
        ok: false,
        reason: "input-boundary-unproven",
      });
      const source = new QueuedHandoverRuntime(f.sourceOptions);
      const target = new QueuedHandoverRuntime({ ...f.targetOptions, target: binding });
      f.stale();
      expect(await source.status()).toMatchObject({ reason: "progress-unknown", queues: [{ quality: "unknown", pending: null, inFlight: null }] });
      await expect(source.respond(request(binding.sourceLaunchId, "freeze-source", binding))).resolves.toMatchObject({
        ok: false,
        reason: "progress-unknown",
      });
      expect(readQueuedHandoverState(f.sourceOptions.directory)).toBeUndefined();
      expect(() =>
        queuedHandoverTargetLaunch({
          UNS_HANDOVER_LAUNCH_ID: "different",
          UNS_QUEUED_HANDOVER_ROLE: "target",
          UNS_QUEUED_HANDOVER_BINDING: JSON.stringify(binding),
        }),
      ).toThrow(/differs/);
      expect(queuedHandoverTargetLaunch({ UNS_QUEUED_HANDOVER_ROLE: "", UNS_QUEUED_HANDOVER_BINDING: "" })).toBeUndefined();
      expect((await target.status()).phase).toBe("held");
    } finally {
      f.records.close();
    }
  });
});

it.skipIf(!process.env["UNS_QUEUED_HANDOVER_MQTT_TEST_URL"])(
  "holds a real worker's durable MQTT outbox until release and observes broker acknowledgement",
  async () => {
    const endpoint = new URL(process.env["UNS_QUEUED_HANDOVER_MQTT_TEST_URL"]!);
    expect(endpoint.hostname).toBe("127.0.0.1");
    const require = createRequire(new URL("../packages/uns-core/package.json", import.meta.url));
    const { connectAsync } = require("mqtt") as typeof import("mqtt");
    const observer = await connectAsync(endpoint.href);
    const topic = "queued-handover-test/" + randomUUID();
    const received: string[] = [];
    observer.on("message", (_, message) => received.push(message.toString()));
    await observer.subscribeAsync(topic, { qos: 1 });
    const proxy = new UnsMqttProxy(
      "127.0.0.1",
      "isolated",
      "queue",
      {
        port: Number(endpoint.port),
        publishQueue: { mode: "durable", directory: path.join(temp(), "outbox"), minFreeBytes: 0 },
      },
      false,
      false,
      true,
    );
    try {
      expect(() => proxy.setPublisherActive()).toThrow(/held/);
      await proxy.publishMessage(topic, "held-record", { qos: 1 });
      expect(await proxy.observeQueuedHandoverOutput()).toMatchObject({ pending: 1, inFlight: 0, quality: "exact" });
      expect(received).toEqual([]);
      await proxy.releaseQueuedHandoverOutput();
      await proxy.flush(5_000);
      expect(await proxy.observeQueuedHandoverOutput()).toMatchObject({ pending: 0, inFlight: 0, quality: "exact" });
      await vi.waitFor(() => expect(received).toEqual(["held-record"]));
    } finally {
      await proxy.stop({ drain: false });
      await observer.endAsync();
    }
  },
  15_000,
);

it.skipIf(!process.env["UNS_QUEUED_HANDOVER_MQTT_TEST_URL"])(
  "waits for broker-confirmed QoS 0 cutover, drains accepted callbacks and suppresses retained replay",
  async () => {
    const endpoint = new URL(process.env["UNS_QUEUED_HANDOVER_MQTT_TEST_URL"]!);
    expect(endpoint.hostname).toBe("127.0.0.1");
    const require = createRequire(new URL("../packages/uns-core/package.json", import.meta.url));
    const { connectAsync } = require("mqtt") as typeof import("mqtt");
    const publisher = await connectAsync(endpoint.href);
    const topic = "queued-handover-test/gap/" + randomUUID();
    await publisher.publishAsync(topic, "historical-retained", { qos: 1, retain: true });
    const oldMessages: string[] = [];
    const newMessages: string[] = [];
    const accepted = deferred();
    const callbackMayFinish = deferred();
    const sourceClientId = "old-gap-" + randomUUID();
    const options = {
      port: Number(endpoint.port),
      mqttSubToTopics: [topic],
      subscribeThrottlingDelay: 0,
      managedInput: true,
      queuedHandoverQos0Input: true as const,
      reconnectPeriod: 50,
    };
    const source = new UnsMqttProxy("127.0.0.1", "gap-source", "input", { ...options, clientId: sourceClientId }, false, true);
    const target = new UnsMqttProxy("127.0.0.1", "gap-target", "input", options, false, false, true);
    source.setInputHandler(async ({ message }) => {
      oldMessages.push(message);
      if (message === "accepted-before-close") {
        accepted.resolve();
        await callbackMayFinish.promise;
      }
    });
    target.setInputHandler(async ({ message }) => {
      newMessages.push(message);
    });
    try {
      await vi.waitFor(() => expect(source.getInputReadiness() && target.getConnectionReadiness()).toBe(true));
      await vi.waitFor(() => expect(oldMessages).toEqual(["historical-retained"]));
      expect(target.getInputReadiness()).toBe(false);
      expect(newMessages).toEqual([]);
      await publisher.publishAsync(topic, "accepted-before-close", { qos: 1 });
      await accepted.promise;
      let closeReported = false;
      const close = source.closeQueuedHandoverQos0Input().then((proof) => {
        closeReported = true;
        return proof;
      });
      await vi.waitFor(() => expect(source.getInputReadiness()).toBe(false));
      expect(closeReported).toBe(false);
      await publisher.publishAsync(topic, "during-gap-retained", { qos: 1, retain: true });
      callbackMayFinish.resolve();
      const { closedAt } = await close;
      expect((await source.closeQueuedHandoverQos0Input()).closedAt).toBe(closedAt);
      const { openedAt } = await target.openQueuedHandoverQos0Input();
      expect(openedAt).toBeGreaterThanOrEqual(closedAt);
      expect(target.getInputReadiness()).toBe(true);
      await publisher.publishAsync(topic, "first-live-retained", { qos: 1, retain: true });
      await vi.waitFor(() => expect(newMessages).toEqual(["first-live-retained"]));

      let disconnected = false;
      source.event.on("mqttProxyStatus", ({ event, value }) => {
        if (event === "mqtt-connected" && value === 0) disconnected = true;
      });
      const replacement = await connectAsync(endpoint.href, { clientId: sourceClientId, reconnectPeriod: 0 });
      await replacement.endAsync();
      await vi.waitFor(() => expect(disconnected && source.getConnectionReadiness()).toBe(true));
      source.subscribeAsync([topic]);
      source.setSubscriberActive();
      await publisher.publishAsync(topic, "after-reconnect", { qos: 1 });
      await vi.waitFor(() => expect(newMessages).toEqual(["first-live-retained", "after-reconnect"]));
      expect(source.getInputReadiness()).toBe(false);
      expect(oldMessages).toEqual(["historical-retained", "accepted-before-close"]);
    } finally {
      callbackMayFinish.resolve();
      await Promise.all([source.stop({ drain: false }), target.stop({ drain: false }), publisher.endAsync()]);
    }
  },
  15_000,
);

it.skipIf(!process.env["UNS_QUEUED_HANDOVER_MQTT_TEST_URL"])(
  "reports an ordinary SDK outbox over launch IPC without an application observer or manifest",
  async () => {
    const endpoint = new URL(process.env["UNS_QUEUED_HANDOVER_MQTT_TEST_URL"]!);
    expect(endpoint.hostname).toBe("127.0.0.1");
    const directory = temp();
    const launchId = randomUUID();
    fs.writeFileSync(path.join(directory, "package.json"), '{"name":"ordinary-service","version":"1.0.0"}');
    const ownerModule = new URL("../packages/uns-core/dist/uns/uns-proxy-process.js", import.meta.url).href;
    const entry = path.join(directory, "ordinary.mjs");
    fs.writeFileSync(
      entry,
      `
      import UnsProxyProcess from ${JSON.stringify(ownerModule)};
      const owner = new UnsProxyProcess("127.0.0.1", { processName: "ordinary", port: ${Number(endpoint.port)} });
      const output = await owner.createUnsMqttProxy("127.0.0.1", "output", "force", false, {
        port: ${Number(endpoint.port)}, publishQueue: { mode: "durable", directory: ${JSON.stringify(path.join(directory, "outbox"))}, minFreeBytes: 0 }
      });
      await output.publishMessage("fixture/ordinary/observation", "durable-record", { qos: 1 });
      process.on("message", message => {
        if (message === "stop") void owner.shutdown({ drain: false }).then(() => process.exit(0));
      });
      process.send("ready");
    `,
    );
    const child = spawn(process.execPath, [entry], {
      cwd: directory,
      env: { PATH: process.env.PATH, UNS_HANDOVER_LAUNCH_ID: launchId },
      stdio: ["ignore", "pipe", "pipe", "ipc"],
    });
    let output = "";
    for (const stream of [child.stdout, child.stderr])
      stream.on("data", (data) => {
        output = (output + data.toString()).slice(-4096);
      });
    const exited = once(child, "exit");
    const nextMessage = () =>
      Promise.race([
        once(child, "message"),
        exited.then(([code, signal]) => {
          throw Error(`Ordinary worker exited (${code ?? signal}): ${output}`);
        }),
      ]);
    try {
      expect((await nextMessage())[0]).toBe("ready");
      const statusRequest = request(launchId, "status");
      const pending = nextMessage();
      child.send({ type: "process:msg", topic: QUEUED_HANDOVER_TOPIC, data: statusRequest });
      const response = queuedHandoverResponseSchema.parse(((await pending)[0] as { data: unknown }).data);
      expect(response).toMatchObject({
        ok: true,
        requestId: statusRequest.requestId,
        launchId,
        status: {
          inputBoundary: "none",
          reason: "input-boundary-unproven",
          queues: [{ owner: "sdk", pending: 1, quality: "exact" }],
        },
      });
      child.send("stop");
      expect((await exited)[0]).toBe(0);
    } finally {
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
      await exited;
    }
  },
  15_000,
);
