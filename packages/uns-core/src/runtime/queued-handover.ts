import fs from "node:fs";
import path from "node:path";
import { z } from "zod";

import { readRetainedStoreJson, retainedStoreDigest, retainedStoreEntry, writeRetainedStoreJson } from "./retained-store-files.js";
import type { StoreBinding } from "./retained-store.js";

export { queuedHandoverManagedMqttIdentity } from "./mqtt-output-state-contract.js";

export const QUEUED_HANDOVER_TOPIC = "uns-kit:queued-handover:v1";
export const QUEUED_HANDOVER_RESPONSE_TOPIC = "uns-kit:queued-handover-response:v1";
export const QUEUED_HANDOVER_STATE = "queued-handover-state.json";
const hash = z.string().regex(/^[a-f0-9]{64}$/);
const launch = z.string().min(1).max(512);
const epoch = z.int().positive();
const role = z.enum(["source", "target"]);
const qos0Gap = "mqtt-qos0-gap-v1";
const boundary = z.enum(["none", "durable-replay-v1", qos0Gap]);
const stage = z.enum(["source-frozen", "target-capturing", "source-drained", "target-released"]);
const stageForAction = {
  "freeze-source": "source-frozen",
  "activate-target": "target-capturing",
  "drain-source": "source-drained",
  "release-target": "target-released",
} as const;
const priorForAction = {
  "activate-target": "source-frozen",
  "drain-source": "target-capturing",
  "release-target": "source-drained",
} as const;
const queuedHandoverBindingShape = {
  protocol: z.literal(1),
  operationId: z.uuid(),
  reviewedChecksum: hash,
  sourceLaunchId: launch,
  targetLaunchId: launch,
  sourceQueueIdentity: hash,
  targetQueueIdentity: hash,
  expiresAt: epoch,
  acceptedInputLoss: z.literal(true).optional(),
};
export const queuedHandoverBindingSchema = z
  .strictObject(queuedHandoverBindingShape)
  .refine((value) => value.sourceLaunchId !== value.targetLaunchId && value.sourceQueueIdentity !== value.targetQueueIdentity);
export type QueuedHandoverBinding = z.infer<typeof queuedHandoverBindingSchema>;
export const queuedHandoverEvidenceSchema = z
  .strictObject({
    ...queuedHandoverBindingShape,
    phase: stage,
    observedAt: epoch,
    inputBoundaryDigest: hash,
    previousDigest: hash.nullable(),
    closedAt: epoch.optional(),
    openedAt: epoch.optional(),
  })
  .refine((value) => {
    const {
      phase,
      previousDigest,
      sourceLaunchId,
      targetLaunchId,
      sourceQueueIdentity,
      targetQueueIdentity,
      acceptedInputLoss,
      closedAt,
      openedAt,
      observedAt,
    } = value;
    if ((phase === "source-frozen") !== (previousDigest === null) || sourceLaunchId === targetLaunchId || sourceQueueIdentity === targetQueueIdentity)
      return false;
    if (!acceptedInputLoss) return closedAt === undefined && openedAt === undefined;
    if (closedAt === undefined || closedAt > observedAt) return false;
    if (phase === "source-frozen") return openedAt === undefined;
    return openedAt !== undefined && openedAt >= closedAt && openedAt <= observedAt;
  });
export type QueuedHandoverEvidence = z.infer<typeof queuedHandoverEvidenceSchema>;
export function queuedHandoverEvidenceDigest(evidence: QueuedHandoverEvidence): string {
  return retainedStoreDigest(queuedHandoverEvidenceSchema.parse(evidence));
}

/** A verified binding is supplied by the runtime that opened the physical queue. */
export function queuedHandoverRetainedStoreIdentity(binding: StoreBinding): string {
  const { storeId, generation, directory, format, owner } = binding;
  return retainedStoreDigest({ storeId, generation, directory, format, owner });
}

export const queuedHandoverCapabilitySchema = z.strictObject({ protocolVersion: z.literal(1), inputBoundary: boundary });
export function readQueuedHandoverCapability(packageRoot: string): z.infer<typeof queuedHandoverCapabilitySchema> | undefined {
  const file = path.join(packageRoot, "package.json");
  let fd: number;
  try {
    fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
    throw error;
  }
  try {
    const stat = fs.fstatSync(fd);
    const maximum = 1024 * 1024;
    if (!stat.isFile() || stat.size > maximum) throw Error("Unsafe queued handover package declaration");
    const buffer = Buffer.alloc(maximum + 1);
    let length = 0;
    while (length < buffer.length) {
      const read = fs.readSync(fd, buffer, length, buffer.length - length, null);
      if (!read) break;
      length += read;
    }
    if (length > maximum) throw Error("Unsafe queued handover package declaration");
    const value = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(buffer.subarray(0, length)))?.unsDatahub?.queuedHandover;
    return value === undefined ? undefined : queuedHandoverCapabilitySchema.parse(value);
  } finally {
    fs.closeSync(fd);
  }
}

const observationShape = {
  id: z
    .string()
    .min(1)
    .max(128)
    .regex(/^[A-Za-z0-9][A-Za-z0-9._:-]*$/),
  purpose: z.string().min(1).max(128),
  unit: z.string().min(1).max(64),
  pending: z.int().nonnegative().nullable(),
  inFlight: z.int().nonnegative().nullable(),
  quality: z.enum(["exact", "lower-bound", "unknown"]),
  observedAt: epoch,
};
export const queuedHandoverApplicationObservationSchema = z.strictObject(observationShape);
export type QueueObservation = z.infer<typeof queuedHandoverApplicationObservationSchema>;
const queueObservationSchema = z
  .strictObject({ ...observationShape, owner: z.enum(["sdk", "application"]) })
  .refine((sample) =>
    sample.quality === "unknown"
      ? sample.pending === null && sample.inFlight === null
      : sample.pending !== null && (sample.quality !== "exact" || sample.inFlight !== null),
  );
const queuesSchema = z
  .array(queueObservationSchema)
  .max(64)
  .refine((values) => new Set(values.map((value) => value.id)).size === values.length);
const reason = z.enum([
  "input-boundary-unproven",
  "stale-launch",
  "request-conflict",
  "invalid-evidence",
  "operation-pending",
  "progress-unknown",
  "drain-blocked",
  "outcome-unknown",
  "target-capacity-blocked",
  "target-not-empty",
  "input-loss-acceptance-mismatch",
]);
type QueuedHandoverReason = z.infer<typeof reason>;
export const queuedHandoverStatusSchema = z.strictObject({
  protocol: z.literal(1),
  launchId: launch,
  inputBoundary: boundary,
  queueIdentity: hash.optional(),
  role: role.optional(),
  binding: queuedHandoverBindingSchema.optional(),
  phase: z.enum(["ready", "held", "blocked", ...stage.options]),
  observedAt: epoch,
  queues: queuesSchema,
  evidence: queuedHandoverEvidenceSchema.optional(),
  reason: reason.optional(),
});
export type QueuedHandoverStatus = z.infer<typeof queuedHandoverStatusSchema>;
const requestBase = { type: z.literal(QUEUED_HANDOVER_TOPIC), protocol: z.literal(1), requestId: z.uuid(), launchId: launch };
export const queuedHandoverRequestSchema = z.discriminatedUnion("action", [
  z.strictObject({ ...requestBase, action: z.literal("status") }),
  z.strictObject({ ...requestBase, action: z.literal("freeze-source"), binding: queuedHandoverBindingSchema }),
  z.strictObject({
    ...requestBase,
    action: z.literal("activate-target"),
    binding: queuedHandoverBindingSchema,
    previous: queuedHandoverEvidenceSchema,
  }),
  z.strictObject({ ...requestBase, action: z.literal("drain-source"), binding: queuedHandoverBindingSchema, previous: queuedHandoverEvidenceSchema }),
  z.strictObject({
    ...requestBase,
    action: z.literal("release-target"),
    binding: queuedHandoverBindingSchema,
    previous: queuedHandoverEvidenceSchema,
  }),
]);
export type QueuedHandoverRequest = z.infer<typeof queuedHandoverRequestSchema>;
const responseBase = { type: z.literal(QUEUED_HANDOVER_RESPONSE_TOPIC), protocol: z.literal(1), requestId: z.uuid(), launchId: launch };
export const queuedHandoverResponseSchema = z.discriminatedUnion("ok", [
  z.strictObject({ ...responseBase, ok: z.literal(true), status: queuedHandoverStatusSchema }),
  z.strictObject({ ...responseBase, ok: z.literal(false), reason }),
]);
export type QueuedHandoverResponse = z.infer<typeof queuedHandoverResponseSchema>;

export interface QueuedHandoverAdapter {
  observeApplication?(): QueueObservation[];
  queueIdentity?(): string;
  /** Durable replay and explicitly accepted QoS 0 gaps use distinct proofs. */
  inputBoundary?: {
    kind?: typeof qos0Gap;
    quiesceSource(binding: QueuedHandoverBinding): Promise<string | { closedAt: number }>;
    openTarget(checkpoint: string, binding: QueuedHandoverBinding): Promise<string | { openedAt: number }>;
  };
  drainApplication?(): Promise<void>;
  /** Reestablish the frozen input checkpoint before this replacement may drain. */
  recoverSourceInput?(checkpoint: string, binding: QueuedHandoverBinding): Promise<string>;
  assertTargetEffectsHeld?(): void;
  releaseTargetEffects?(): Promise<void>;
  /** Confirm application readiness after output release, before committing the target receipt. */
  assertTargetReleaseReady?(): void;
  targetCapacityAvailable?(): boolean;
}

export interface QueuedHandoverOutput {
  instanceName: string;
  observeQueuedHandoverOutput(): Promise<QueueObservation | undefined>;
  flush(timeoutMs: number): Promise<void>;
  sealPublishes(): void;
  releaseQueuedHandoverOutput(): Promise<void>;
}

export interface QueuedHandoverManagedOutput {
  identity(): string | undefined;
  targetCapacityAvailable(): boolean;
  assertTargetEffectsHeld(): void;
}

type Action = keyof typeof stageForAction;
const recordSchema = z
  .strictObject({
    binding: queuedHandoverBindingSchema,
    role,
    evidence: z.array(queuedHandoverEvidenceSchema).max(2),
    pending: z.enum(["freeze-source", "activate-target", "drain-source", "release-target"]).nullable(),
    queueIds: z.array(z.string().max(128)).max(64),
  })
  .refine((record) => {
    const expected = record.role === "source" ? ["source-frozen", "source-drained"] : ["target-capturing", "target-released"];
    return record.evidence.every(
      (item, index) =>
        item.phase === expected[index] &&
        matchesBinding(record.binding, item) &&
        item.inputBoundaryDigest === record.evidence[0]?.inputBoundaryDigest &&
        item.closedAt === record.evidence[0]?.closedAt &&
        (record.evidence[0]?.openedAt === undefined || item.openedAt === record.evidence[0].openedAt),
    );
  });
type DurableRecord = z.infer<typeof recordSchema>;
/** Read an exact owner's local receipt after its process has exited. */
export function readQueuedHandoverState(instanceDirectory: string): DurableRecord | undefined {
  const file = path.join(instanceDirectory, QUEUED_HANDOVER_STATE);
  return retainedStoreEntry(file) ? recordSchema.parse(readRetainedStoreJson(file)) : undefined;
}
const FRESH_SAMPLE_MS = 10_000;
const clockSkewMs = 5_000;
const actionTimeoutMs = 30_000;
const actionResponseBudgetMs = 3000;

class Blocked extends Error {
  constructor(readonly reason: QueuedHandoverReason) {
    super(reason);
  }
}
const bindingKeys = Object.keys(queuedHandoverBindingShape) as (keyof QueuedHandoverBinding)[];
function matchesBinding(binding: QueuedHandoverBinding, evidence: QueuedHandoverBinding): boolean {
  return bindingKeys.every((key) => binding[key] === evidence[key]);
}

function qos0BoundaryDigest(binding: QueuedHandoverBinding, closedAt: number): string {
  return retainedStoreDigest({ kind: qos0Gap, binding, closedAt });
}

function fresh(observedAt: number, now: number): boolean {
  return observedAt >= now - FRESH_SAMPLE_MS && observedAt <= now + clockSkewMs;
}

/** A prepared successor must prove all inventoried queues empty before intake opens. */
export function queuedHandoverFreshTargetReason(
  queues: QueuedHandoverStatus["queues"],
  now = Date.now(),
): "progress-unknown" | "target-not-empty" | undefined {
  if (!queues.length || queues.some((queue) => queue.quality !== "exact" || !fresh(queue.observedAt, now))) return "progress-unknown";
  if (queues.some(({ pending, inFlight }) => pending !== 0 || inFlight !== 0)) return "target-not-empty";
}
function normalizeObservation(sample: unknown, owner: "sdk" | "application", now: number) {
  const parsed = queueObservationSchema.parse({ ...(sample as QueueObservation), owner });
  return fresh(parsed.observedAt, now) ? parsed : { ...parsed, pending: null, inFlight: null, quality: "unknown" as const };
}

/** Target launch parameters are supplied by the authenticated host before application startup. */
export function queuedHandoverTargetLaunch(env: NodeJS.ProcessEnv = process.env): QueuedHandoverBinding | undefined {
  const raw = env["UNS_QUEUED_HANDOVER_BINDING"];
  const launchRole = env["UNS_QUEUED_HANDOVER_ROLE"];
  if ((raw === undefined && launchRole === undefined) || (raw === "" && launchRole === "")) return;
  if (launchRole !== "target" || !raw || Buffer.byteLength(raw) > 4096) throw Error("Invalid queued handover target launch");
  const binding = queuedHandoverBindingSchema.parse(JSON.parse(raw));
  if (binding.targetLaunchId !== env["UNS_HANDOVER_LAUNCH_ID"]) throw Error("Queued handover target launch differs");
  return binding;
}

export const queuedHandoverSourceRecoverySchema = z.strictObject({
  protocol: z.literal(1),
  launchId: launch,
  operationId: z.uuid(),
  sourceFrozenDigest: hash,
});
export type QueuedHandoverSourceRecovery = z.infer<typeof queuedHandoverSourceRecoverySchema>;
export function queuedHandoverSourceRecoveryLaunch(env: NodeJS.ProcessEnv = process.env): QueuedHandoverSourceRecovery | undefined {
  const raw = env["UNS_QUEUED_HANDOVER_RECOVERY"];
  if (!raw) return;
  if (Buffer.byteLength(raw) > 4096) throw Error("Invalid queued handover source recovery");
  const recovery = queuedHandoverSourceRecoverySchema.parse(JSON.parse(raw));
  if (recovery.launchId !== env["UNS_HANDOVER_LAUNCH_ID"]) throw Error("Queued handover recovery launch differs");
  return recovery;
}

/** Local proof ledger. Host owns cross-owner authorization, RPC authentication and distributed orchestration. */
export class QueuedHandoverRuntime {
  private readonly file: string;
  private readonly boundary: z.infer<typeof boundary>;
  private readonly sdkOnly: boolean;
  private record?: DurableRecord;
  private busy = false;
  private pendingPreviousDigest?: string | null;
  private sourcePublishSealed = false;
  private resumedFrozenSource = false;
  private drainFailure?: QueuedHandoverReason;
  private listener?: (message: unknown) => void;
  private heldQueueIds?: readonly string[];

  constructor(
    private readonly options: {
      directory: string;
      launchId: string;
      target?: QueuedHandoverBinding;
      recovery?: QueuedHandoverSourceRecovery;
      adapter: QueuedHandoverAdapter;
      outputs(): QueuedHandoverOutput[];
      managedOutput?: QueuedHandoverManagedOutput;
      fenceLegacyHandover?(role: "source" | "target" | "recovery"): void;
      sourceInputActive?(): boolean;
      qos0InputReady?(subscribed: boolean): boolean;
      targetReleased?(): void;
      now?: () => number;
      responseBudgetMs?: number;
    },
  ) {
    launch.parse(options.launchId);
    this.file = path.join(options.directory, QUEUED_HANDOVER_STATE);
    const adapter = options.adapter;
    const applicationEffects =
      adapter.inputBoundary &&
      adapter.queueIdentity &&
      adapter.drainApplication &&
      adapter.assertTargetEffectsHeld &&
      adapter.releaseTargetEffects &&
      adapter.targetCapacityAvailable;
    this.sdkOnly = Boolean(
      options.managedOutput &&
      adapter.inputBoundary &&
      !adapter.queueIdentity &&
      !adapter.observeApplication &&
      !adapter.drainApplication &&
      !adapter.assertTargetEffectsHeld &&
      !adapter.releaseTargetEffects &&
      !adapter.assertTargetReleaseReady &&
      !adapter.targetCapacityAvailable,
    );
    this.boundary = applicationEffects || this.sdkOnly ? (adapter.inputBoundary?.kind ?? "durable-replay-v1") : "none";
    boundary.parse(this.boundary);
    if (adapter.inputBoundary && this.boundary === "none") throw Error("Incomplete durable queued handover adapter");
    const prior = readQueuedHandoverState(options.directory);
    if (options.recovery) {
      const recovery = queuedHandoverSourceRecoverySchema.parse(options.recovery);
      const frozen = prior?.evidence.at(-1);
      if (
        !prior ||
        prior.role !== "source" ||
        (prior.pending && prior.pending !== "drain-source") ||
        frozen?.phase !== "source-frozen" ||
        !adapter.recoverSourceInput ||
        options.target ||
        prior.binding.operationId !== recovery.operationId ||
        recovery.launchId !== options.launchId ||
        prior.binding.sourceLaunchId === recovery.launchId ||
        recovery.sourceFrozenDigest !== queuedHandoverEvidenceDigest(frozen) ||
        prior.binding.sourceQueueIdentity !== this.identity() ||
        Boolean(prior.binding.acceptedInputLoss) !== (this.boundary === qos0Gap)
      )
        throw Error("Queued handover source recovery does not match the durable frozen input");
      options.fenceLegacyHandover?.("recovery");
      this.persist({ ...prior, pending: null });
    } else if (prior) {
      if (prior.pending) throw Error("Prior queued handover remains fenced; reconcile its exact launch");
      if (options.target && prior.role === "target" && !prior.evidence.length && matchesBinding(prior.binding, options.target)) {
        this.record = prior;
      } else if (
        options.target ||
        prior.role !== "target" ||
        prior.evidence.at(-1)?.phase !== "target-released" ||
        this.identity() !== prior.binding.targetQueueIdentity ||
        options.launchId === prior.binding.targetLaunchId
      ) {
        throw Error("Prior queued handover remains fenced; reconcile its exact launch");
      }
    }
    if (options.target) {
      if (
        this.boundary === "none" ||
        Boolean(options.target.acceptedInputLoss) !== (this.boundary === qos0Gap) ||
        options.target.targetLaunchId !== options.launchId ||
        options.target.targetQueueIdentity !== this.identity() ||
        options.target.expiresAt <= this.now
      )
        throw Error("Queued handover target lacks a valid bound replay adapter");
      this.assertTargetEffectsHeld();
      options.fenceLegacyHandover?.("target");
      this.record ??= this.persist({ binding: options.target, role: "target", evidence: [], pending: null, queueIds: [] });
    }
  }

  private get now(): number {
    return this.options.now?.() ?? Date.now();
  }
  private identity(): string | undefined {
    const declared = this.options.adapter.queueIdentity?.();
    const actual = declared ?? this.options.managedOutput?.identity();
    return actual === undefined ? undefined : hash.parse(actual);
  }
  private targetCapacityAvailable(): boolean {
    return this.sdkOnly ? this.options.managedOutput!.targetCapacityAvailable() : this.options.adapter.targetCapacityAvailable!();
  }
  private assertTargetEffectsHeld(): void {
    if (this.sdkOnly) this.options.managedOutput!.assertTargetEffectsHeld();
    else this.options.adapter.assertTargetEffectsHeld!();
  }
  private persist(record: DurableRecord): DurableRecord {
    writeRetainedStoreJson(this.file, recordSchema.parse(record));
    this.record = record;
    return record;
  }

  private assertDurableRecord(record: DurableRecord): void {
    const durable = readQueuedHandoverState(this.options.directory);
    if (!durable || retainedStoreDigest(durable) !== retainedStoreDigest(record)) throw new Blocked("outcome-unknown");
  }
  private async queues(requireApplicationLanes = false) {
    const application = this.options.adapter.observeApplication?.() ?? [];
    if (requireApplicationLanes && this.options.adapter.observeApplication && !application.length) throw new Blocked("progress-unknown");
    if (application.length > 64) throw new Blocked("progress-unknown");
    const outputs = this.options.outputs();
    if (application.length + outputs.length > 64) throw new Blocked("progress-unknown");
    const mqtt = await Promise.all(outputs.map(async (output) => output.observeQueuedHandoverOutput()));
    const now = this.now;
    return queuesSchema.parse([
      ...application.map((sample) => normalizeObservation(sample, "application", now)),
      ...mqtt.flatMap((sample) => (sample ? [normalizeObservation(sample, "sdk", now)] : [])),
    ]);
  }

  private freshTargetReason(queues: QueuedHandoverStatus["queues"]) {
    const ids = queues.map(({ id }) => id).sort();
    if (this.heldQueueIds?.some((id) => !ids.includes(id))) return "progress-unknown";
    const reason = queuedHandoverFreshTargetReason(queues, this.now);
    if (!reason) this.heldQueueIds = ids;
    return reason;
  }

  async status(): Promise<QueuedHandoverStatus> {
    const record = this.record;
    const operationPending = this.busy;
    let queues: QueuedHandoverStatus["queues"] = [];
    let failure: QueuedHandoverReason | undefined;
    try {
      queues = await this.queues(record?.role === "target" && !record.evidence.length && !record.pending);
      if (queues.some(({ quality }) => quality === "unknown")) failure = "progress-unknown";
    } catch {
      failure = "progress-unknown";
    }
    const evidence = record?.evidence.at(-1);
    const phase = record?.pending ? "blocked" : (evidence?.phase ?? (record ? "held" : "ready"));
    if (this.boundary === "none") failure = "input-boundary-unproven";
    if ((phase === "ready" || phase === "target-released") && this.options.sourceInputActive?.() === false) failure ??= "input-boundary-unproven";
    if (
      this.boundary === qos0Gap &&
      (phase === "ready" || phase === "held" || phase === "target-capturing" || phase === "target-released") &&
      this.options.qos0InputReady?.(phase !== "held") === false
    )
      failure ??= "input-boundary-unproven";
    if (record?.pending) failure = operationPending ? "operation-pending" : "outcome-unknown";
    else failure ??= this.drainFailure;
    if (record?.role === "target" && evidence?.phase !== "target-released" && !record.pending) {
      try {
        this.assertTargetEffectsHeld();
        if (phase === "held") failure ??= this.freshTargetReason(queues);
        if (!this.targetCapacityAvailable()) failure ??= "target-capacity-blocked";
      } catch {
        failure = "outcome-unknown";
      }
    }
    const queueIdentity = this.identity();
    if (this.boundary !== "none" && !queueIdentity) failure ??= "progress-unknown";
    return queuedHandoverStatusSchema.parse({
      protocol: 1,
      launchId: this.options.launchId,
      inputBoundary: this.boundary,
      queueIdentity,
      role: record?.role,
      binding: record?.binding,
      phase,
      observedAt: this.now,
      queues,
      evidence,
      reason: failure,
    });
  }

  private async apply(request: Exclude<QueuedHandoverRequest, { action: "status" }>): Promise<QueuedHandoverStatus> {
    if (this.boundary === "none") throw new Blocked("input-boundary-unproven");
    const binding = request.binding;
    const action = request.action;
    if (Boolean(binding.acceptedInputLoss) !== (this.boundary === qos0Gap)) throw new Blocked("input-loss-acceptance-mismatch");
    const owner = action === "freeze-source" || action === "drain-source" ? "source" : "target";
    const recovery = this.options.recovery;
    if (
      (recovery && action !== "drain-source") ||
      (owner === "source" ? (recovery?.launchId ?? binding.sourceLaunchId) : binding.targetLaunchId) !== this.options.launchId
    )
      throw new Blocked("stale-launch");
    const starting = this.record;
    const releasedTarget =
      action === "freeze-source" &&
      starting?.role === "target" &&
      starting.pending === null &&
      starting.evidence.at(-1)?.phase === "target-released" &&
      starting.binding.targetLaunchId === this.options.launchId &&
      starting.binding.targetQueueIdentity === binding.sourceQueueIdentity &&
      starting.binding.operationId !== binding.operationId &&
      starting.binding.sourceLaunchId !== binding.targetLaunchId
        ? starting
        : undefined;
    let record = releasedTarget ? undefined : starting;
    if (record && (record.role !== owner || !matchesBinding(binding, record.binding))) throw new Blocked("request-conflict");
    if (!record && (action !== "freeze-source" || binding.expiresAt <= this.now)) throw new Blocked("request-conflict");
    const previous = action === "freeze-source" ? undefined : request.previous;
    const current = record?.evidence.at(-1);
    const latestExpected =
      action === "freeze-source" || action === "activate-target" ? undefined : action === "drain-source" ? "source-frozen" : "target-capturing";
    const previousDigest = previous ? queuedHandoverEvidenceDigest(previous) : null;
    if (previous) {
      if (
        action === "freeze-source" ||
        !matchesBinding(binding, previous) ||
        previous.phase !== priorForAction[action] ||
        previous.observedAt > this.now + clockSkewMs ||
        (previous.acceptedInputLoss && previous.inputBoundaryDigest !== qos0BoundaryDigest(binding, previous.closedAt!))
      )
        throw new Blocked("invalid-evidence");
    }
    const issued = record?.evidence.find((evidence) => evidence.phase === stageForAction[action]);
    if (action === "release-target" && record?.pending === null && issued?.phase === "target-released") {
      if (issued.previousDigest !== previousDigest) throw new Blocked("invalid-evidence");
      this.assertDurableRecord(record);
      return this.status();
    }
    if (this.busy) {
      if (record?.pending === action && previousDigest === this.pendingPreviousDigest) return this.status();
      throw new Blocked("operation-pending");
    }
    if (record?.pending) throw new Blocked("outcome-unknown");
    if (issued) {
      if (issued.previousDigest !== previousDigest) throw new Blocked("invalid-evidence");
      return this.status();
    }
    if (
      current?.phase !== latestExpected ||
      (current &&
        previous &&
        (previous.previousDigest !== queuedHandoverEvidenceDigest(current) ||
          previous.inputBoundaryDigest !== current.inputBoundaryDigest ||
          previous.closedAt !== current.closedAt ||
          (current.openedAt !== undefined && previous.openedAt !== current.openedAt)))
    )
      throw new Blocked("invalid-evidence");

    this.busy = true;
    this.drainFailure = undefined;
    let background = false;
    try {
      const effects = this.options.adapter;
      const queues = await this.queues(action === "activate-target");
      if (queues.some(({ quality }) => quality === "unknown")) throw new Blocked("progress-unknown");
      if (action === "activate-target") {
        const unready = this.freshTargetReason(queues);
        if (unready) throw new Blocked(unready);
      }
      if (!record && binding.expiresAt <= this.now) throw new Blocked("request-conflict");
      const expectedQueue = owner === "source" ? binding.sourceQueueIdentity : binding.targetQueueIdentity;
      if (expectedQueue !== this.identity()) throw new Blocked("stale-launch");
      if (action === "freeze-source" && this.options.sourceInputActive?.() === false) throw new Blocked("input-boundary-unproven");
      if (this.boundary === qos0Gap && action !== "drain-source" && this.options.qos0InputReady?.(action !== "activate-target") === false)
        throw new Blocked("input-boundary-unproven");
      if (owner === "target") {
        if (action === "activate-target" && !this.targetCapacityAvailable()) throw new Blocked("target-capacity-blocked");
        this.assertTargetEffectsHeld();
      }
      if (releasedTarget) this.assertDurableRecord(releasedTarget);
      if (action === "freeze-source") this.options.fenceLegacyHandover?.("source");
      const queueIds = queues.map(({ id }) => id).sort();
      record ??= { binding, role: owner, evidence: [], pending: null, queueIds };
      const baseline = action === "activate-target" ? { ...record, queueIds } : record;
      this.persist({ ...baseline, pending: action });
      this.pendingPreviousDigest = previousDigest;
      let closedAt = previous?.closedAt;
      let openedAt = previous?.openedAt;
      const startedAt = this.now;
      const input = effects.inputBoundary!;
      const confirmedAt = (value: unknown) => {
        const timestamp = epoch.parse(value);
        if (timestamp < startedAt - clockSkewMs || timestamp > this.now) throw new Blocked("invalid-evidence");
        return timestamp;
      };
      const readDrainLanes = async () => {
        const lanes = await this.queues();
        if (
          JSON.stringify(lanes.map(({ id }) => id).sort()) !== JSON.stringify(baseline.queueIds) ||
          lanes.some(({ quality }) => quality === "unknown")
        )
          throw new Blocked("outcome-unknown");
        return lanes;
      };
      const knownAppEmpty = (lanes: QueuedHandoverStatus["queues"]) => {
        const application = lanes.filter(({ owner }) => owner === "application");
        if (application.some(({ quality, pending, inFlight }) => quality !== "exact" && pending === 0 && inFlight === 0))
          throw new Blocked("outcome-unknown");
        return application.every(({ quality, pending, inFlight }) => quality === "exact" && pending === 0 && inFlight === 0);
      };
      const work = async () => {
        switch (action) {
          case "freeze-source": {
            if (input.kind !== qos0Gap) return hash.parse(await input.quiesceSource(binding));
            const proof = z.strictObject({ closedAt: epoch }).parse(await input.quiesceSource(binding));
            closedAt = confirmedAt(proof.closedAt);
            return qos0BoundaryDigest(binding, closedAt);
          }
          case "activate-target": {
            const expected = request.previous.inputBoundaryDigest;
            if (input.kind === qos0Gap) {
              const proof = z.strictObject({ openedAt: epoch }).parse(await input.openTarget(expected, binding));
              openedAt = confirmedAt(proof.openedAt);
              if (closedAt === undefined || openedAt < closedAt) throw new Blocked("invalid-evidence");
            } else if (hash.parse(await input.openTarget(expected, binding)) !== expected) throw new Blocked("outcome-unknown");
            this.assertTargetEffectsHeld();
            return expected;
          }
          case "drain-source": {
            if (recovery && !this.resumedFrozenSource) {
              const checkpoint = request.previous.inputBoundaryDigest;
              if (hash.parse(await effects.recoverSourceInput!(checkpoint, binding)) !== checkpoint) throw new Blocked("outcome-unknown");
              await Promise.all(this.options.outputs().map((output) => output.releaseQueuedHandoverOutput()));
              this.resumedFrozenSource = true;
            }
            if (!this.sourcePublishSealed) await effects.drainApplication?.();
            const beforeFlush = await readDrainLanes();
            if (!knownAppEmpty(beforeFlush)) throw new Blocked(this.sourcePublishSealed ? "outcome-unknown" : "drain-blocked");
            if (!this.sourcePublishSealed) {
              for (const output of this.options.outputs()) output.sealPublishes();
              this.sourcePublishSealed = true;
            }
            const flushed = await Promise.allSettled(this.options.outputs().map((output) => output.flush(actionTimeoutMs)));
            const drained = await readDrainLanes();
            if (!knownAppEmpty(drained)) throw new Blocked("outcome-unknown");
            if (
              flushed.some(({ status }) => status === "rejected") ||
              drained.some(({ quality, pending, inFlight }) => quality !== "exact" || pending !== 0 || inFlight !== 0)
            )
              throw new Blocked("drain-blocked");
            return request.previous.inputBoundaryDigest;
          }
          case "release-target": {
            await effects.releaseTargetEffects?.();
            await Promise.all(this.options.outputs().map((output) => output.releaseQueuedHandoverOutput()));
            return request.previous.inputBoundaryDigest;
          }
        }
      };
      const finish = async (inputBoundaryDigest: string) => {
        if (this.sdkOnly) await this.queues();
        if (expectedQueue !== this.identity()) throw new Blocked("outcome-unknown");
        const evidence = queuedHandoverEvidenceSchema.parse({
          ...binding,
          phase: stageForAction[action],
          inputBoundaryDigest,
          previousDigest,
          ...(closedAt === undefined ? {} : { closedAt }),
          ...(openedAt === undefined ? {} : { openedAt }),
          observedAt: this.now,
        });
        if (action === "release-target") {
          if (this.boundary === qos0Gap && this.options.qos0InputReady?.(true) === false) throw new Blocked("input-boundary-unproven");
          effects.assertTargetReleaseReady?.();
        }
        this.persist({ ...baseline, evidence: [...baseline.evidence, evidence], pending: null });
        if (action === "release-target") this.options.targetReleased?.();
      };
      background = true;
      const completion = work()
        .then(finish)
        .catch((error: unknown) => {
          if (action === "drain-source" && error instanceof Blocked && error.reason === "drain-blocked") {
            this.persist(baseline);
            this.drainFailure = error.reason;
            return;
          }
          throw error;
        })
        .finally(() => {
          this.pendingPreviousDigest = undefined;
          this.busy = false;
        });
      if (action === "drain-source") {
        void completion.catch(() => {});
      } else {
        // Time bounds the response, never the already-started effect. The
        // same callback keeps its durable intent and can publish proof later.
        let timer: ReturnType<typeof setTimeout> | undefined;
        await Promise.race([
          completion,
          new Promise<void>((resolve) => {
            timer = setTimeout(resolve, this.options.responseBudgetMs ?? actionResponseBudgetMs);
          }),
        ]).finally(() => clearTimeout(timer));
      }
      return this.status();
    } finally {
      if (!background) this.busy = false;
    }
  }

  async respond(raw: unknown): Promise<QueuedHandoverResponse | undefined> {
    const parsed = queuedHandoverRequestSchema.safeParse(raw);
    if (!parsed.success) return;
    const request = parsed.data;
    const base = { type: QUEUED_HANDOVER_RESPONSE_TOPIC, protocol: 1, requestId: request.requestId, launchId: this.options.launchId } as const;
    if (request.launchId !== this.options.launchId) return { ...base, ok: false, reason: "stale-launch" };
    try {
      const status = request.action === "status" ? await this.status() : await this.apply(request);
      return { ...base, ok: true, status };
    } catch (error) {
      return { ...base, ok: false, reason: error instanceof Blocked ? error.reason : "outcome-unknown" };
    }
  }

  listen(): void {
    if (this.listener) return;
    this.listener = (input) => {
      const envelope = input as { type?: string; topic?: string; data?: unknown } | undefined;
      if (envelope?.type !== "process:msg" || envelope.topic !== QUEUED_HANDOVER_TOPIC) return;
      void this.respond(envelope.data)
        .then((data) => {
          if (data && process.connected) process.send?.({ type: "process:msg", topic: QUEUED_HANDOVER_RESPONSE_TOPIC, data }, () => {});
        })
        .catch(() => {});
    };
    process.on("message", this.listener);
  }
  close(): void {
    if (this.listener) process.off("message", this.listener);
    this.listener = undefined;
  }
}
