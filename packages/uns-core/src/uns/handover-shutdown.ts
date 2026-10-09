/** Application-owned work is separate from the MQTT subscriber handover. */
export interface HandoverShutdownHooks {
  /** Synchronous: stop admitting new background work when releasing ownership. */
  onRelease?: () => void;
  /** After target acknowledgement: finish accepted work and close writers. Never call process.exit here. */
  drain: () => Promise<void>;
  /** Total time for remaining proxy stops and application drain; default 30 seconds. */
  timeoutMs?: number;
}

export function validateHandoverShutdown(hooks?: HandoverShutdownHooks): void {
  if (!hooks) return;
  if (typeof hooks.drain !== "function" || (hooks.onRelease !== undefined && typeof hooks.onRelease !== "function")) {
    throw new TypeError("Invalid handover shutdown hooks.");
  }
  const timeout = hooks.timeoutMs ?? 30_000;
  if (!Number.isInteger(timeout) || timeout < 1 || timeout > 300_000) {
    throw new RangeError("Handover shutdown timeout must be between 1 and 300000 milliseconds.");
  }
}

export type HandoverShutdownResult = "completed" | "failed" | "timed-out";

/** A timeout does not cancel IO or prove durability. The process owner must exit nonzero. */
export async function runHandoverShutdown(work: () => Promise<void>, timeoutMs = 30_000): Promise<HandoverShutdownResult> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      Promise.resolve()
        .then(work)
        .then(
          () => "completed" as const,
          () => "failed" as const,
        ),
      new Promise<"timed-out">((resolve) => {
        timer = setTimeout(() => resolve("timed-out"), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}
