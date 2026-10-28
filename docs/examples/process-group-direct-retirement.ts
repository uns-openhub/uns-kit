// Host-agnostic application integration; use actual installed launch paths and resolved configuration.
import {
  type DirectRuntimeContext,
  assertProcessGroupDirectLaunch,
  trackProcessGroupDirectReadiness,
} from "@uns-kit/core/runtime/process-group-direct-runtime.js";

type Runtime = {
  input: { getInputReadiness(): boolean };
  output: { flush(timeoutMs: number): Promise<void>; getConnectionReadiness(): boolean };
  process: { enableMigrationDrain(): void };
};

export async function announceDirectRuntime(context: DirectRuntimeContext, openRuntime: () => Promise<Runtime>) {
  assertProcessGroupDirectLaunch(context); // Call before input is opened in the real startup path.
  const runtime = await openRuntime();
  runtime.process.enableMigrationDrain(); // Register normal standalone cleanup/drain handlers first.
  let active = true;
  let confirmed = false;
  // Input and the configured persistence policy can continue while output is unavailable.
  const confirmation = (async () => {
    while (active) {
      try {
        await runtime.output.flush(1000);
        confirmed = true;
        return;
      } catch {
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  })();
  const stopReadiness = trackProcessGroupDirectReadiness(
    context,
    () => active && confirmed && runtime.input.getInputReadiness() && runtime.output.getConnectionReadiness(),
  );
  return async () => {
    active = false;
    stopReadiness();
    await confirmation;
    // Continue the application's ordinary input stop, output drain and resource cleanup here.
  };
}
