import { isUnsupportedLegacyReceiver } from "./compatibility/energenti-descriptor-v1.js";
import { type ProcessGroupRuntimeDescriptor, isLocalPilotPath, normalizeProcessGroupConfiguration } from "./process-group-contract.js";

/** SDK receiver contract; host launchers translate these neutral settings to PM2/systemd. */
export function processGroupReceiverSupervision(
  raw: Record<string, unknown>,
  descriptor: ProcessGroupRuntimeDescriptor | undefined,
  instanceDirectory: string,
  capabilities: { supervised: boolean; persistent: boolean; authorized: boolean },
  supervisor: { enabled: boolean; restartOnExit: boolean; restartOnUnhealthy: boolean; maxMemoryMb?: number | null },
): Record<string, unknown> {
  if (!descriptor) {
    if (isUnsupportedLegacyReceiver(raw)) throw Error("Installed package has no supported receiver contract");
    return {};
  }
  const c = normalizeProcessGroupConfiguration(raw, descriptor);
  if (c.runtimeRole !== "admission") return {};
  if (!c.matches || !c.journal || (!isLocalPilotPath(instanceDirectory) && !capabilities.authorized))
    throw Error("Admission supervision requires an explicit reviewed journal instance");
  if (!capabilities.supervised) throw Error("Installed package does not support the supervised admission role");
  const directory = (c.journal as Record<string, unknown>)["directory"];
  if (typeof directory === "string" && !isLocalPilotPath(directory) && !capabilities.persistent)
    throw Error("Installed package does not support persistent admission ownership");
  if (!supervisor.enabled || !supervisor.restartOnExit || supervisor.restartOnUnhealthy)
    throw Error("Admission requires restart-on-exit supervision; generic MQTT unhealthy restart is unsupported");
  const max = supervisor.maxMemoryMb ?? 256;
  if (max < 128 || max > 4096) throw Error("Admission supervisor memory limit must be 128–4096 MiB");
  return {
    autorestart: true,
    stop_exit_codes: [78],
    min_uptime: 10000,
    max_restarts: 5,
    exp_backoff_restart_delay: 1000,
    max_memory_restart: `${max}M`,
  };
}
