import { type ConfigurationGroupDescriptor, configurationGroupDescriptor, configurationGroupSnapshot } from "./configuration-group-contract.js";
import {
  type ProcessGroupRuntimeDescriptor,
  normalizeProcessGroupConfiguration,
  processGroupDescriptor,
  readConfigurationSelector,
} from "./process-group-contract.js";

export type RuntimeGroupDescriptor = ProcessGroupRuntimeDescriptor | ConfigurationGroupDescriptor;

/** Pick configuration resources only when explicitly selected; file consumers stay unchanged. */
export function selectedProcessGroupDescriptor(pkg: string, raw: Record<string, unknown>): RuntimeGroupDescriptor | undefined {
  const configured = configurationGroupDescriptor(pkg);
  if (configured && readConfigurationSelector(raw, configured.groupPath) !== undefined) return configured;
  return processGroupDescriptor(pkg);
}

export function normalizeRuntimeProcessGroup(raw: Record<string, unknown>, descriptor: RuntimeGroupDescriptor) {
  if (descriptor.protocolVersion === 1) return { ...normalizeProcessGroupConfiguration(raw, descriptor), configurationSnapshot: undefined };
  const matches = !descriptor.match || readConfigurationSelector(raw, descriptor.match.path) === descriptor.match.value;
  const snapshot = configurationGroupSnapshot(readConfigurationSelector(raw, descriptor.groupPath));
  const runtimeRole = readConfigurationSelector(raw, descriptor.runtimeRolePath);
  const publisher = readConfigurationSelector(raw, descriptor.publisherPath);
  const uns = raw["uns"];
  if (!uns || typeof uns !== "object" || Array.isArray(uns) || !publisher || typeof publisher !== "object" || Array.isArray(publisher))
    throw Error("Explicit process-group publisher and identity required");
  return {
    matches,
    uns: uns as Record<string, unknown>,
    publisher: publisher as Record<string, unknown>,
    journal: readConfigurationSelector(raw, descriptor.journalPath),
    runtimeRole: runtimeRole === descriptor.receiverRole ? "admission" : runtimeRole,
    configurationSnapshot: snapshot,
  };
}
