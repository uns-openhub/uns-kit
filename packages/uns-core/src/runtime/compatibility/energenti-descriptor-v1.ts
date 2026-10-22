import type { ProcessGroupRuntimeDescriptor } from "../process-group-contract.js";

export const legacyEnergentiDescriptor: ProcessGroupRuntimeDescriptor = {
  protocolVersion: 1,
  namespace: "energenti",
  publisherPath: ["kepware"],
  journalPath: ["kepwareJournal"],
  runtimeRolePath: ["kepwareRuntimeRole"],
  receiverRole: "admission",
  defaultDataGroup: "kepware",
  match: { path: ["sourceKind"], value: "kepware" },
  manifests: ["hv", "vv", "jek"].map((id) => ({ id, file: `docs/kepware-groups/${id}.json` })),
  legacy: true,
};

/** Frozen legacy declaration lookup; new packages use processGroupRuntime metadata. */
export const legacyDescriptorForPackage = (name: string) => (name === "uns-energenti" ? legacyEnergentiDescriptor : undefined);

/** Reject the historical receiver spelling when its installed capability is absent. */
export const isUnsupportedLegacyReceiver = (raw: Record<string, unknown>) => raw["kepwareRuntimeRole"] === "admission";
export const legacyPilotEnvironment = (localPilot: boolean) => ({ UNS_ENERGENTI_GROUP_PILOT: localPilot ? "1" : "0" });
