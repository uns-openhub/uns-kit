/** Frozen source-API bridge for older callers; new launchers use installed descriptors. */
import { configuredGroupProcessNames } from "./process-group-authorization.js";
import { legacyEnergentiDescriptor, normalizeProcessGroupConfiguration } from "./process-group-contract.js";

export function assertLegacyDirectStartAllowed(raw: Record<string, unknown>, configured?: boolean): void {
  const c = normalizeProcessGroupConfiguration(raw, legacyEnergentiDescriptor);
  if (!c.matches || c.journal) return;
  const names =
    configured === undefined
      ? configuredGroupProcessNames()
      : configured
        ? new Set(["uns-energenti-san", "uns-energenti-crno", "uns-energenti-kepware-hv", "uns-energenti-kepware-hv-admission"])
        : new Set<string>();
  if (names.has(String(c.uns?.["processName"])))
    throw Error("Group has a configured admission policy; direct-input originals cannot be started beside retained journal work");
}
