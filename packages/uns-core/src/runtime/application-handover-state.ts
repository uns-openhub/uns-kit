import { createHash } from 'node:crypto';

/** Versioned application-owned RAM state; SDK owns bounds, peer binding and transport. */
export interface ApplicationHandoverState {
  contract: string;
  exportState(): unknown;
  validateState(value: unknown): void;
  importState(value: unknown): void | Promise<void>;
}

export function validateApplicationHandoverContract(state?: ApplicationHandoverState): void {
  if (!state) return;
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/.test(state.contract) ||
      [state.exportState, state.validateState, state.importState].some(fn => typeof fn !== "function")) {
    throw new TypeError("Invalid application handover contract");
  }
}

/** Plain bounded JSON only. No files, executable code, credentials or implicit truncation. */
export function cloneApplicationHandoverState(value: unknown): unknown {
  let nodes = 0;
  const visit = (item: unknown, depth: number): void => {
    if (++nodes > 50000 || depth > 32) throw new Error("Application handover state exceeds structural bounds");
    if (item === null || typeof item === "string" || typeof item === "boolean") return;
    if (typeof item === "number" && Number.isFinite(item)) return;
    if (!item || typeof item !== "object" || (!Array.isArray(item) && Object.getPrototypeOf(item) !== Object.prototype))
      throw new TypeError("Application handover state must be plain JSON");
    for (const [key, child] of Object.entries(item)) {
      if (["__proto__", "constructor", "prototype"].includes(key)) throw new Error("Unsafe application state key");
      visit(child, depth + 1);
    }
  };
  visit(value, 0);
  const json = JSON.stringify(value);
  if (Buffer.byteLength(json) > 768 * 1024) throw new Error("Application handover state exceeds byte bound");
  return JSON.parse(json);
}

/** Bind application-selected business resources without transmitting their contents. */
export function createApplicationHandoverContract(revision: string, resources: unknown): string {
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,59}$/.test(revision)) throw Error("Invalid application state revision");
  const normalized = cloneApplicationHandoverState(resources);
  const stable = (value: any): any => Array.isArray(value) ? value.map(stable)
    : value && typeof value === 'object' ? Object.fromEntries(Object.keys(value).sort().map(key => [key, stable(value[key])])) : value;
  return revision + '/' + createHash('sha256').update(JSON.stringify(stable(normalized))).digest('hex');
}
