import { ServiceTokenProvider } from "@uns-kit/core/tools/auth/service-token-provider.js";
import { loadServiceAccessDeclaration } from "@uns-kit/core/runtime/service-access-package.js";

const declaration = await loadServiceAccessDeclaration(new URL("../package.json", import.meta.url));
if (declaration?.permissions.scopes.length !== 1 || declaration.permissions.scopes[0] !== "read:uns" ||
    declaration.permissions.accessRules.length !== 1 || /[+#]/.test(declaration.permissions.accessRules[0]))
  throw new Error("This example requires one exact declared background source.");
export const sourceTopic = declaration.permissions.accessRules[0];

/** A background API read has a workload identity and no human caller token. */
export async function readHistory(controllerBase, from, to, { tokenProvider = new ServiceTokenProvider(), fetch: request = fetch, signal } = {}) {
  const token = await tokenProvider.getAccessToken();
  if (!token) throw new Error("Service credential unavailable; review Service access.");
  const response = await request(new URL("/api/catchall/batch/range", controllerBase), {
    method: "POST",
    redirect: "error",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
    body: JSON.stringify({ topics: [sourceTopic], from, to, limit: 100 }),
    signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(10000)]) : AbortSignal.timeout(10000),
  });
  if (!response.ok) throw new Error(`UNS history returned HTTP ${response.status}; no credential fallback.`);
  const result = await response.json();
  const rows = result.results?.[0]?.data;
  if (!Array.isArray(rows)) throw new Error("Unexpected history response.");
  return rows;
}
