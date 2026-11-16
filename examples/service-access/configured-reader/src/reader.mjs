import { ServiceTokenProvider } from "@uns-kit/core/tools/auth/service-token-provider.js";
import { loadConfiguredServiceAccessDeclaration } from "@uns-kit/core/runtime/service-access-package.js";
import { deriveConfiguredUnsAccess } from "@uns-kit/core/runtime/configured-service-access.js";

const declaration = await loadConfiguredServiceAccessDeclaration(new URL("../package.json", import.meta.url));
if (!declaration) throw new Error("Configured Service access declaration missing.");
export function sourceTopic(config) {
  const request = deriveConfiguredUnsAccess(declaration, config);
  if (request.policy.accessRules.length !== 1 || /[+#]/.test(request.policy.accessRules[0]))
    throw new Error("This example requires one exact source.");
  return request.policy.accessRules[0];
}

/** A background API read has a workload identity and no human caller token. */
export async function readHistory(controllerBase, from, to, { config, tokenProvider = new ServiceTokenProvider(), fetch: request = fetch, signal } = {}) {
  const topic = sourceTopic(config);
  const token = await tokenProvider.getAccessToken();
  if (!token) throw new Error("Service credential unavailable; review Service access.");
  const response = await request(new URL("/api/catchall/batch/range", controllerBase), {
    method: "POST",
    redirect: "error",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
    body: JSON.stringify({ topics: [topic], from, to, limit: 100 }),
    signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(10000)]) : AbortSignal.timeout(10000),
  });
  if (!response.ok) throw new Error(`UNS history returned HTTP ${response.status}; no credential fallback.`);
  const result = await response.json();
  const rows = result.results?.[0]?.data;
  if (!Array.isArray(rows)) throw new Error("Unexpected history response.");
  return rows;
}
