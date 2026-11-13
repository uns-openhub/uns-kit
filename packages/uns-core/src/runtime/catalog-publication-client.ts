import { type AccessTokenProvider, ServiceTokenProvider } from "../tools/auth/service-token-provider.js";
import {
  CatalogAccessError,
  type CatalogInvocation,
  type CatalogOperation,
  type CatalogPublicationManifest,
  assertCatalogRequest,
  assertCatalogResponse,
  catalogAccessDigest,
} from "./catalog-publication.js";

/** No caller-token fallback and no arbitrary source URL or topic. */
export class CatalogPublicationClient {
  constructor(
    private readonly options: {
      controllerBase: string;
      rttNode: string;
      version: string;
      instanceId: string;
      manifest: CatalogPublicationManifest;
      tokenProvider?: AccessTokenProvider;
      fetch?: typeof fetch;
    },
  ) {}
  private async call(
    action: "authorize" | "history",
    operation: CatalogOperation,
    input: Record<string, unknown>,
    callerToken: string,
    sourceId?: string,
    signal?: AbortSignal,
  ): Promise<unknown> {
    assertCatalogRequest(operation, input);
    const token = await (this.options.tokenProvider ?? new ServiceTokenProvider()).getAccessToken();
    if (!token) throw Error("Catalog service credential unavailable.");
    const body: CatalogInvocation = {
      rttNode: this.options.rttNode,
      version: this.options.version,
      instanceId: this.options.instanceId,
      operationId: operation.id,
      manifestDigest: catalogAccessDigest(this.options.manifest),
      input,
      callerToken,
    };
    const response = await (this.options.fetch ?? fetch)(new URL(`/api/catalog-publications/${action}`, this.options.controllerBase), {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
      body: JSON.stringify({ ...body, ...(sourceId ? { sourceId } : {}) }),
      signal: signal ?? AbortSignal.timeout(operation.timeoutMs),
      redirect: "error",
    });
    if (!response.ok)
      throw new CatalogAccessError("catalog_access_denied", [400, 401, 403, 409, 429].includes(response.status) ? response.status : 503);
    if (!response.body) throw new CatalogAccessError("catalog_source_unavailable", 503);
    const reader = response.body.getReader();
    let size = 0,
      text = "";
    const decoder = new TextDecoder();
    try {
      while (true) {
        const r = await reader.read();
        if (r.done) break;
        size += r.value.byteLength;
        if (size > 1048576) {
          await reader.cancel();
          throw new CatalogAccessError("catalog_source_budget", 503);
        }
        text += decoder.decode(r.value, { stream: true });
      }
      text += decoder.decode();
      return JSON.parse(text);
    } finally {
      reader.releaseLock();
    }
  }
  async execute<T>(
    operationId: string,
    input: Record<string, unknown>,
    callerToken: string,
    transform: (context: { signal: AbortSignal; history: (sourceId: string) => Promise<unknown> }) => Promise<T>,
  ): Promise<T> {
    const operation = this.options.manifest.operations.find((op) => op.id === operationId);
    if (!operation) throw Error("Catalog operation not declared.");
    const signal = AbortSignal.timeout(operation.timeoutMs);
    await this.call("authorize", operation, input, callerToken, undefined, signal);
    const work = transform({
      signal,
      history: async (sourceId) => {
        if (!operation.sources.some((source) => source.id === sourceId)) throw Error("Catalog source not declared.");
        return this.call("history", operation, input, callerToken, sourceId, signal);
      },
    });
    let stop: (() => void) | undefined;
    const deadline = new Promise<never>((_resolve, reject) => {
      stop = () => reject(new CatalogAccessError("catalog_execution_timeout", 504));
      if (signal.aborted) stop();
      else signal.addEventListener("abort", stop, { once: true });
    });
    let result: T;
    try {
      result = await Promise.race([work, deadline]);
    } finally {
      if (stop) signal.removeEventListener("abort", stop);
    }
    // Recheck after asynchronous processing, including a cached application result.
    await this.call("authorize", operation, input, callerToken, undefined, signal);
    assertCatalogResponse(operation, result);
    return result;
  }
}
