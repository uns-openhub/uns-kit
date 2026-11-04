import { OPERATOR_JWT_ALGORITHM, OPERATOR_JWT_AUDIENCE, OPERATOR_JWT_TYPE, OperatorApiError } from "@uns-kit/core/runtime/operator-api.js";
import jwt from "jsonwebtoken";
import { createPublicKey } from "node:crypto";

const CACHE_TTL_MS = 15_000;
const FETCH_TIMEOUT_MS = 1_000;
const FAILURE_COOLDOWN_MS = 1_000;
const MAX_DOCUMENT_BYTES = 131_072;
const MAX_FETCH_ATTEMPTS = 2;

class JwksFetchFailure extends Error {
  constructor(readonly retryable: boolean) {
    super("Operator key fetch failed");
  }
}

/** Purpose-specific JWKS uses existing approved service public keys. PS256/type/
 * audience keep these tokens out of the normal RS256 API/GraphQL credential path. */
export class OperatorJwtVerifier {
  private keys: Array<Record<string, unknown>> = [];
  private expires = 0;
  private pending: Promise<void> | undefined;
  private retryAt = 0;
  private transportActive = false;
  constructor(
    private readonly url?: string,
    private readonly fetcher: typeof fetch = fetch,
  ) {}
  async verify(token: string): Promise<Record<string, unknown>> {
    const decoded = jwt.decode(token, { complete: true });
    if (
      !decoded ||
      decoded.header.alg !== OPERATOR_JWT_ALGORITHM ||
      decoded.header.typ !== OPERATOR_JWT_TYPE ||
      typeof decoded.header.kid !== "string" ||
      decoded.header.kid.length > 256 ||
      !this.url
    )
      throw new OperatorApiError("operator_jwt_invalid", 401);
    if (Date.now() >= this.expires || !this.keys.some((k) => k["kid"] === decoded.header.kid)) await this.load();
    const keys = this.keys.filter((k) => k["kid"] === decoded.header.kid && k["kty"] === "RSA" && k["alg"] === OPERATOR_JWT_ALGORITHM && !k["d"]);
    if (keys.length !== 1) throw new OperatorApiError("operator_jwt_key_unavailable", 401);
    try {
      const key = createPublicKey({ key: keys[0] as never, format: "jwk" });
      const claims = jwt.verify(token, key, { algorithms: [OPERATOR_JWT_ALGORITHM], audience: OPERATOR_JWT_AUDIENCE, maxAge: "30s" });
      if (!claims || typeof claims !== "object") throw new Error("invalid");
      return claims;
    } catch {
      throw new OperatorApiError("operator_jwt_invalid", 401);
    }
  }
  private load(): Promise<void> {
    if (this.pending) return this.pending;
    if (Date.now() < this.retryAt) return Promise.reject(new OperatorApiError("operator_jwks_unavailable", 503));
    this.pending = (async () => {
      for (let attempt = 0; attempt < MAX_FETCH_ATTEMPTS; attempt++) {
        try {
          const keys = await this.fetchKeys();
          // Replace trust only with a complete successful response. Failure may
          // retain a still-fresh snapshot, but never extends its 15-second TTL.
          this.keys = keys;
          this.expires = Date.now() + CACHE_TTL_MS;
          this.retryAt = 0;
          return;
        } catch (error) {
          if (!(error instanceof JwksFetchFailure) || !error.retryable || attempt + 1 === MAX_FETCH_ATTEMPTS) break;
          // Retry only the public-key GET, never the operator request or effect.
          // All concurrent verifications share this same bounded recovery.
          await new Promise((resolve) => setTimeout(resolve, 100 + Math.floor(Math.random() * 100)));
        }
      }
      this.retryAt = Date.now() + FAILURE_COOLDOWN_MS;
      throw new OperatorApiError("operator_jwks_unavailable", 503);
    })().finally(() => {
      this.pending = undefined;
    });
    return this.pending;
  }

  private async fetchKeys(): Promise<Array<Record<string, unknown>>> {
    // A transport ignoring abort must not accumulate parallel replacement
    // requests or install a late response after the caller's deadline.
    if (this.transportActive) throw new JwksFetchFailure(false);
    this.transportActive = true;
    const controller = new AbortController();
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    let expired = false;
    let timer: ReturnType<typeof setTimeout>;
    const timeout = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => {
        expired = true;
        controller.abort();
        void reader?.cancel().catch(() => {});
        reject(new JwksFetchFailure(true));
      }, FETCH_TIMEOUT_MS);
    });
    const work = (async () => {
      try {
        const response = await this.fetcher(this.url!, { redirect: "error", signal: controller.signal });
        if (expired || !response.ok || !response.body || Number(response.headers.get("content-length") ?? 0) > MAX_DOCUMENT_BYTES) {
          void response.body?.cancel().catch(() => {});
          throw new JwksFetchFailure(!expired && (response.status === 408 || response.status === 429 || response.status >= 500));
        }
        reader = response.body.getReader();
        const decoder = new TextDecoder();
        let size = 0,
          body = "";
        while (true) {
          const part = await reader.read();
          if (expired) throw new JwksFetchFailure(false);
          if (part.done) break;
          size += part.value.byteLength;
          if (size > MAX_DOCUMENT_BYTES) {
            void reader.cancel().catch(() => {});
            throw new JwksFetchFailure(false);
          }
          body += decoder.decode(part.value, { stream: true });
        }
        body += decoder.decode();
        let document: unknown;
        try {
          document = JSON.parse(body);
        } catch {
          throw new JwksFetchFailure(false);
        }
        if (
          !document ||
          typeof document !== "object" ||
          !("keys" in document) ||
          !Array.isArray(document.keys) ||
          document.keys.length > 64 ||
          document.keys.some((k) => !k || typeof k !== "object" || Array.isArray(k))
        )
          throw new JwksFetchFailure(false);
        return document.keys as Array<Record<string, unknown>>;
      } catch (error) {
        if (error instanceof JwksFetchFailure) throw error;
        throw new JwksFetchFailure(true);
      }
    })();
    const settled = () => {
      this.transportActive = false;
      try {
        reader?.releaseLock();
      } catch {
        /* An aborted stream may still be settling. */
      }
    };
    void work.then(settled, settled);
    try {
      return await Promise.race([work, timeout]);
    } finally {
      clearTimeout(timer!);
      controller.abort();
    }
  }
}
