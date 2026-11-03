import { OPERATOR_JWT_ALGORITHM, OPERATOR_JWT_AUDIENCE, OPERATOR_JWT_TYPE, OperatorApiError } from "@uns-kit/core/runtime/operator-api.js";
import jwt from "jsonwebtoken";
import { createPublicKey } from "node:crypto";

/** Purpose-specific JWKS uses existing approved service public keys. PS256/type/
 * audience keep these tokens out of the normal RS256 API/GraphQL credential path. */
export class OperatorJwtVerifier {
  private keys: Array<Record<string, unknown>> = [];
  private expires = 0;
  private pending: Promise<void> | undefined;
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
  private async load(): Promise<void> {
    this.pending ??= (async () => {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 1000);
      try {
        const response = await this.fetcher(this.url!, { redirect: "error", signal: controller.signal });
        if (!response.ok || !response.body || Number(response.headers.get("content-length") ?? 0) > 131072) throw new Error("unavailable");
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let size = 0,
          body = "";
        try {
          while (true) {
            const part = await reader.read();
            if (part.done) break;
            size += part.value.byteLength;
            if (size > 131072) {
              await reader.cancel();
              throw new Error("oversize");
            }
            body += decoder.decode(part.value, { stream: true });
          }
          body += decoder.decode();
        } finally {
          reader.releaseLock();
        }
        const document = JSON.parse(body);
        if (!Array.isArray(document.keys) || document.keys.length > 64) throw new Error("invalid");
        this.keys = document.keys;
        this.expires = Date.now() + 15000;
      } catch {
        this.keys = [];
        this.expires = 0;
        throw new OperatorApiError("operator_jwks_unavailable", 503);
      } finally {
        controller.abort();
        clearTimeout(timer);
      }
    })().finally(() => {
      this.pending = undefined;
    });
    return this.pending;
  }
}
