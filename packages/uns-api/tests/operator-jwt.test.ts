import jwt from "jsonwebtoken";
import { generateKeyPairSync } from "node:crypto";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { OPERATOR_JWT_AUDIENCE, OPERATOR_JWT_TYPE } from "../../uns-core/src/runtime/operator-api.js";
import { OperatorJwtVerifier } from "../src/operator-jwt.js";

const pair = generateKeyPairSync("rsa", { modulusLength: 2048 });
const rotatedPair = generateKeyPairSync("rsa", { modulusLength: 2048 });
const key = { ...pair.publicKey.export({ format: "jwk" }), kid: "shared-operator-key", alg: "PS256", use: "sig" };
const rotatedKey = { ...rotatedPair.publicKey.export({ format: "jwk" }), kid: "rotated-operator-key", alg: "PS256", use: "sig" };
const url = "http://controller.example.invalid/api/auth/operator-jwks.json";
function token(kid = key.kid, privateKey = pair.privateKey) {
  return jwt.sign({ sub: "synthetic-operator" }, privateKey, {
    algorithm: "PS256",
    keyid: kid,
    header: { alg: "PS256", typ: OPERATOR_JWT_TYPE },
    audience: OPERATOR_JWT_AUDIENCE,
    expiresIn: 30,
  });
}
function document(keys: unknown[] = [key]) {
  return Response.json({ keys });
}
function settle<T>(work: Promise<T>) {
  return work.then(
    (value) => ({ value }),
    (error) => ({ error }),
  );
}
beforeEach(() => {
  vi.useFakeTimers();
});
afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("shared operator JWKS recovery", () => {
  it("recovers one transient JWKS failure inside the original verification", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(new Response("unavailable", { status: 503 }))
      .mockResolvedValueOnce(document());
    const verifier = new OperatorJwtVerifier(url, fetcher);
    const verified = settle(verifier.verify(token()));
    await vi.runAllTimersAsync();
    expect(await verified).toMatchObject({ value: { sub: "synthetic-operator" } });
    expect(fetcher).toHaveBeenCalledTimes(2);
    for (const [target, options] of fetcher.mock.calls) {
      expect(target).toBe(url);
      expect(options?.redirect).toBe("error");
      expect(options?.headers).toBeUndefined();
    }
  });

  it("coalesces simultaneous cold reads including the recovery attempt", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(new Response("busy", { status: 502 }))
      .mockResolvedValueOnce(document());
    const verifier = new OperatorJwtVerifier(url, fetcher);
    const encoded = token();
    const verified = settle(Promise.all(Array.from({ length: 24 }, () => verifier.verify(encoded))));
    await vi.runAllTimersAsync();
    expect(await verified).toHaveProperty("value.length", 24);
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it("recovers an aborted first fetch without a second operator request", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockImplementationOnce(
        (_target, options) =>
          new Promise((_resolve, reject) => {
            options?.signal?.addEventListener("abort", () => reject(new Error("simulated transport timeout")), { once: true });
          }),
      )
      .mockResolvedValueOnce(document());
    const verifier = new OperatorJwtVerifier(url, fetcher);
    const verified = settle(verifier.verify(token()));
    await vi.runAllTimersAsync();
    expect(await verified).toMatchObject({ value: { sub: "synthetic-operator" } });
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it("recovers a streamed body that stalls after successful headers", async () => {
    const cancelled = vi.fn();
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode('{"keys":'));
      },
      cancel: cancelled,
    });
    const fetcher = vi.fn<typeof fetch>().mockResolvedValueOnce(new Response(body)).mockResolvedValueOnce(document());
    const verifier = new OperatorJwtVerifier(url, fetcher);
    const verified = settle(verifier.verify(token()));
    await vi.runAllTimersAsync();
    expect(await verified).toMatchObject({ value: { sub: "synthetic-operator" } });
    expect(cancelled).toHaveBeenCalledTimes(1);
    expect(fetcher).toHaveBeenCalledTimes(2);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([408, 429, 500, 502, 504])("recovers transient HTTP %i", async (status) => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValueOnce(new Response("busy", { status })).mockResolvedValueOnce(document());
    const verifier = new OperatorJwtVerifier(url, fetcher);
    const verified = settle(verifier.verify(token()));
    await vi.runAllTimersAsync();
    expect(await verified).toHaveProperty("value");
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it("recovers a connection failure", async () => {
    const fetcher = vi.fn<typeof fetch>().mockRejectedValueOnce(new TypeError("synthetic connection reset")).mockResolvedValueOnce(document());
    const verifier = new OperatorJwtVerifier(url, fetcher);
    const verified = settle(verifier.verify(token()));
    await vi.runAllTimersAsync();
    expect(await verified).toHaveProperty("value");
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it("recovers periodic cache expiry without making every simultaneous caller fetch keys", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(document())
      .mockResolvedValueOnce(new Response("temporarily down", { status: 503 }))
      .mockResolvedValueOnce(document());
    const verifier = new OperatorJwtVerifier(url, fetcher);
    await verifier.verify(token());
    await vi.advanceTimersByTimeAsync(15_000);
    const verified = settle(Promise.all(Array.from({ length: 24 }, () => verifier.verify(token()))));
    await vi.runAllTimersAsync();
    expect(await verified).toHaveProperty("value.length", 24);
    expect(fetcher).toHaveBeenCalledTimes(3);
  });

  it("bounds two consecutive transport timeouts within the shared recovery budget", async () => {
    const fetcher = vi.fn<typeof fetch>().mockImplementation(
      (_target, options) =>
        new Promise((_resolve, reject) => {
          options?.signal?.addEventListener("abort", () => reject(new Error("timeout")), { once: true });
        }),
    );
    const verifier = new OperatorJwtVerifier(url, fetcher);
    const started = Date.now();
    const failed = settle(verifier.verify(token()));
    await vi.runAllTimersAsync();
    expect(await failed).toMatchObject({ error: { code: "operator_jwks_unavailable", status: 503 } });
    expect(fetcher).toHaveBeenCalledTimes(2);
    expect(Date.now() - started).toBeGreaterThanOrEqual(2_100);
    expect(Date.now() - started).toBeLessThan(2_200);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("bounds persistent outages, coalesces retries and resumes after cooldown", async () => {
    let available = false;
    const fetcher = vi.fn<typeof fetch>().mockImplementation(async () => (available ? document() : new Response("down", { status: 503 })));
    const verifier = new OperatorJwtVerifier(url, fetcher);
    const started = Date.now();
    const failed = settle(Promise.all(Array.from({ length: 24 }, () => verifier.verify(token()))));
    await vi.runAllTimersAsync();
    expect(await failed).toMatchObject({ error: { code: "operator_jwks_unavailable", status: 503 } });
    expect(fetcher).toHaveBeenCalledTimes(2);
    expect(Date.now() - started).toBeLessThan(2_200);
    for (let i = 0; i < 24; i++) await expect(verifier.verify(token())).rejects.toMatchObject({ status: 503 });
    expect(fetcher).toHaveBeenCalledTimes(2);
    available = true;
    await vi.advanceTimersByTimeAsync(1_000);
    await expect(verifier.verify(token())).resolves.toMatchObject({ sub: "synthetic-operator" });
    expect(fetcher).toHaveBeenCalledTimes(3);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("never extends expired key trust while the Controller is unavailable", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(document())
      .mockImplementation(async () => new Response("down", { status: 503 }));
    const verifier = new OperatorJwtVerifier(url, fetcher);
    await expect(verifier.verify(token())).resolves.toHaveProperty("sub");
    await vi.advanceTimersByTimeAsync(15_000);
    const failed = settle(verifier.verify(token()));
    await vi.runAllTimersAsync();
    expect(await failed).toMatchObject({ error: { status: 503 } });
    await expect(verifier.verify(token())).rejects.toMatchObject({ status: 503 });
    expect(fetcher).toHaveBeenCalledTimes(3);
  });

  it("an unsuccessful unknown-key refresh does not evict still-fresh trusted keys", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(document())
      .mockImplementation(async () => new Response("down", { status: 503 }));
    const verifier = new OperatorJwtVerifier(url, fetcher);
    const encoded = token();
    await verifier.verify(encoded);
    const failed = settle(verifier.verify(token("unknown-key")));
    await vi.runAllTimersAsync();
    expect(await failed).toMatchObject({ error: { status: 503 } });
    await expect(verifier.verify(encoded)).resolves.toHaveProperty("sub");
    expect(fetcher).toHaveBeenCalledTimes(3);
    // The original deadline still applies despite the failed refresh.
    await vi.advanceTimersByTimeAsync(15_000);
    const expired = settle(verifier.verify(token()));
    await vi.runAllTimersAsync();
    expect(await expired).toMatchObject({ error: { status: 503 } });
  });

  it("accepts a new key and rejects a removed key after an authoritative refresh", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(document())
      .mockImplementation(async () => document([rotatedKey]));
    const verifier = new OperatorJwtVerifier(url, fetcher);
    await verifier.verify(token());
    await expect(verifier.verify(token(rotatedKey.kid, rotatedPair.privateKey))).resolves.toHaveProperty("sub");
    await expect(verifier.verify(token())).rejects.toMatchObject({ code: "operator_jwt_key_unavailable", status: 401 });
    expect(fetcher).toHaveBeenCalledTimes(3);
  });

  it("treats an empty successful key set as removal, never an outage fallback", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(document())
      .mockImplementation(async () => document([]));
    const verifier = new OperatorJwtVerifier(url, fetcher);
    await verifier.verify(token());
    await vi.advanceTimersByTimeAsync(15_000);
    await expect(verifier.verify(token())).rejects.toMatchObject({ code: "operator_jwt_key_unavailable", status: 401 });
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it.each([400, 401, 403, 404])("does not retry a definite JWKS HTTP %i rejection", async (status) => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(new Response("denied", { status }));
    await expect(new OperatorJwtVerifier(url, fetcher).verify(token())).rejects.toMatchObject({ code: "operator_jwks_unavailable", status: 503 });
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([
    ["invalid JSON", () => new Response("{")],
    ["null document", () => Response.json(null)],
    ["non-object key", () => document([null])],
    ["too many keys", () => document(Array.from({ length: 65 }, () => key))],
    ["declared oversize", () => new Response("{}", { headers: { "content-length": "131073" } })],
    ["streamed oversize", () => new Response(" ".repeat(131073))],
  ] as const)("rejects %s without retrying malformed trust data", async (_name, response) => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(response());
    await expect(new OperatorJwtVerifier(url, fetcher).verify(token())).rejects.toMatchObject({ code: "operator_jwks_unavailable", status: 503 });
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([
    ["duplicate kid", [key, key]],
    ["private key", [{ ...key, d: "not-public" }]],
    ["wrong key algorithm", [{ ...key, alg: "RS256" }]],
  ] as const)("does not relax key validation for %s", async (_name, keys) => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(document([...keys]));
    await expect(new OperatorJwtVerifier(url, fetcher).verify(token())).rejects.toMatchObject({ code: "operator_jwt_key_unavailable", status: 401 });
  });

  it("bounds an abort-ignoring fetch, prevents overlap and discards its late response", async () => {
    let resolveLate!: (response: Response) => void;
    const fetcher = vi
      .fn<typeof fetch>()
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveLate = resolve;
          }),
      )
      .mockResolvedValueOnce(document([rotatedKey]));
    const verifier = new OperatorJwtVerifier(url, fetcher);
    const failed = settle(verifier.verify(token()));
    await vi.runAllTimersAsync();
    expect(await failed).toMatchObject({ error: { status: 503 } });
    expect(fetcher).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1_000);
    await expect(verifier.verify(token())).rejects.toMatchObject({ status: 503 });
    expect(fetcher).toHaveBeenCalledTimes(1);
    resolveLate(document());
    await vi.runAllTimersAsync();
    await vi.advanceTimersByTimeAsync(1_000);
    await expect(verifier.verify(token(rotatedKey.kid, rotatedPair.privateKey))).resolves.toHaveProperty("sub");
    expect(fetcher).toHaveBeenCalledTimes(2);
    expect(vi.getTimerCount()).toBe(0);
  });
});
