import { afterEach, expect, it, vi } from "vitest";
import { OperatorApiAuthority } from "../src/operator-api-authority.js";

afterEach(() => vi.useRealTimers());

it("bounds callers and occupied slots when an authority ignores abort", async () => {
  vi.useFakeTimers();
  let release!: () => void;
  const stalled = new Promise<void>(resolve => { release = resolve; });
  const fetcher = vi.fn(async () => {
    await stalled;
    return new Response(JSON.stringify({active: true, protocolVersion: 1,
      principalType: "machine", configuredAccessAuthority: 2}));
  }) as unknown as typeof fetch;
  const authority = new OperatorApiAuthority({
    statusUrl: "http://127.0.0.1/status",
    fetch: fetcher,
    deployment: () => { throw new Error("unused"); },
  });
  const request = () => authority.authorize({
    declarations: [], claims: {serviceToken: true}, authorization: "Bearer test", payload: null,
  });
  const calls = Array.from({length: 16}, () => request().catch(error => error));
  await vi.advanceTimersByTimeAsync(1000);
  for (const result of await Promise.all(calls)) expect(result).toMatchObject({status: 503});
  await expect(request()).rejects.toMatchObject({code: "operator_authorization_capacity", status: 503});
  expect(fetcher).toHaveBeenCalledTimes(16);
  release();
  await vi.waitFor(() => expect(request()).resolves.toBeUndefined());
});
