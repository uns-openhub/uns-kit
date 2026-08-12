import { describe, expect, it, vi } from "vitest";
import { ServiceTokenProvider, UnsClient } from "../packages/uns-core/src/index.js";

describe("ServiceTokenProvider", () => {
  it("uses a controller-managed token file and reloads it after rotation", async () => {
    let fileValue = " first-token ";
    const readFile = vi.fn(async () => fileValue);
    const provider = new ServiceTokenProvider({
      environment: { UNS_SERVICE_TOKEN_FILE: "/run/uns-service-credentials/service.jwt", UNS_SERVICE_TOKEN: "env-token" },
      configToken: "config-token",
      fallback: { getAccessToken: vi.fn(async () => "legacy-token") },
      readFile,
    });

    await expect(provider.getAccessToken()).resolves.toBe("first-token");
    fileValue = "second-token";
    await expect(provider.getAccessToken()).resolves.toBe("second-token");
    expect(readFile).toHaveBeenCalledTimes(2);
  });

  it("does not downgrade to another credential source when the mounted file is unavailable", async () => {
    const fallback = { getAccessToken: vi.fn(async () => "legacy-token") };
    const provider = new ServiceTokenProvider({
      tokenFile: "/run/uns-service-credentials/service.jwt",
      environment: { UNS_SERVICE_TOKEN: "env-token" },
      configToken: "config-token",
      fallback,
      readFile: async () => {
        throw new Error("ENOENT");
      },
    });

    await expect(provider.getAccessToken()).rejects.toThrow("Controller-managed service token file is unavailable.");
    expect(fallback.getAccessToken).not.toHaveBeenCalled();
  });

  it("uses environment, config, then legacy login when no mounted token file is configured", async () => {
    const fallback = { getAccessToken: vi.fn(async () => "legacy-token") };

    await expect(new ServiceTokenProvider({ environment: { UNS_SERVICE_TOKEN: " env-token " }, configToken: "config-token", fallback }).getAccessToken())
      .resolves.toBe("env-token");
    await expect(new ServiceTokenProvider({ environment: {}, configToken: " config-token ", fallback }).getAccessToken())
      .resolves.toBe("config-token");
    await expect(new ServiceTokenProvider({ environment: {}, fallback }).getAccessToken()).resolves.toBe("legacy-token");
    expect(fallback.getAccessToken).toHaveBeenCalledTimes(1);
  });

  it("lets UnsClient authenticate through a token provider", async () => {
    const provider = { getAccessToken: vi.fn(async () => "service-token") };
    const client = new UnsClient("http://controller.local", { tokenProvider: provider });

    await expect(client.ensureToken()).resolves.toBe("service-token");
    expect(provider.getAccessToken).toHaveBeenCalledTimes(1);
  });
});
