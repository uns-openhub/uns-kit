import { describe, expect, it, vi } from "vitest";
import { registerService } from "../packages/uns-core/src/index.js";

describe("registerService", () => {
  it("does nothing for a direct development process without controller RTT identifiers", async () => {
    const client = { post: vi.fn() };

    await expect(registerService({
      client,
      service: { id: "uns-archiver" },
      environment: {},
    })).resolves.toBeNull();
    expect(client.post).not.toHaveBeenCalled();
  });

  it("registers controller-injected instance metadata without sending a credential", async () => {
    const client = {
      post: vi.fn(async () => ({
        registered: true,
        service: { rttNode: "uns-archiver", instanceId: "instance-1", serviceVersion: "5.2.3" },
        controller: { restPath: "/api", graphqlPath: "/graphql" },
      })),
    };

    await expect(registerService({
      client,
      service: {
        id: "uns-archiver",
        version: "5.2.3",
        capabilities: ["uns.archiver", "uns.archiver"],
        healthContract: "uns.service-health/v1",
        processName: "uns-archiver",
      },
      environment: { RTT_NODE: "uns-archiver", RTT_INSTANCE_ID: "instance-1" },
    })).resolves.toMatchObject({
      registered: true,
      service: { rttNode: "uns-archiver", instanceId: "instance-1" },
    });
    expect(client.post).toHaveBeenCalledWith("runtime-services/register", {
      rttNode: "uns-archiver",
      instanceId: "instance-1",
      serviceVersion: "5.2.3",
      capabilities: ["uns.archiver"],
      healthContract: "uns.service-health/v1",
      processName: "uns-archiver",
    });
  });

  it("fails closed when a process claims a different service than its RTT launch metadata", async () => {
    const client = { post: vi.fn() };

    await expect(registerService({
      client,
      service: { id: "uns-api-global" },
      environment: { RTT_NODE: "uns-archiver", RTT_INSTANCE_ID: "instance-1" },
    })).rejects.toThrow("does not match");
    expect(client.post).not.toHaveBeenCalled();
  });
});
