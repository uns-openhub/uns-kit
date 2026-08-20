import { describe, expect, it, vi } from "vitest";

import { ACTIVE_STATUS_EXPIRY_SECONDS, INACTIVE_STATUS_EXPIRY_SECONDS } from "../packages/uns-core/src/uns/process-config.js";
import { StatusMonitor } from "../packages/uns-core/src/uns/status-monitor.js";

describe("StatusMonitor active heartbeat", () => {
  it("retains active state only for its bounded expiry interval", async () => {
    const publish = vi.fn(async () => undefined);
    const monitor = new StatusMonitor({ publish } as any, "uns-infra/example/1/process/", () => true, 10_000, 10_000, {
      processName: "process",
      processId: "instance-a",
    });

    await (monitor as any).publishStatusUpdates();

    expect(publish).toHaveBeenCalledWith("uns-infra/example/1/process/active", expect.any(String), {
      retain: true,
      properties: {
        messageExpiryInterval: ACTIVE_STATUS_EXPIRY_SECONDS,
        userProperties: {
          processName: "process",
          processId: "instance-a",
        },
      },
    });
  });

  it("replaces active state with a short-lived passive heartbeat on shutdown", async () => {
    const publish = vi.fn(async () => undefined);
    const monitor = new StatusMonitor({ publish } as any, "uns-infra/example/1/process/", () => true, 10_000, 10_000);

    await monitor.publishInactiveStatus();

    expect(publish).toHaveBeenCalledWith("uns-infra/example/1/process/active", expect.any(String), {
      retain: true,
      properties: { messageExpiryInterval: INACTIVE_STATUS_EXPIRY_SECONDS },
    });
  });
});
