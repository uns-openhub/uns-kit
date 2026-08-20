import { afterEach, describe, expect, it, vi } from "vitest";

import { HandoverManager } from "../packages/uns-core/src/uns/handover-manager.js";
import { ACTIVE_TIMEOUT, MQTT_UPDATE_INTERVAL, PACKAGE_INFO } from "../packages/uns-core/src/uns/process-config.js";

const activeEvent = (retain: boolean) => ({
  topic: `uns-infra/${PACKAGE_INFO.name}/5.2.9/uns-archiver/active`,
  message: JSON.stringify({ message: { data: { value: 1 } } }),
  packet: {
    retain,
    properties: { userProperties: { processId: "old-instance" } },
  },
});

describe("HandoverManager startup discovery", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("waits for a fresh heartbeat instead of handing over from a retained snapshot", async () => {
    vi.useFakeTimers();
    const publish = vi.fn(async () => undefined);
    const manager = new HandoverManager("uns-archiver", "new-instance", { publish } as any, [], true, true, false);

    await manager.handleMqttMessage(activeEvent(true));

    expect(publish).not.toHaveBeenCalled();
  });

  it("activates after the bounded timeout when a retained snapshot has no fresh heartbeat", async () => {
    vi.useFakeTimers();
    const publisher = { setPublisherActive: vi.fn(), setSubscriberActive: vi.fn() };
    const manager = new HandoverManager(
      "uns-archiver",
      "new-instance",
      { publish: vi.fn(async () => undefined) } as any,
      [publisher] as any,
      true,
      true,
      false,
    );

    await manager.handleMqttMessage(activeEvent(true));
    await vi.advanceTimersByTimeAsync(ACTIVE_TIMEOUT);

    expect(publisher.setPublisherActive).toHaveBeenCalledOnce();
    expect(publisher.setSubscriberActive).toHaveBeenCalledOnce();
  });

  it("leaves enough time for more than one fresh heartbeat before assuming isolation", () => {
    expect(ACTIVE_TIMEOUT).toBeGreaterThan(MQTT_UPDATE_INTERVAL * 2);
  });

  it("requests handover from a fresh active heartbeat across versions", async () => {
    vi.useFakeTimers();
    const publish = vi.fn(async () => undefined);
    const manager = new HandoverManager("uns-archiver", "new-instance", { publish } as any, [], true, true, false);

    await manager.handleMqttMessage(activeEvent(false));

    expect(publish).toHaveBeenCalledWith(
      `uns-infra/${PACKAGE_INFO.name}/5.2.9/uns-archiver/handover`,
      JSON.stringify({ type: "handover_intent" }),
      expect.objectContaining({ retain: false }),
    );
  });
});
