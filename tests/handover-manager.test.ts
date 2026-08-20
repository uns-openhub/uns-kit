import { afterEach, describe, expect, it, vi } from "vitest";

import { HandoverManager } from "../packages/uns-core/src/uns/handover-manager.js";
import { ACTIVE_TIMEOUT, MQTT_UPDATE_INTERVAL, PACKAGE_INFO } from "../packages/uns-core/src/uns/process-config.js";
import { MqttTopicBuilder } from "../packages/uns-core/src/uns-mqtt/mqtt-topic-builder.js";

const ownHandoverTopic = (processName: string) =>
  `uns-infra/${MqttTopicBuilder.sanitizeTopicPart(PACKAGE_INFO.name)}/${MqttTopicBuilder.sanitizeTopicPart(PACKAGE_INFO.version)}/${MqttTopicBuilder.sanitizeTopicPart(processName)}/handover`;

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

  it("adds an optional migration id to new handover requests", async () => {
    vi.useFakeTimers();
    const publish = vi.fn(async () => undefined);
    const manager = new HandoverManager("uns-archiver", "new-instance", { publish } as any, [], true, true, false, "migration-123");

    await manager.handleMqttMessage(activeEvent(false));

    expect(publish).toHaveBeenCalledWith(
      `uns-infra/${PACKAGE_INFO.name}/5.2.9/uns-archiver/handover`,
      JSON.stringify({ type: "handover_intent", handoverId: "migration-123" }),
      expect.objectContaining({
        properties: expect.objectContaining({
          userProperties: expect.objectContaining({ handoverId: "migration-123" }),
        }),
      }),
    );
  });

  it("echoes a new request id from a newer target while retaining legacy source behavior", async () => {
    const publish = vi.fn(async () => undefined);
    const proxy = {
      instanceName: "input",
      setSubscriberPassiveAndDrainQueue: vi.fn(async () => ({
        command: "handover_subscriber",
        instanceName: "input",
        batchSize: 7,
        referenceHash: "abc",
      })),
      stop: vi.fn(async () => undefined),
    };
    const manager = new HandoverManager("uns-archiver", "old-source", { publish } as any, [proxy] as any, false, true, false);
    const handoverTopic = ownHandoverTopic("uns-archiver");

    await manager.handleMqttMessage({
      topic: handoverTopic,
      message: JSON.stringify({ type: "handover_request", handoverId: "migration-123" }),
      packet: {
        properties: {
          responseTopic: "uns-infra/target/next/uns-archiver/handover",
          userProperties: { processId: "new-target", processName: "uns-archiver", handoverId: "migration-123" },
        },
      },
    });

    expect(publish).toHaveBeenCalledWith(
      "uns-infra/target/next/uns-archiver/handover",
      JSON.stringify({
        type: "handover_subscriber",
        batchSize: 7,
        referenceHash: "abc",
        instanceName: "input",
        handoverId: "migration-123",
      }),
      expect.anything(),
    );
    expect(publish).toHaveBeenCalledWith(
      "uns-infra/target/next/uns-archiver/handover",
      JSON.stringify({ type: "handover_fin", handoverId: "migration-123" }),
      expect.anything(),
    );
  });

  it("accepts legacy source completion and emits a correlated acknowledgement", async () => {
    const publish = vi.fn(async () => undefined);
    const proxy = { setPublisherActive: vi.fn(), setSubscriberActive: vi.fn() };
    const manager = new HandoverManager("uns-archiver", "new-target", { publish } as any, [proxy] as any, true, true, false, "migration-123");
    const handoverTopic = ownHandoverTopic("uns-archiver");

    await manager.handleMqttMessage({
      topic: handoverTopic,
      message: JSON.stringify({ type: "handover_fin" }),
      packet: {
        properties: {
          responseTopic: "uns-infra/source/previous/uns-archiver/handover",
          userProperties: { processId: "old-source", processName: "uns-archiver" },
        },
      },
    });

    expect(proxy.setPublisherActive).toHaveBeenCalledOnce();
    expect(proxy.setSubscriberActive).toHaveBeenCalledOnce();
    expect(publish).toHaveBeenCalledWith(
      "uns-infra/source/previous/uns-archiver/handover",
      JSON.stringify({ type: "handover_ack", handoverId: "migration-123" }),
      expect.objectContaining({
        properties: expect.objectContaining({
          userProperties: expect.objectContaining({ handoverId: "migration-123" }),
        }),
      }),
    );
  });
});
