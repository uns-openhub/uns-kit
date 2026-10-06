import { describe, expect, it, vi } from "vitest";

import type { ITopicObject } from "../packages/uns-core/src/uns/uns-interfaces.js";
import UnsProxy from "../packages/uns-core/src/uns/uns-proxy.js";

class Publisher extends UnsProxy {
  constructor() {
    super();
    this.instanceStatusTopic = "test/";
    this.instanceNameWithSuffix = "test";
  }

  add(attribute: string): void {
    this.registerUniqueTopic({
      topic: "enterprise/site",
      asset: "device",
      objectType: "equipment",
      objectId: "main",
      attribute,
      timestamp: new Date().toISOString(),
    } as ITopicObject);
  }
}

function snapshots(publisher: Publisher): string[][] {
  const values: string[][] = [];
  publisher.event.on("unsProxyProducedTopics", (event) => {
    values.push(event.producedTopics.map((topic) => topic.attribute as string));
  });
  return values;
}

const target = (attribute: string): string => `enterprise/site/device/equipment/main/${attribute}`;

describe("publisher topic lifecycle", () => {
  it("removes exact owned metadata including the last topic and emits an empty snapshot", async () => {
    const publisher = new Publisher();
    const values = snapshots(publisher);
    try {
      publisher.add("temperature");
      publisher.add("state");
      expect(publisher.retainProducedTopics([target("state")])).toBe(1);
      expect(values.at(-1)).toEqual(["state"]);
      expect(publisher.retainProducedTopics([])).toBe(1);
      expect(values.at(-1)).toEqual([]);
      const count = values.length;
      expect(publisher.retainProducedTopics([])).toBe(0);
      expect(values).toHaveLength(count);
    } finally {
      await publisher.stop();
    }
  });

  it("leaves another publisher and unobserved configured targets unaffected", async () => {
    const first = new Publisher();
    const second = new Publisher();
    const values = snapshots(second);
    try {
      first.add("temperature");
      second.add("temperature");
      first.retainProducedTopics([]);
      expect(values.at(-1)).toEqual(["temperature"]);
      expect(second.retainProducedTopics([target("temperature"), target("unseen")])).toBe(0);
      expect(values.at(-1)).toEqual(["temperature"]);
    } finally {
      await first.stop();
      await second.stop();
    }
  });

  it("keeps removed targets absent on subsequent heartbeats and allows explicit re-registration", async () => {
    vi.useFakeTimers();
    const publisher = new Publisher();
    const values = snapshots(publisher);
    try {
      publisher.add("temperature");
      publisher.add("state");
      publisher.retainProducedTopics([target("state")]);
      await vi.advanceTimersByTimeAsync(60_000);
      expect(values.at(-1)).toEqual(["state"]);
      publisher.retainProducedTopics([]);
      const count = values.length;
      await vi.advanceTimersByTimeAsync(120_000);
      expect(values).toHaveLength(count);
      expect(values.at(-1)).toEqual([]);
      publisher.add("temperature");
      expect(values.at(-1)).toEqual(["temperature"]);
    } finally {
      await publisher.stop();
      vi.useRealTimers();
    }
  });

  it("normalizes retained paths, deduplicates them and never invents metadata", async () => {
    const publisher = new Publisher();
    const values = snapshots(publisher);
    try {
      publisher.add("state");
      expect(publisher.retainProducedTopics([` /${target("state")}/ `, target("state"), target("unseen")])).toBe(0);
      expect(values).toEqual([["state"]]);
    } finally {
      await publisher.stop();
    }
  });
});
