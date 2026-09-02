import { describe, expect, it } from "vitest";
import { UnsAttributeType } from "../packages/uns-core/src/graphql/schema.ts";
import UnsProxy from "../packages/uns-core/src/uns/uns-proxy.ts";
import type {
  ITopicObject,
  IUnsData,
  UnsEvents,
} from "../packages/uns-core/src/uns/uns-interfaces.ts";

class TestUnsProxy extends UnsProxy {
  constructor() {
    super();
    this.instanceStatusTopic = "uns-infra/test/1/process/instance/";
    this.instanceNameWithSuffix = "test-instance";
  }

  register(topic: ITopicObject): void {
    this.registerUniqueTopic(topic);
  }
}

describe("UNS produced topic metadata", () => {
  it("allows generic relationship evidence fields and scalar arrays", () => {
    const data: IUnsData = {
      time: "2026-06-02T12:00:00.000Z",
      value: ["slab-1122", "slab-1123"],
      currentMaterialObjectId: "slab-1124",
      operationId: "weld-1122-1123",
    };

    expect(data.value).toEqual(["slab-1122", "slab-1123"]);
    expect(data.currentMaterialObjectId).toBe("slab-1124");
  });

  it("publishes counter metadata into the produced topics registry", () => {
    const proxy = new TestUnsProxy();
    let event: UnsEvents["unsProxyProducedTopics"] | null = null;
    proxy.event.on("unsProxyProducedTopics", value => {
      event = value;
    });

    proxy.register({
      timestamp: "2026-06-02T12:00:00.000Z",
      topic: "plant/line/",
      asset: "meter-1",
      objectType: "energy-resource",
      objectId: "main",
      attribute: "active-energy-total",
      attributeType: UnsAttributeType.Data,
      description: "Active energy cumulative total",
      tags: null,
      attributeNeedsPersistence: true,
      dataGroup: "metering",
      valueType: "number",
      presentationKind: "counter",
      defaultAggregation: "last",
      counterResetPolicy: "new-value",
      systemRole: "relationship-evidence",
      relationshipEvidence: {
        relationshipKey: "material-renumbering",
        ownerEndpoint: "target",
        valueEndpoint: "source",
        sourceObjectType: "material",
        targetObjectType: "material",
      },
      lifecycle: {
        timestampFrom: "packetTimestamp",
      },
      tableColumns: [
        {
          name: "active_energy_total",
          valueType: "number",
          presentationKind: "counter",
          defaultAggregation: "last",
          counterResetPolicy: "new-value",
        },
      ],
    });

    expect(event?.producedTopics).toHaveLength(1);
    expect(event?.producedTopics[0]).toMatchObject({
      valueType: "number",
      presentationKind: "counter",
      defaultAggregation: "last",
      counterResetPolicy: "new-value",
      systemRole: "relationship-evidence",
      relationshipEvidence: {
        relationshipKey: "material-renumbering",
        ownerEndpoint: "target",
        valueEndpoint: "source",
        sourceObjectType: "material",
        targetObjectType: "material",
      },
      lifecycle: {
        timestampFrom: "packetTimestamp",
      },
      tableColumns: [
        {
          name: "active_energy_total",
          valueType: "number",
          presentationKind: "counter",
          defaultAggregation: "last",
          counterResetPolicy: "new-value",
        },
      ],
    });
  });

  it("keeps virtual grouping separate from storage dataGroup", () => {
    const proxy = new TestUnsProxy();
    let event: UnsEvents["unsProxyProducedTopics"] | null = null;
    proxy.event.on("unsProxyProducedTopics", value => {
      event = value;
    });

    proxy.register({
      timestamp: "2026-06-16T08:00:00.000Z",
      topic: "sij/acroni/vv/",
      asset: "hrm-stand-1",
      objectType: "material",
      objectId: "slab-001",
      attribute: "location",
      attributeType: UnsAttributeType.Data,
      description: "Material location",
      tags: null,
      attributeNeedsPersistence: true,
      dataGroup: "asset",
      virtualGroup: "material",
    });

    expect(event?.producedTopics).toHaveLength(1);
    expect(event?.producedTopics[0]).toMatchObject({
      dataGroup: "asset",
      virtualGroup: "material",
      objectType: "material",
      objectId: "slab-001",
    });
  });

  it("publishes stable Asset metadata only as an atomic proof-bound set and refreshes rotated proof", () => {
    const proxy = new TestUnsProxy();
    const events: UnsEvents["unsProxyProducedTopics"][] = [];
    proxy.event.on("unsProxyProducedTopics", value => events.push(value));
    const baseTopic = {
      timestamp: "2026-09-02T12:00:00.000Z",
      topic: "enterprise/site-a/line-4/",
      asset: "PACKER-07",
      objectType: "equipment",
      objectId: "main",
      attribute: "state",
      attributeType: UnsAttributeType.Data,
      description: "Packer state",
      tags: null,
      attributeNeedsPersistence: true,
      dataGroup: "",
    } as const;

    proxy.register({
      ...baseTopic,
      assetStableEntityId: "11111111-1111-4111-8111-111111111111",
      assetDisplayName: "Packer 07",
      assetIdentityProof: "proof-1",
    });
    proxy.register({
      ...baseTopic,
      timestamp: "2026-09-02T12:01:00.000Z",
      assetStableEntityId: "11111111-1111-4111-8111-111111111111",
      assetDisplayName: "Packer 07",
      assetIdentityProof: "proof-2",
    });

    expect(events).toHaveLength(2);
    expect(events[1]?.producedTopics[0]).toMatchObject({
      assetStableEntityId: "11111111-1111-4111-8111-111111111111",
      assetDisplayName: "Packer 07",
      assetIdentityProof: "proof-2",
    });
  });

  it("removes a prior identity claim when later publications omit it", () => {
    const proxy = new TestUnsProxy();
    let event: UnsEvents["unsProxyProducedTopics"] | null = null;
    proxy.event.on("unsProxyProducedTopics", value => { event = value; });
    const baseTopic = {
      timestamp: "2026-09-02T12:00:00.000Z",
      topic: "enterprise/site-a/line-4/",
      asset: "PACKER-07",
      objectType: "equipment",
      objectId: "main",
      attribute: "state",
      attributeType: UnsAttributeType.Data,
      description: "Packer state",
      tags: null,
      attributeNeedsPersistence: true,
      dataGroup: "",
    } as const;
    proxy.register({
      ...baseTopic,
      assetStableEntityId: "11111111-1111-4111-8111-111111111111",
      assetIdentityProof: "proof-1",
    });
    proxy.register({ ...baseTopic, timestamp: "2026-09-02T12:02:00.000Z" });

    expect(event?.producedTopics[0]).not.toHaveProperty("assetStableEntityId");
    expect(event?.producedTopics[0]).not.toHaveProperty("assetIdentityProof");
  });
});
