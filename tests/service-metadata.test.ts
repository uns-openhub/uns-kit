import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { MqttTopicBuilder } from "../packages/uns-core/src/uns-mqtt/mqtt-topic-builder.ts";
import { buildUnsServiceMetadata } from "../packages/uns-core/src/uns/service-metadata.ts";

describe("service metadata", () => {
  const fixtures = JSON.parse(readFileSync(new URL("./fixtures/service-metadata-v1.json", import.meta.url), "utf8"));
  for (const fixture of fixtures) {
    it(`matches the shared Python/TypeScript wire fixture for ${fixture.processName}`, () => {
      const value = buildUnsServiceMetadata({
        ...fixture,
        now: new Date(fixture.now),
      });
      expect(JSON.parse(JSON.stringify(value))).toEqual(fixture.expected);
    });
  }
  it("builds a normalized retained service metadata payload", () => {
    const metadata = buildUnsServiceMetadata({
      processName: "uns-bridge-mqtt",
      processId: "process-1",
      now: new Date("2026-05-01T08:00:00.000Z"),
      metadata: {
        serviceId: " uns-bridge-mqtt ",
        kind: "addon",
        addonId: "uns-bridge-mqtt",
        packageName: "uns-bridge-mqtt",
        packageVersion: "1.2.3",
        capabilities: [" addon ", "mqtt-source-browser", "addon"],
        apiRoutes: [
          {
            path: " /api/system/bridge/mqtt/runtime/service/bridge/health ",
            kind: "health",
          },
          {
            path: "   ",
          },
        ],
        controller: {
          name: " controller-a ",
          publicBase: " http://controller-a:3200 ",
        },
      },
    });

    expect(metadata).toMatchObject({
      schemaVersion: 1,
      serviceId: "uns-bridge-mqtt",
      kind: "addon",
      addonId: "uns-bridge-mqtt",
      packageName: "uns-bridge-mqtt",
      packageVersion: "1.2.3",
      processName: "uns-bridge-mqtt",
      processId: "process-1",
      capabilities: ["addon", "mqtt-source-browser"],
      apiRoutes: [
        {
          path: "/api/system/bridge/mqtt/runtime/service/bridge/health",
          kind: "health",
        },
      ],
      controller: {
        name: "controller-a",
        publicBase: "http://controller-a:3200",
      },
      publishedAt: "2026-05-01T08:00:00.000Z",
    });
  });

  it("uses the standard process-scoped service metadata topic", () => {
    const builder = new MqttTopicBuilder("uns-infra/uns-bridge-mqtt/1.2.3/uns-bridge-mqtt/");

    expect(builder.getServiceMetadataTopic()).toBe(
      "uns-infra/uns-bridge-mqtt/1.2.3/uns-bridge-mqtt/service-metadata",
    );
  });

  it("rejects missing service ids", () => {
    expect(() =>
      buildUnsServiceMetadata({
        processName: "process",
        processId: "process-1",
        metadata: {
          serviceId: " ",
          kind: "service",
        },
      }),
    ).toThrow("serviceId");
  });
});
