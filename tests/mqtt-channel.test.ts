import { describe, expect, it } from "vitest";
import { mqttChannelParameters, resolveMqttChannel } from "../packages/uns-core/src/index.js";

describe("resolveMqttChannel", () => {
  const infra = {
    host: "mosquitto",
    hosts: ["mosquitto", "mosquitto-backup"],
    servers: [{ host: "mosquitto", port: 8883, protocol: "mqtts" as const }],
    port: 8883,
    protocol: "mqtts" as const,
    username: "service-user",
    password: "service-password",
    clean: false,
    reconnectPeriod: 5_000,
    rejectUnauthorized: true,
    ca: "ca-pem",
    cert: "cert-pem",
    key: "key-pem",
    servername: "broker.internal",
    properties: {
      receiveMaximum: 10,
      userProperties: { source: "infra", region: "local" },
    },
  };

  it("inherits complete infra connection settings for a partial input override", () => {
    const resolved = resolveMqttChannel(infra, {
      clientId: "archiver-input",
      properties: { userProperties: { source: "input" } },
    });

    expect(resolved).toMatchObject({
      host: "mosquitto",
      hosts: ["mosquitto", "mosquitto-backup"],
      servers: [{ host: "mosquitto", port: 8883, protocol: "mqtts" }],
      port: 8883,
      protocol: "mqtts",
      username: "service-user",
      password: "service-password",
      clientId: "archiver-input",
      clean: false,
      reconnectPeriod: 5_000,
      rejectUnauthorized: true,
      ca: "ca-pem",
      cert: "cert-pem",
      key: "key-pem",
      servername: "broker.internal",
      properties: {
        receiveMaximum: 10,
        userProperties: { source: "input", region: "local" },
      },
    });
    expect(mqttChannelParameters(resolved)).not.toHaveProperty("host");
    expect(infra.properties.userProperties).toEqual({ source: "infra", region: "local" });
  });

  it("lets an explicit channel host and credentials replace infra", () => {
    const resolved = resolveMqttChannel(infra, {
      host: "isolated-broker",
      username: "output-user",
      password: "output-password",
      protocol: "mqtt",
      rejectUnauthorized: false,
    });

    expect(resolved.host).toBe("isolated-broker");
    expect(resolved.username).toBe("output-user");
    expect(resolved.password).toBe("output-password");
    expect(resolved.protocol).toBe("mqtt");
    expect(resolved.rejectUnauthorized).toBe(false);
    expect(resolved.ca).toBe("ca-pem");
  });

  it("fails clearly when neither infra nor the channel identifies a broker", () => {
    expect(() => resolveMqttChannel({}, { port: 1883 })).toThrow(/MQTT channel requires infra\.host/);
  });
});
