import type {
  IMqttConnectProperties,
  IMqttParameters,
  IMqttServerConfig,
  MqttProtocol,
} from "../uns-mqtt/mqtt-interfaces.js";

/** A config-file MQTT channel before its optional override is merged with infra. */
export type MqttChannelConfig = Pick<
  IMqttParameters,
  | "username"
  | "password"
  | "clientId"
  | "hosts"
  | "servers"
  | "port"
  | "protocol"
  | "keepalive"
  | "clean"
  | "connectTimeout"
  | "reconnectPeriod"
  | "reconnectOnConnackError"
  | "resubscribe"
  | "queueQoSZero"
  | "rejectUnauthorized"
  | "properties"
  | "ca"
  | "cert"
  | "key"
  | "servername"
> & {
  host?: string;
};

export type ResolvedMqttChannel = MqttChannelConfig & {
  host: string;
};

const CHANNEL_FIELDS: Array<keyof MqttChannelConfig> = [
  "host",
  "hosts",
  "servers",
  "port",
  "protocol",
  "username",
  "password",
  "clientId",
  "clean",
  "keepalive",
  "connectTimeout",
  "reconnectPeriod",
  "reconnectOnConnackError",
  "resubscribe",
  "queueQoSZero",
  "rejectUnauthorized",
  "ca",
  "cert",
  "key",
  "servername",
];

/**
 * Resolves `input` or `output` as an overlay on the full `infra` MQTT
 * connection. This preserves credentials, TLS, reconnect behaviour, and
 * multi-host configuration when a service only needs to override one field.
 */
export function resolveMqttChannel(
  infra: MqttChannelConfig,
  override?: MqttChannelConfig,
): ResolvedMqttChannel {
  const resolved: MqttChannelConfig = {};
  for (const field of CHANNEL_FIELDS) {
    const value = override?.[field] ?? infra[field];
    if (value !== undefined) {
      setChannelField(resolved, field, cloneChannelValue(value));
    }
  }

  const infraProperties = infra.properties;
  const overrideProperties = override?.properties;
  if (infraProperties || overrideProperties) {
    resolved.properties = {
      ...(infraProperties ?? {}),
      ...(overrideProperties ?? {}),
      ...(infraProperties?.userProperties || overrideProperties?.userProperties
        ? {
            userProperties: {
              ...(infraProperties?.userProperties ?? {}),
              ...(overrideProperties?.userProperties ?? {}),
            },
          }
        : {}),
    };
  }

  const host = firstNonEmptyString(
    resolved.host,
    resolved.hosts?.[0],
    resolved.servers?.[0]?.host,
  );
  if (!host) {
    throw new Error("MQTT channel requires infra.host, infra.hosts, infra.servers, or an explicit channel host.");
  }
  return { ...resolved, host };
}

/** Returns the connection options accepted by UnsProxyProcess/UnsMqttProxy. */
export function mqttChannelParameters(channel: ResolvedMqttChannel): IMqttParameters {
  const { host: _host, ...parameters } = channel;
  return parameters;
}

function setChannelField(
  target: MqttChannelConfig,
  field: keyof MqttChannelConfig,
  value: unknown,
): void {
  (target as Record<string, unknown>)[field] = value;
}

function cloneChannelValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((entry) =>
      entry && typeof entry === "object" ? { ...(entry as Record<string, unknown>) } : entry,
    );
  }
  return value;
}

function firstNonEmptyString(...values: Array<string | undefined>): string | undefined {
  return values.find((value) => typeof value === "string" && value.trim().length > 0)?.trim();
}

export type { IMqttConnectProperties, IMqttServerConfig, MqttProtocol };
