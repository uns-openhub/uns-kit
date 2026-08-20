// src/uns-config/uns-core-schema.ts
import { z } from "zod";

import { hostValueSchema } from "./host-placeholders.js";
import { secretValueSchema } from "./secret-placeholders.js";

// Zod 4's .int() also enforces Number.isSafeInteger(). Zod 3 accepted every
// finite integer, so multipleOf(1) preserves the existing config contract.
const integerSchema = () => z.number().multipleOf(1);

const mqttProtocolSchema = z.enum(["mqtt", "mqtts", "ws", "wss", "tcp", "ssl"]);

const mqttServerSchema = z
  .object({
    host: hostValueSchema,
    port: integerSchema().positive().optional(),
    protocol: mqttProtocolSchema.optional(),
  })
  .strict();

const mqttConnectPropertiesSchema = z
  .object({
    sessionExpiryInterval: integerSchema().nonnegative().optional(),
    receiveMaximum: integerSchema().positive().optional(),
    maximumPacketSize: integerSchema().positive().optional(),
    topicAliasMaximum: integerSchema().nonnegative().optional(),
    requestResponseInformation: z.boolean().optional(),
    requestProblemInformation: z.boolean().optional(),
    userProperties: z.record(z.string(), z.string()).optional(),
  })
  .strict();

const loggingLevelSchema = z.enum(["error", "warn", "info", "http", "verbose", "debug", "silly"]);

const supervisorSchema = z
  .object({
    enabled: z.boolean().default(false).describe("Enable controller/PM2 supervisor handling for this RTT instance."),
    restartOnExit: z.boolean().default(false).describe("Let PM2 restart the process when it exits unexpectedly."),
    maxMemoryMb: z.number().multipleOf(1).positive().optional().describe("Optional PM2 memory restart limit in megabytes."),
    restartOnUnhealthy: z
      .boolean()
      .default(false)
      .describe("Let the controller auto-start this instance when required system-service runtime signals are absent."),
    unhealthyAfterMs: z
      .number()
      .multipleOf(1)
      .positive()
      .default(60_000)
      .describe("How long runtime signals must stay unhealthy before the controller supervisor can act."),
    restartCooldownMs: z
      .number()
      .multipleOf(1)
      .positive()
      .default(300_000)
      .describe("Minimum time between controller supervisor restart attempts for this instance."),
  })
  .strict()
  .describe("Optional PM2/controller supervisor guard settings for this RTT instance.");

export const mqttChannelSchema = z
  .object({
    host: hostValueSchema.optional(),
    hosts: z.array(hostValueSchema).optional(),
    servers: z.array(mqttServerSchema).optional(),
    port: integerSchema().positive().optional(),
    protocol: mqttProtocolSchema.optional(),
    username: z.string().optional(),
    password: secretValueSchema.optional(),
    clientId: z.string().optional(),
    clean: z.boolean().optional(),
    keepalive: integerSchema().nonnegative().optional(),
    connectTimeout: integerSchema().nonnegative().optional(),
    reconnectPeriod: integerSchema().nonnegative().optional(),
    reconnectOnConnackError: z.boolean().optional(),
    resubscribe: z.boolean().optional(),
    queueQoSZero: z.boolean().optional(),
    rejectUnauthorized: z.boolean().optional(),
    properties: mqttConnectPropertiesSchema.optional(),
    ca: z.string().optional(),
    cert: z.string().optional(),
    key: z.string().optional(),
    servername: z.string().optional(),
  })
  .strict()
  .refine(
    (value) => {
      return !!value.host || (Array.isArray(value.hosts) && value.hosts.length > 0) || (Array.isArray(value.servers) && value.servers.length > 0);
    },
    {
      message: "One of host, hosts, or servers must be provided.",
    },
  );

export const unsCoreSchema = z
  .object({
    uns: z
      .object({
        graphql: z.string().url(),
        rest: z.string().url(),
        token: secretValueSchema.optional().describe("Bearer token used for service-to-service access to the UNS instance."),
        email: z.string().email().optional().describe("Email used when authenticating to graphql endpoint of the UNS instance."),
        password: secretValueSchema.optional().describe("Password or secret value paired with the UNS email."),
        instanceMode: z.enum(["wait", "force", "handover"]).default("wait"),
        processName: z.string().min(1).describe("Process name used in MQTT topics and logs."),
        handover: z.boolean().default(true),
        handoverProtocol: z.literal("correlated-v1").optional().describe("Opt-in controller-correlated MQTT handover protocol."),
        supervisor: supervisorSchema.optional(),
        jwksWellKnownUrl: z.string().url().optional(),
        kidWellKnownUrl: z.string().url().optional(),
        env: z.enum(["dev", "staging", "test", "prod"]).default("dev"),
      })
      .strict(),

    logging: z
      .object({
        adapter: z.string().min(1).default("udp"),
        host: hostValueSchema,
        port: integerSchema().positive().default(12201),
        level: loggingLevelSchema.default("info"),
      })
      .strict()
      .optional(),

    input: mqttChannelSchema.optional(),
    output: mqttChannelSchema.optional(),
    infra: mqttChannelSchema,
    devops: z
      .object({
        provider: z.enum(["azure-devops"]).default("azure-devops"),
        organization: z.string().min(1),
        project: z.string().min(1).optional(),
      })
      .strict()
      .optional(),
  })
  .strict();

export type UnsCore = z.infer<typeof unsCoreSchema>;
