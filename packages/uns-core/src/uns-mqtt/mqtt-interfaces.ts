import type { MqttPublishQueueOptions } from "./publish-queue-contract.js";
import { IClientPublishOptions } from "mqtt";

export interface IMqttParameters {
  /** Opt-in output queue; not a durable MQTT input subscription. */
  publishQueue?: MqttPublishQueueOptions;
  mqttSubToTopics?: string | string[];
  username?: string;
  password?: string;
  mqttSSL?: boolean;
  clientId?: string;
  hosts?: string[];
  servers?: IMqttServerConfig[];
  port?: number;
  protocol?: MqttProtocol;
  keepalive?: number;
  clean?: boolean;
  connectTimeout?: number;
  reconnectPeriod?: number;
  reconnectOnConnackError?: boolean;
  resubscribe?: boolean;
  queueQoSZero?: boolean;
  properties?: IMqttConnectProperties;
  ca?: string;
  cert?: string;
  key?: string;
  servername?: string;
  statusTopic?: string;
  rejectUnauthorized?: boolean;
}

// Narrow alias to avoid leaking mqtt dependency details elsewhere.
export type IMqttPublishOptions = IClientPublishOptions;

export type MqttProtocol = "mqtt" | "mqtts" | "ws" | "wss" | "tcp" | "ssl";

export interface IMqttServerConfig {
  host: string;
  port?: number;
  protocol?: MqttProtocol;
}

export interface IMqttConnectProperties {
  sessionExpiryInterval?: number;
  receiveMaximum?: number;
  maximumPacketSize?: number;
  topicAliasMaximum?: number;
  requestResponseInformation?: boolean;
  requestProblemInformation?: boolean;
  userProperties?: Record<string, string>;
}

export interface HandoverManagerEvents {
  handoverManager: { active: boolean; };
}

export interface IMqttWorkerData {
  publishQueue?: MqttPublishQueueOptions;
  publishQueueIdentity?: string;
  publishThrottlingDelay?: number; // Delay in milliseconds; default is 1ms
  publishConcurrency?: number; // Maximum concurrent outgoing publishes; default is 1
  maxPendingPublishes?: number; // Maximum accepted queued+in-flight publishes; default is unlimited
  subscribeThrottlingDelay?: number; // Delay in milliseconds; default is 1ms
  persistToDisk?: boolean; // Whether to persist the queue to disk; default is false
  mqttHost: string; // MQTT broker host
  instanceNameWithSuffix: string; // Unique instance name for logging
  mqttParameters?: IMqttParameters; // Additional parameters for the MQTT client  
  publisherActive: boolean; // Whether the publisher is active
  subscriberActive: boolean; // Whether the subscriber is active
  defaultPublishOptions?: IMqttPublishOptions; // Default publish options (QoS/retain/etc.)
}
