import { SubscriptionReadiness } from "./subscription-readiness.js";
import mqtt, { IClientOptions, MqttClient } from "mqtt";
import logger from "../logger.js";
import { UnsEvents } from "../uns/uns-interfaces.js";
import { UnsEventEmitter } from "../uns/uns-event-emitter.js";
import { IMqttParameters } from "./mqtt-interfaces.js";
import { MqttWorker } from "./mqtt-worker.js";
import { DataSizeMeasurements, PhysicalMeasurements } from "../uns/uns-measurements.js";

export type MqttStopOptions = {
  silent?: boolean;
  reason?: "retry" | "shutdown";
};

type MqttErrorInfo = {
  code: string | number;
  message: string;
};

export default class MqttProxy {
  public event: UnsEventEmitter<UnsEvents> = new UnsEventEmitter<UnsEvents>();
  public statusTopic: string;
  public instanceName: string;
  private mqttHost: string;
  private mqttSubToTopics: string | string[];
  private mqttSSL: boolean;
  private mqttClient: MqttClient;
  private startDate: Date;
  private mqttParameters: IMqttParameters;
  private statusUpdateInterval: ReturnType<typeof setInterval>;
  private transformationStatsInterval: NodeJS.Timeout | null = null;
  private publishedMessageCount = 0;
  private publishedMessageBytes = 0;
  private subscribedMessageCount = 0;
  private subscribedMessageBytes = 0;
  private mqttWorker: MqttWorker;
  public isConnected = false;
  private subscriptionReadiness = new SubscriptionReadiness();
  private lastSubscriptionReady: boolean | null = null;
  private rejectUnauthorized: boolean;
  private pendingReconnectWait: Promise<void> | null = null;
  private hasEstablishedConnection = false;
  private startupSettled = false;
  private startupResolve: (() => void) | null = null;
  private startupReject: ((error: unknown) => void) | null = null;

  constructor(mqttHost: string, instanceName: string, mqttParameters: IMqttParameters, mqttWorker?: MqttWorker) {
    this.mqttSSL = mqttParameters?.mqttSSL ?? false;
    this.rejectUnauthorized = mqttParameters.rejectUnauthorized ?? false;
    this.mqttSubToTopics = mqttParameters?.mqttSubToTopics ?? [];
    this.mqttHost = mqttHost;
    this.startDate = new Date();
    this.instanceName = instanceName ?? "process";
    this.statusTopic = `${mqttParameters?.statusTopic}`;
    this.mqttParameters = mqttParameters ?? {};
    this.mqttWorker = mqttWorker;
  }

  private resolveProtocol(): IMqttParameters["protocol"] {
    return this.mqttParameters.protocol ?? (this.mqttSSL ? "mqtts" : "mqtt");
  }

  private resolveDefaultPort(protocol: IMqttParameters["protocol"]): number {
    switch (protocol) {
      case "mqtts":
      case "ssl":
        return 8883;
      case "wss":
        return 443;
      case "ws":
        return 80;
      case "tcp":
      case "mqtt":
      default:
        return 1883;
    }
  }

  private ensureStatusUpdateInterval(): void {
    if (this.statusTopic && !this.statusUpdateInterval) {
      this.statusUpdateInterval = setInterval(() => this.emitStatusUpdates(), 10000);
    }
  }

  private ensureTransformationStatsInterval(): void {
    if (!this.transformationStatsInterval) {
      this.transformationStatsInterval = setInterval(() => {
        this.emitTransformationStatistics();
      }, 60000);
    }
  }

  private emitSubscriptionReadiness(force = false): void {
    const ready = this.subscriptionReadiness.ready();
    if (!force && ready === this.lastSubscriptionReady) return;
    this.lastSubscriptionReady = ready;
    this.event.emit("mqttProxyStatus", {event: "mqtt-subscriptions-ready", value: ready ? 1 : 0,
      uom: DataSizeMeasurements.Bit, statusTopic: this.statusTopic + "mqtt-subscriptions-ready"});
  }

  private subscribeToTopics(): void {
    if (this.mqttSubToTopics && this.mqttSubToTopics.length > 0) {
      logger.debug(`${this.instanceName} - Subscribed to ${this.mqttSubToTopics.length} topics.`);
      this.mqttClient.subscribe(this.mqttSubToTopics, { qos: 0 });
    }
  }

  private buildServers(defaultProtocol: IMqttParameters["protocol"]): IClientOptions["servers"] | undefined {
    const servers: IClientOptions["servers"] = [];
    const { port } = this.mqttParameters;
    const protocol = this.mqttParameters.protocol ?? defaultProtocol;

    if (Array.isArray(this.mqttParameters.servers) && this.mqttParameters.servers.length > 0) {
      for (const server of this.mqttParameters.servers) {
        if (!server?.host) continue;
        const resolvedProtocol = server.protocol ?? protocol;
        const resolvedPort =
          typeof server.port === "number"
            ? server.port
            : typeof port === "number"
              ? port
              : this.resolveDefaultPort(resolvedProtocol);
        const entry: { host: string; port: number; protocol?: IMqttParameters["protocol"] } = {
          host: server.host,
          port: resolvedPort,
        };
        if (resolvedProtocol) {
          entry.protocol = resolvedProtocol;
        }
        servers.push(entry);
      }
    } else if (Array.isArray(this.mqttParameters.hosts) && this.mqttParameters.hosts.length > 0) {
      for (const host of this.mqttParameters.hosts) {
        if (!host) continue;
        const resolvedPort = typeof port === "number" ? port : this.resolveDefaultPort(protocol);
        const entry: { host: string; port: number; protocol?: IMqttParameters["protocol"] } = {
          host,
          port: resolvedPort,
        };
        if (protocol) {
          entry.protocol = protocol;
        }
        servers.push(entry);
      }
    }

    return servers.length > 0 ? servers : undefined;
  }

  public async start(): Promise<void> {
    logger.debug(`${this.instanceName} - Connecting to MQTT broker...`);
    this.startupSettled = false;

    return new Promise<void>((resolve, reject) => {
      this.startupResolve = resolve;
      this.startupReject = reject;
      try {
        const username = this.mqttParameters.username;
        const password = this.mqttParameters.password;
        const clientId = this.mqttParameters.clientId ?? this.instanceName;
        const protocol = this.resolveProtocol();

        const options: IClientOptions = {
          username,
          password,
          protocolVersion: 5,
          rejectUnauthorized: this.rejectUnauthorized,
          keepalive: this.mqttParameters.keepalive,
          clean: this.mqttParameters.clean ?? true,
          connectTimeout: this.mqttParameters.connectTimeout,
          reconnectPeriod: this.mqttParameters.reconnectPeriod,
          reconnectOnConnackError: this.mqttParameters.reconnectOnConnackError,
          resubscribe: this.mqttParameters.resubscribe,
          queueQoSZero: this.mqttParameters.queueQoSZero,
          properties: this.mqttParameters.properties,
          ca: this.mqttParameters.ca,
          cert: this.mqttParameters.cert,
          key: this.mqttParameters.key,
          servername: this.mqttParameters.servername,
          
          will: {
            topic: `${this.statusTopic}alive`,
            payload: Buffer.from(""),
            qos: 0,
            retain: true,
            properties: { messageExpiryInterval: 3600 },
          },
          clientId,
        };

        const servers = this.buildServers(protocol);
        if (servers) {
          options.servers = servers;
          options.protocol = protocol;
          this.mqttClient = mqtt.connect(options);
        } else {
          options.host = this.mqttHost;
          options.port = this.mqttParameters.port;
          options.protocol = protocol;
          this.mqttClient = mqtt.connect(options);
        }

        const onConnect = () => this.handleMqttConnect();

        this.mqttClient.on("connect", onConnect);
        this.mqttClient.on("packetsend", packet => {
          this.subscriptionReadiness.sent(packet as Parameters<SubscriptionReadiness["sent"]>[0]);
          this.emitSubscriptionReadiness();
        });
        this.mqttClient.on("packetreceive", packet => {
          this.subscriptionReadiness.received(packet as Parameters<SubscriptionReadiness["received"]>[0]);
          this.emitSubscriptionReadiness();
        });

        this.mqttClient.on("message", (topic: string, message: Buffer, packet: mqtt.IPublishPacket) => {
          try {
            logger.debug(`${this.instanceName} - Message received on topic ${topic}`);
            this.event.emit("input", {
              message: message.toString(),
              topic: topic,
              packet: packet,
            });
            this.updateSubscribeTransformationStats(message.length);
          } catch (error) {
            logger.error(`${this.instanceName} - Error in MQTT message handler: ${error.message}`);
          }
        });

        this.mqttClient.on("error", (error) => {
          const errorInfo = formatMqttError(error);
          logger.error(`${this.instanceName} - MQTT client error: ${errorInfo.message}`);
          this.isConnected = false;
          this.subscriptionReadiness.connection(false);
          this.emitSubscriptionReadiness();
          this.event.emit("error", {
            code: typeof errorInfo.code === "number" ? errorInfo.code : 0,
            message: errorInfo.message,
          });
          reject(error);
        });

        this.mqttClient.on("reconnect", () => {
          logger.debug(`${this.instanceName} - Attempting to reconnect to MQTT broker...`);
        });

        this.mqttClient.on("close", () => {
          logger.debug(`${this.instanceName} - MQTT connection closed.`);
          this.isConnected = false;
          this.subscriptionReadiness.connection(false);
          this.emitSubscriptionReadiness();
        });

        this.mqttClient.on("offline", () => {
          logger.debug(`${this.instanceName} - MQTT client is offline.`);
          this.isConnected = false;
          this.subscriptionReadiness.connection(false);
          this.emitSubscriptionReadiness();
        });

        this.mqttClient.on("end", () => {
          logger.debug(`${this.instanceName} - MQTT client connection ended.`);
          this.isConnected = false;
          this.subscriptionReadiness.connection(false);
          this.emitSubscriptionReadiness();
        });

        this.mqttClient.on("disconnect", (packet) => {
          logger.debug(`${this.instanceName} - MQTT client disconnected. Reason: ${packet?.reasonCode}`);
          this.isConnected = false;
          this.subscriptionReadiness.connection(false);
          this.emitSubscriptionReadiness();
        });
      } catch (error) {
        logger.error(`${this.instanceName} - Error starting MQTT proxy: ${formatMqttError(error).message}`);
        this.isConnected = false;
        this.subscriptionReadiness.connection(false);
        this.emitSubscriptionReadiness();
        reject(error);
      }
    });
  }

  public async publish(topic: string, message: string | Buffer, options?: mqtt.IClientPublishOptions): Promise<void> {
    this.updatePublishTransformationStats(JSON.stringify(message).length);
    const client = this.mqttClient;
    if (!client) {
      throw new Error(`${this.instanceName} - MQTT client missing; cannot publish to ${topic}`);
    }

    if (!client.connected) {
      // Wait briefly for reconnection before giving up.
      await this.waitForReconnect(client, 5000);
    }

    if (!client.connected) {
      throw new Error(`${this.instanceName} - MQTT client still disconnected; publish failed for ${topic}`);
    }

    return new Promise((resolve, reject) => {
      client.publish(topic, message, options || {}, (err) => {
        if (err) {
          logger.error(`${this.instanceName} - Error publishing to topic ${topic}: ${err.message}`);
          return reject(err);
        }
        logger.debug(`${this.instanceName} - Published message to topic ${topic}`);
        resolve();
      });
    });
  }

  public async subscribeAsync(topic: string | string[], options?: mqtt.IClientSubscribeOptions): Promise<mqtt.ISubscriptionGrant[]> {
    return this.mqttClient.subscribeAsync(topic, options);
  }

  public async unsubscribeAsync(topic: string | string[]): Promise<mqtt.Packet | undefined> {
    const topics = Array.isArray(topic) ? topic.filter((t) => typeof t === "string" && t.length > 0) : [topic].filter((t): t is string => typeof t === "string" && t.length > 0);
    if (topics.length === 0) {
      logger.warn(`${this.instanceName} - unsubscribeAsync called with empty topic list; skipping.`);
      return Promise.resolve(undefined);
    }
    return this.mqttClient.unsubscribeAsync(topics);
  }

  public stop(options: MqttStopOptions = {}): Promise<void> {
    const silent = options.silent === true || options.reason === "retry";
    const shouldLogDisconnect = !silent && this.hasEstablishedConnection;

    if (shouldLogDisconnect) {
      logger.debug(`${this.instanceName} - Disconnecting from MQTT broker...`);
    }

    return new Promise((resolve, reject) => {
      const resetState = () => {
        this.isConnected = false;
        this.subscriptionReadiness.connection(false);
        this.emitSubscriptionReadiness();
        this.pendingReconnectWait = null;
        this.startupSettled = false;
        this.hasEstablishedConnection = false;
        this.startupResolve = null;
        this.startupReject = null;
      };

      try {
        if (this.statusUpdateInterval) {
          clearInterval(this.statusUpdateInterval);
          this.statusUpdateInterval = null;
        }
        if (this.transformationStatsInterval) {
          clearInterval(this.transformationStatsInterval);
          this.transformationStatsInterval = null;
        }
        if (this.mqttClient) {
          this.mqttClient.end(false, () => {
            resetState();
            if (shouldLogDisconnect) {
              logger.debug(`${this.instanceName} - Disconnected from MQTT broker.`);
            }
            resolve();
          });
        } else {
          resetState();
          resolve();
        }
      } catch (error) {
        resetState();
        const reason = error instanceof Error ? error : new Error(String(error));
        if (!silent) {
          logger.error(`${this.instanceName} - Error during stop: ${formatMqttError(reason).message}`);
        }
        reject(reason);
      }
    });
  }

  private handleMqttConnect(): void {
    const resolve = this.startupResolve;
    const reject = this.startupReject;
    if (!resolve || !reject) {
      return;
    }

    try {
      this.mqttClient.stream.setMaxListeners(0);
      const isFirstConnect = !this.startupSettled;
      this.startupSettled = true;
      this.hasEstablishedConnection = true;
      this.isConnected = true;
      this.subscriptionReadiness.connection(true);
      this.emitSubscriptionReadiness();

      if (isFirstConnect) {
        logger.info(`${this.instanceName} - Connected to MQTT broker at ${this.mqttHost}`);
        this.subscribeToTopics();
        this.ensureStatusUpdateInterval();
        this.ensureTransformationStatsInterval();
        resolve();
        return;
      }

      logger.info(`${this.instanceName} - Reconnected to MQTT broker at ${this.mqttHost}`);
      if (this.mqttParameters.resubscribe === false) {
        this.subscribeToTopics();
      }
      this.ensureStatusUpdateInterval();
      this.ensureTransformationStatsInterval();
    } catch (error) {
      logger.error(`${this.instanceName} - Error in MQTT connect handler: ${formatMqttError(error).message}`);
      this.isConnected = false;
      this.subscriptionReadiness.connection(false);
      this.emitSubscriptionReadiness();
      reject(error);
    }
  }

  private emitStatusUpdates() {
    try {
      this.emitSubscriptionReadiness(true);
      const uptime = Math.round((new Date().getTime() - this.startDate.getTime()) / 60000);
      this.event.emit("mqttProxyStatus", { event: "uptime", value: uptime, uom: PhysicalMeasurements.Minute, statusTopic: this.statusTopic + "uptime" });
      this.event.emit("mqttProxyStatus", { event: "alive", value: 1, uom: DataSizeMeasurements.Bit, statusTopic: this.statusTopic + "alive" });

      if (this.mqttWorker !== undefined) {
        const tpValue = this.mqttWorker.getPublisherState() ? 1 : 0;
        this.event.emit("mqttProxyStatus", { event: "t-publisher-active", value: tpValue, uom: DataSizeMeasurements.Bit, statusTopic: this.statusTopic + "t-publisher-active" });
        const tsValue = this.mqttWorker.getSubscriberState() ? 1 : 0;
        this.event.emit("mqttProxyStatus", { event: "t-subscriber-active", value: tsValue, uom: DataSizeMeasurements.Bit, statusTopic: this.statusTopic + "t-subscriber-active" });
      }
    } catch (error) {
      logger.error(`${this.instanceName} - Error publishing MQTT status: ${error.message}`);
    }
  }

  private updatePublishTransformationStats(messageSizeOut: number): void {
    this.publishedMessageCount += 1;
    this.publishedMessageBytes += messageSizeOut;
  }

  private updateSubscribeTransformationStats(messageSizeIn: number): void {
    this.subscribedMessageCount += 1;
    this.subscribedMessageBytes += messageSizeIn;
  }

  private waitForReconnect(client: mqtt.MqttClient, timeoutMs: number): Promise<void> {
    if (client.connected) {
      return Promise.resolve();
    }

    if (this.pendingReconnectWait) {
      return this.pendingReconnectWait;
    }

    this.pendingReconnectWait = new Promise<void>((resolve) => {
      if (client.connected) return resolve();

      const onConnect = () => {
        cleanup();
        resolve();
      };
      const onClose = () => {
        // stay waiting; close is expected during reconnect attempts
      };
      const cleanup = () => {
        client.off("connect", onConnect);
        client.off("close", onClose);
      };

      client.on("connect", onConnect);
      client.on("close", onClose);

      setTimeout(() => {
        cleanup();
        resolve();
      }, timeoutMs).unref?.();
    }).finally(() => {
      this.pendingReconnectWait = null;
    });

    return this.pendingReconnectWait;
  }

  private async emitTransformationStatistics(): Promise<void> {
    if (this.statusTopic !== "") {
      try {
        this.event.emit("mqttProxyStatus", {
          event: "published-message-count",
          value: this.publishedMessageCount,
          uom: "",
          statusTopic: this.statusTopic + "published-message-count",
        });
        this.event.emit("mqttProxyStatus", {
          event: "published-message-bytes",
          value: Math.round(this.publishedMessageBytes / 1024),
          uom: DataSizeMeasurements.KiloByte,
          statusTopic: this.statusTopic + "published-message-bytes",
        });
        this.event.emit("mqttProxyStatus", {
          event: "subscribed-message-count",
          value: this.subscribedMessageCount,
          uom: "",
          statusTopic: this.statusTopic + "subscribed-message-count",
        });
        this.event.emit("mqttProxyStatus", {
          event: "subscribed-message-bytes",
          value: Math.round(this.subscribedMessageBytes / 1024),
          uom: DataSizeMeasurements.KiloByte,
          statusTopic: this.statusTopic + "subscribed-message-bytes",
        });
        this.publishedMessageCount = 0;
        this.publishedMessageBytes = 0;
        this.subscribedMessageCount = 0;
        this.subscribedMessageBytes = 0;
      } catch (error: any) {
        this.publishedMessageCount = 0;
        this.publishedMessageBytes = 0;
        this.subscribedMessageCount = 0;
        this.subscribedMessageBytes = 0;
        logger.error(`${this.instanceName} - Error emitting transformation statistics: ${error.message}`);
      }
    }
  }
}

export function formatMqttError(error: unknown): MqttErrorInfo {
  const parts: string[] = [];
  const codes: Array<string | number> = [];
  collectMqttErrorParts(error, parts, codes);

  const uniqueParts = Array.from(new Set(parts.map((part) => part.trim()).filter(Boolean)));
  const message = uniqueParts.join("; ") || "Unknown MQTT error";

  return {
    code: codes[0] ?? 0,
    message,
  };
}

function collectMqttErrorParts(
  error: unknown,
  parts: string[],
  codes: Array<string | number>,
): void {
  if (error instanceof AggregateError) {
    if (typeof error.message === "string" && error.message.trim()) {
      parts.push(error.message.trim());
    }
    for (const nested of error.errors) {
      collectMqttErrorParts(nested, parts, codes);
    }
    return;
  }

  if (typeof error === "string") {
    if (error.trim()) {
      parts.push(error.trim());
    }
    return;
  }

  if (!error || typeof error !== "object") {
    return;
  }

  const record = error as {
    message?: unknown;
    name?: unknown;
    code?: unknown;
    address?: unknown;
    port?: unknown;
    cause?: unknown;
  };

  const code =
    typeof record.code === "string" || typeof record.code === "number"
      ? record.code
      : undefined;
  if (code !== undefined) {
    codes.push(code);
  }

  const message =
    typeof record.message === "string" && record.message.trim()
      ? record.message.trim()
      : undefined;
  const address =
    typeof record.address === "string" && record.address.trim()
      ? record.address.trim()
      : undefined;
  const port = typeof record.port === "number" ? record.port : undefined;
  const endpoint =
    address || port !== undefined
      ? `${address ?? ""}${address && port !== undefined ? ":" : ""}${port ?? ""}`.trim()
      : undefined;

  const detailParts = [
    code !== undefined && !message?.includes(String(code)) ? String(code) : undefined,
    message,
    endpoint && !message?.includes(endpoint) ? endpoint : undefined,
  ].filter((part): part is string => typeof part === "string" && part.length > 0);

  if (detailParts.length > 0) {
    parts.push(detailParts.join(" "));
  } else if (typeof record.name === "string" && record.name.trim()) {
    parts.push(record.name.trim());
  }

  if (record.cause !== undefined) {
    collectMqttErrorParts(record.cause, parts, codes);
  }
}
