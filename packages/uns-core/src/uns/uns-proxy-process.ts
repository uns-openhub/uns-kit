import { randomBytes } from "crypto";

import logger from "../logger.js";
import { IMqttParameters } from "../uns-mqtt/mqtt-interfaces.js";
import MqttProxy from "../uns-mqtt/mqtt-proxy.js";
import { MqttTopicBuilder } from "../uns-mqtt/mqtt-topic-builder.js";
import UnsMqttProxy from "../uns-mqtt/uns-mqtt-proxy.js";
import type { UnsMqttProxyStopOptions } from "../uns-mqtt/uns-mqtt-proxy.js";
import { HandoverManager } from "./handover-manager.js";
// Import configuration and initialization modules.
import { MQTT_UPDATE_INTERVAL, PACKAGE_INFO } from "./process-config.js";
import { type UnsServiceMetadata, type UnsServiceMetadataInput, buildUnsServiceMetadata } from "./service-metadata.js";
import { StatusMonitor } from "./status-monitor.js";
import { IUnsMessage, IUnsParameters, IUnsProcessParameters, UnsEvents } from "./uns-interfaces.js";
import { UnsPacket } from "./uns-packet.js";

/**
 * UnsProxyProcess is responsible for managing the process lifecycle,
 * configuring MQTT (subscriptions and publishing for status updates), and
 * handling UNS proxy instances.
 *
 * It leverages HandoverManager for handover events and StatusMonitor for health reporting.
 */
export type UnsProxyProcessPluginMethod = (this: UnsProxyProcess, ...args: unknown[]) => unknown;

export type UnsProxyProcessPluginMethods = Record<string, UnsProxyProcessPluginMethod>;

export interface UnsProxyProcessPluginApi {
  version: number;
  define(methods: UnsProxyProcessPluginMethods): void;
  UnsProxyProcess: typeof UnsProxyProcess;
}

export type UnsProxyProcessPlugin = (api: UnsProxyProcessPluginApi) => void;

class UnsProxyProcess {
  private active: boolean = false;
  private processStatusTopic: string;
  private processName: string;
  private processId: string;
  private unsMqttProxies: UnsMqttProxy[];
  private unsApiProxies: unknown[];
  private processMqttProxy: MqttProxy;
  private handoverManager: HandoverManager;
  private statusMonitor: StatusMonitor;
  private serviceMetadataTopic: string;
  private shutdownPromise: Promise<void> | null = null;

  // Plugin
  static pluginApiVersion = 1;
  private static registered = new Set<UnsProxyProcessPlugin>();
  private static registeredMethodNames = new Set<string>();
  private static applied = false;

  static use(plugin: UnsProxyProcessPlugin) {
    if (this.registered.has(plugin)) return this;
    this.registered.add(plugin);
    if (this.applied) this.applyPlugin(plugin);
    return this;
  }

  private static applyPlugin(plugin: UnsProxyProcessPlugin) {
    const define: UnsProxyProcessPluginApi["define"] = (methods) => {
      for (const [name, fn] of Object.entries(methods)) {
        if (typeof fn !== "function") {
          throw new TypeError(`UnsProxyProcess plugin attempted to register non-function member \"${name}\".`);
        }
        if (this.registeredMethodNames.has(name) || Reflect.has(this.prototype, name)) {
          throw new Error(`UnsProxyProcess plugin attempted to overwrite existing member \"${name}\".`);
        }
        Object.defineProperty(this.prototype, name, {
          value: fn,
          writable: true,
          configurable: true,
          enumerable: false,
        });
        this.registeredMethodNames.add(name);
      }
    };

    plugin({
      define,
      version: this.pluginApiVersion,
      UnsProxyProcess: this,
    });
  }

  private static applyAll() {
    if (this.applied) return;
    this.applied = true;
    for (const p of this.registered) this.applyPlugin(p);
  }

  // References for cleanup.
  private mqttInputHandler: (event: any) => void;

  constructor(mqttHost: string, unsProxyProcessParameters: IUnsProcessParameters) {
    (this.constructor as typeof UnsProxyProcess).applyAll(); // Activate all the plugins

    this.unsMqttProxies = [];
    this.unsApiProxies = [];
    if (!unsProxyProcessParameters?.processName) {
      throw new Error("UnsProxyProcess requires a processName in configuration.");
    }
    this.processName = unsProxyProcessParameters.processName;
    this.processId = randomBytes(16).toString("hex");
    const { name: packageName, version } = PACKAGE_INFO;

    // Instantiate the topic builder.
    const topicBuilder = new MqttTopicBuilder(
      `uns-infra/${MqttTopicBuilder.sanitizeTopicPart(packageName)}/${MqttTopicBuilder.sanitizeTopicPart(version)}/${MqttTopicBuilder.sanitizeTopicPart(this.processName)}/`,
    );

    // Generate topics.
    const processStatusTopic = topicBuilder.getProcessStatusTopic();
    const handoverTopic = topicBuilder.getHandoverTopic();
    const wildcardActiveTopic = topicBuilder.getWildcardActiveTopic();
    const serviceMetadataTopic = topicBuilder.getServiceMetadataTopic();

    this.processStatusTopic = processStatusTopic;
    this.serviceMetadataTopic = serviceMetadataTopic;

    // Configure MQTT topics for subscription.
    const mqttSubToTopics = unsProxyProcessParameters?.mqttSubToTopics ?? [wildcardActiveTopic, handoverTopic];
    const mqttParameters: IMqttParameters = {
      mqttSubToTopics,
      username: unsProxyProcessParameters?.username ?? "",
      password: unsProxyProcessParameters?.password ?? "",
      mqttSSL: unsProxyProcessParameters?.mqttSSL ?? false,
      statusTopic: this.processStatusTopic,
      clientId: unsProxyProcessParameters?.clientId ?? `${this.processName}-${this.processId}`,
      hosts: unsProxyProcessParameters?.hosts,
      servers: unsProxyProcessParameters?.servers,
      port: unsProxyProcessParameters?.port,
      protocol: unsProxyProcessParameters?.protocol,
      keepalive: unsProxyProcessParameters?.keepalive,
      clean: unsProxyProcessParameters?.clean,
      connectTimeout: unsProxyProcessParameters?.connectTimeout,
      reconnectPeriod: unsProxyProcessParameters?.reconnectPeriod,
      reconnectOnConnackError: unsProxyProcessParameters?.reconnectOnConnackError,
      resubscribe: unsProxyProcessParameters?.resubscribe,
      queueQoSZero: unsProxyProcessParameters?.queueQoSZero,
      rejectUnauthorized: unsProxyProcessParameters?.rejectUnauthorized ?? false,
      properties: unsProxyProcessParameters?.properties,
      ca: unsProxyProcessParameters?.ca,
      cert: unsProxyProcessParameters?.cert,
      key: unsProxyProcessParameters?.key,
      servername: unsProxyProcessParameters?.servername,
    };

    // Initialize MQTT proxy and start connection.
    this.processMqttProxy = new MqttProxy(mqttHost, this.processName, mqttParameters);
    this.processMqttProxy.start();

    // Instantiate and start the StatusMonitor for memory and status updates.
    this.statusMonitor = new StatusMonitor(
      this.processMqttProxy,
      this.processStatusTopic,
      () => this.active,
      MQTT_UPDATE_INTERVAL,
      MQTT_UPDATE_INTERVAL,
      { processName: this.processName, processId: this.processId },
    );
    this.statusMonitor.start();
  }

  /**
   * Initializes the HandoverManager instance and sets up event listeners for handover and MQTT input events.
   * Determines handover and force start modes based on process arguments.
   */
  private initHandoverManager(instanceMode: string, handover: boolean) {
    const handoverRequestEnabled = instanceMode == "handover" ? true : false;
    const forceStartEnabled = instanceMode == "force" ? true : false;

    this.handoverManager = new HandoverManager(
      this.processName,
      this.processId,
      this.processMqttProxy,
      this.unsMqttProxies,
      handoverRequestEnabled,
      handover ?? true,
      forceStartEnabled,
    );

    // Listen for handover events.
    this.handoverManager.event.on("handoverManager", (event) => {
      this.active = event.active;
    });

    // Delegate MQTT events to the HandoverManager.
    this.mqttInputHandler = async (event: UnsEvents["input"]) => {
      await this.handoverManager.handleMqttMessage(event);
    };
    this.processMqttProxy.event.on("input", this.mqttInputHandler);
  }

  /**
   * Creates a new UNS proxy instance and stores it for future management.
   */
  public async createUnsMqttProxy(
    mqttHost: string,
    instanceName: string,
    instanceMode: string,
    handover: boolean,
    unsParameters?: IUnsParameters,
  ): Promise<UnsMqttProxy> {
    // Wait until the MQTT connection is established before proceeding.
    await this.waitForProcessConnection();

    // Currently handover manager can handle only mqtt events
    if (!this.handoverManager) {
      this.initHandoverManager(instanceMode, handover);
    }

    const resolvedUnsParameters: IUnsParameters = {
      ...(unsParameters ?? {}),
      clientId: unsParameters?.clientId ?? `${this.processName}-${instanceName}-${this.processId}`,
    };
    const unsMqttProxy = new UnsMqttProxy(mqttHost, this.processName, instanceName, resolvedUnsParameters);

    // Listen for UNS proxy producing topics and publish them via MQTT.
    unsMqttProxy.event.on("unsProxyProducedTopics", (event) => {
      this.processMqttProxy.publish(event.statusTopic, JSON.stringify(event.producedTopics));
    });

    // Listen for UNS proxy status events and publish them via MQTT.
    unsMqttProxy.event.on("mqttProxyStatus", (event) => {
      const time = UnsPacket.formatToISO8601(new Date());
      const unsMessage: IUnsMessage = { data: { time, value: event.value, uom: event.uom } };
      UnsPacket.unsPacketFromUnsMessage(unsMessage).then((packet) => {
        this.processMqttProxy.publish(event.statusTopic, JSON.stringify(packet));
      });
    });

    this.unsMqttProxies.push(unsMqttProxy);
    return unsMqttProxy;
  }

  public async waitForProcessConnection(): Promise<void> {
    while (!this.processMqttProxy?.isConnected) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }

  /**
   * Publishes retained product/system service metadata for controller discovery.
   *
   * The payload is intentionally separate from the volatile active/alive status
   * topics. Controllers can combine this retained identity/capability metadata
   * with live process status to decide whether a service is currently healthy.
   */
  public async publishServiceMetadata(metadata: UnsServiceMetadataInput): Promise<UnsServiceMetadata> {
    await this.waitForProcessConnection();
    const resolved = buildUnsServiceMetadata({
      processName: this.processName,
      processId: this.processId,
      metadata,
    });
    await this.processMqttProxy.publish(this.serviceMetadataTopic, JSON.stringify(resolved), { retain: true });
    return resolved;
  }

  /**
   * Shuts down the process by clearing intervals, timeouts, and removing
   * MQTT event listeners, and stopping the StatusMonitor.
   */
  public shutdown(options: UnsMqttProxyStopOptions = {}): Promise<void> {
    if (this.shutdownPromise) {
      return this.shutdownPromise;
    }

    this.shutdownPromise = this.performShutdown(options);
    return this.shutdownPromise;
  }

  private async performShutdown(options: UnsMqttProxyStopOptions): Promise<void> {
    logger.info(`${this.processName} - Shutting down UnsProxyProcess...`);
    const shutdownErrors: Error[] = [];

    try {
      this.statusMonitor.stop();
      await this.statusMonitor.publishInactiveStatus();
    } catch (e: any) {
      const reason = e instanceof Error ? e : new Error(String(e));
      logger.error(`${this.processName} - Error stopping StatusMonitor or publishing passive state: ${reason.message}`);
      shutdownErrors.push(reason);
    }

    if (this.mqttInputHandler) {
      try {
        this.processMqttProxy.event.off("input", this.mqttInputHandler);
      } catch (e: any) {
        const reason = e instanceof Error ? e : new Error(String(e));
        logger.error(`${this.processName} - Error removing MQTT input handler: ${reason.message}`);
        shutdownErrors.push(reason);
      }
    }

    const proxyStopResults = await Promise.allSettled(this.unsMqttProxies.map((proxy) => proxy.stop(options)));
    for (const result of proxyStopResults) {
      if (result.status === "rejected") {
        const reason = result.reason instanceof Error ? result.reason : new Error(String(result.reason));
        logger.error(`${this.processName} - Error stopping UNS MQTT proxy: ${reason.message}`);
        shutdownErrors.push(reason);
      }
    }

    try {
      if (typeof this.processMqttProxy.stop === "function") {
        await this.processMqttProxy.stop();
      }
    } catch (e: any) {
      const reason = e instanceof Error ? e : new Error(String(e));
      logger.error(`${this.processName} - Error stopping MQTT proxy: ${reason.message}`);
      shutdownErrors.push(reason);
    }

    if (shutdownErrors.length > 0) {
      throw new AggregateError(shutdownErrors, `${this.processName} - Shutdown completed with errors.`);
    }

    logger.info(`${this.processName} - Shutdown complete.`);
  }
}

interface UnsProxyProcess {}

export default UnsProxyProcess;
export type { UnsServiceMetadata, UnsServiceMetadataInput };
export { UnsProxyProcess };
