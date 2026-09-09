import { readFileSync } from "fs";
import { IClientPublishOptions } from "mqtt";
import * as path from "path";
import { Worker } from "worker_threads";
import { fileURLToPath } from "url";
import { basePath } from "../base-path.js";
import logger from "../logger.js";
import { IMqttPublishRequest, IUnsLifecycleMetadata, IUnsMessage, IUnsPacket, IUnsParameters, IUnsRelationshipEvidenceMetadata, IUnsTableColumnMetadata, UnsAttribute, UnsAttributeSystemRole, UnsEvents, ValueType } from "../uns/uns-interfaces.js";
import { getObjectTypeDescription, type UnsObjectId, type UnsObjectType } from "../uns/uns-object.js";
import type { UnsAsset } from "../uns/uns-asset.js";
import { MeasurementUnit } from "../uns/uns-measurements.js";
import { UnsPacket } from "../uns/uns-packet.js";
import { IMqttParameters, IMqttWorkerData } from "./mqtt-interfaces.js";
import { MqttTopicBuilder } from "./mqtt-topic-builder.js";
import UnsProxy from "../uns/uns-proxy.js";
import { UnsAttributeType } from "../graphql/schema.js";
import { getAttributeDescription } from "../uns/uns-attributes.js";
import { UnsTags } from "../uns/uns-tags.js";
import { UnsTopics } from "../uns/uns-topics.js";

const packageJsonPath = path.join(basePath, "package.json");
const packageJson = JSON.parse(readFileSync(packageJsonPath, "utf8"));

const moduleDirectory = path.dirname(fileURLToPath(import.meta.url));
const packageRoot = path.resolve(moduleDirectory, "..", "..");
const workerScriptPath = path.join(packageRoot, "dist/uns-mqtt/mqtt-worker-init.js");

export enum MessageMode {
  Raw = 'raw',     // Send only the original message
  /**
   * @deprecated Producer-side delta calculation loses state across restarts.
   * Publish raw cumulative counter values and request delta/rate from Datahub history APIs.
   */
  Delta = 'delta', // Send only the delta message
  /**
   * @deprecated Producer-side delta calculation loses state across restarts.
   * Publish raw cumulative counter values and request delta/rate from Datahub history APIs.
   */
  Both = 'both'    // Send both the original and delta messages
}

export interface UnsMqttProxyStopOptions {
  drain?: boolean;
  timeoutMs?: number;
}

type UnsMqttProxyLifecycleState = "running" | "stopping" | "stopped";

type InternalMqttMessage = {
  topic: UnsTopics;
  attribute: UnsAttribute;
  asset: UnsAsset;
  assetDescription?: string;
  assetStableEntityId?: string;
  assetDisplayName?: string;
  assetIdentityProof?: string;
  assetProviderIdentity?: {
    providerId: string;
    externalSystem: string;
    externalType: string;
    externalId: string;
  };
  assetProviderIdentityProof?: string;
  objectType: UnsObjectType;
  objectTypeDescription?: string;
  objectId: UnsObjectId;
  virtualGroup?: string;
  description?: string;
  tags?: UnsTags[];
  attributeNeedsPersistence?: boolean | null;
  valueType?: string;
  presentationKind?: string;
  defaultAggregation?: string;
  counterResetPolicy?: string;
  tableColumns?: IUnsTableColumnMetadata[];
  systemRole?: UnsAttributeSystemRole;
  relationshipEvidence?: IUnsRelationshipEvidenceMetadata;
  lifecycle?: IUnsLifecycleMetadata;
  validityMode?: "interval" | "lifecycle" | "static";
  expectedIntervalMs?: number;
  lifecycleEndValue?: string;
  packet: IUnsPacket;
};

export default class UnsMqttProxy extends UnsProxy {
  public static readonly DEFAULT_DRAIN_TIMEOUT_MS = 30_000;
  private lastValues: Map<string, { value: ValueType; uom: string; timestamp: Date }> = new Map();
  private worker: Worker;
  private pendingEnqueues: Map<string, { resolve: () => void; reject: (reason?: any) => void }> = new Map();
  private pendingPublishCompletions = new Set<string>();
  private unsParameters: IUnsParameters;
  protected processStatusTopic: string;
  public instanceName: string;
  private currentSequenceId: Map<string, number> = new Map();
  private topicBuilder: MqttTopicBuilder;
  private deltaModeDeprecationWarned = false;
  private drainWaiters: Array<{ resolve: () => void; reject: (reason?: any) => void; timer?: NodeJS.Timeout }> = [];
  private workerFailure: Error | null = null;
  private lifecycleState: UnsMqttProxyLifecycleState = "running";
  private stopPromise: Promise<void> | null = null;
  private expectedWorkerExit = false;

  constructor(
    mqttHost: string,
    processName: string,
    instanceName: string,
    unsParameters?: IUnsParameters,
    publisherActive: boolean = false,
    subscriberActive: boolean = false
  ) {
    super();
    this.instanceName = instanceName;
    // Create the topic builder using packageJson values and the processName.
    this.topicBuilder = new MqttTopicBuilder(`uns-infra/${MqttTopicBuilder.sanitizeTopicPart(packageJson.name)}/${MqttTopicBuilder.sanitizeTopicPart(packageJson.version)}/${MqttTopicBuilder.sanitizeTopicPart(processName)}/`);

    // Generate the processStatusTopic using the builder.
    this.processStatusTopic = this.topicBuilder.getProcessStatusTopic();
    // Derive the instanceStatusTopic by appending the instance name.
    this.instanceStatusTopic = this.processStatusTopic + instanceName + "/";

    // Concatenate processName with instanceName for the worker identification.
    this.instanceNameWithSuffix = `${processName}-${instanceName}`;
    
    const mqttParameters: IMqttParameters = {
      mqttSubToTopics: unsParameters?.mqttSubToTopics ?? [],
      username: unsParameters?.username ?? "",
      password: unsParameters?.password ?? "",
      mqttSSL: unsParameters?.mqttSSL ?? false,
      statusTopic: this.instanceStatusTopic,
      rejectUnauthorized: unsParameters?.rejectUnauthorized ?? false,
      clientId: unsParameters?.clientId,
      hosts: unsParameters?.hosts,
      servers: unsParameters?.servers,
      port: unsParameters?.port,
      protocol: unsParameters?.protocol,
      keepalive: unsParameters?.keepalive,
      clean: unsParameters?.clean,
      connectTimeout: unsParameters?.connectTimeout,
      reconnectPeriod: unsParameters?.reconnectPeriod,
      reconnectOnConnackError: unsParameters?.reconnectOnConnackError,
      resubscribe: unsParameters?.resubscribe,
      queueQoSZero: unsParameters?.queueQoSZero,
      properties: unsParameters?.properties,
      ca: unsParameters?.ca,
      cert: unsParameters?.cert,
      key: unsParameters?.key,
      servername: unsParameters?.servername,
    };
    this.unsParameters = unsParameters ?? {};
    this.startQueueWorker(mqttHost, this.instanceNameWithSuffix, mqttParameters, publisherActive, subscriberActive);
  }

  /**
   * Resolve object identity from explicit fields or the tail of the topic path.
   * Falls back to parsing when not provided for backward compatibility.
   */
  private resolveObjectIdentity(msg: InternalMqttMessage): { objectType?: UnsObjectType; objectId?: UnsObjectId; asset?: UnsAsset } {
    const providedType = msg.objectType;
    const providedId = msg.objectId;
    const providedAsset = msg.asset;

    const topicParts = msg.topic.split("/").filter((part) => part.length > 0);
    const hasObjectTail = topicParts.length >= 2;
    const parsedType = hasObjectTail ? topicParts[topicParts.length - 2] as UnsObjectType : undefined;
    const parsedId = hasObjectTail ? topicParts[topicParts.length - 1] as UnsObjectId : undefined;
    const parsedAsset = hasObjectTail
      ? (topicParts.length >= 3 ? topicParts[topicParts.length - 3] as UnsAsset : undefined)
      : (topicParts.length >= 1 ? topicParts[topicParts.length - 1] as UnsAsset : undefined);

    const objectType = providedType ?? parsedType;
    const objectId = providedId ?? parsedId ?? "main";
    const asset = providedAsset ?? parsedAsset;

    // If values are provided, trust them; otherwise derive from topic.
    if (!providedType || !providedId) {
      if (parsedType && parsedId) {
        logger.warn(`${this.instanceNameWithSuffix} - objectType/objectId missing; derived from topic tail ${parsedType}/${parsedId}`);
      } else {
        logger.warn(`${this.instanceNameWithSuffix} - objectType/objectId missing; defaulting objectId to 'main' for topic '${msg.topic}'. Expected topic to end with '<objectType>/<objectId>/'`);
      }
    }
    // Asset is optional; no warning on mismatch to avoid noisy logs when base topics don't carry it.

    msg.objectType = objectType;
    msg.objectId = objectId;
    msg.asset = asset;

    return { objectType, objectId, asset };
  }

  /**
   * Ensure the topic ends with a trailing slash for attribute concatenation.
   */
  private normalizeTopicWithObject(topic: string): string {
    return topic.endsWith("/") ? topic : `${topic}/`;
  }

  /**
   * Starts a worker thread to process the throttled publish queue.
   */
  private startQueueWorker(
    mqttHost: string,
    instanceNameWithSuffix: string,
    mqttParameters: IMqttParameters,
    publisherActive: boolean,
    subscriberActive: boolean
  ): void {
    const workerData: IMqttWorkerData = {
      publishThrottlingDelay: this.unsParameters.publishThrottlingDelay ?? 1,
      publishConcurrency: this.unsParameters.publishConcurrency ?? 1,
      maxPendingPublishes: this.unsParameters.maxPendingPublishes,
      subscribeThrottlingDelay: this.unsParameters.subscribeThrottlingDelay ?? 1,
      persistToDisk: false,
      mqttHost: mqttHost,
      instanceNameWithSuffix: instanceNameWithSuffix,
      mqttParameters: mqttParameters,
      publisherActive,
      subscriberActive,
      defaultPublishOptions: this.unsParameters.defaultPublishOptions,
    };

    this.worker = new Worker(workerScriptPath, { workerData });

    this.worker.on("message", (msg) => {
      if (msg && msg.command === "enqueueAccepted" && msg.id) {
        const pending = this.pendingEnqueues.get(msg.id);
        if (pending) {
          this.pendingEnqueues.delete(msg.id);
          if (msg.status === "accepted") {
            this.pendingPublishCompletions.add(msg.id);
            pending.resolve();
          } else {
            pending.reject(new Error(msg.error));
          }
          this.notifyDrainWaiters();
        }
      } else if (msg && msg.command === "publishResult" && msg.id) {
        this.pendingPublishCompletions.delete(msg.id);
        if (msg.status === "error") {
          const errorMessage = msg.error ?? `Error publishing to topic ${msg.topic ?? "unknown"}`;
          this.event.emit("error", { code: 0, message: errorMessage });
        }
        this.notifyDrainWaiters();
      } else if (msg && msg.command === "input") {
        this.event.emit("input", { topic: msg.topic, message: msg.message.toString(), packet: msg.packet });
      } else if (msg && (msg.command === "handover_subscriber" || msg.command === "handover_publisher")) {
        this.event.emit("mqttWorker", { command: msg.command, batchSize: msg.batchSize, referenceHash: msg.referenceHash, instanceName: this.instanceName });
      } else if (msg && msg.command === "mqttProxyStatus") {
        this.event.emit("mqttProxyStatus", { event: msg.event, value: msg.value, uom: msg.uom, statusTopic: msg.statusTopic });
      }
    });

    this.worker.on("error", (err) => {
      if (this.expectedWorkerExit) {
        return;
      }
      logger.error("Error in worker:", err);
      const reason = err instanceof Error ? err : new Error(String(err));
      this.failPendingPublishes(reason);
    });

    this.worker.on("exit", (code) => {
      if (this.expectedWorkerExit) {
        return;
      }
      logger.error(`Worker exited unexpectedly with code ${code}`);
      const reason = new Error(`MQTT worker exited unexpectedly with code ${code}`);
      this.failPendingPublishes(reason);
    });
  }

  /**
   * Enqueues a message to the worker queue.
   *
   * @param topic - The topic to which the message belongs.
   * @param message - The message to be enqueued.
   * @param options - Optional publish options.
   * @returns A promise that resolves when the message is accepted into the worker queue.
   */
  private async enqueueMessageToWorkerQueue(topic: string, message: string, options?: IClientPublishOptions): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.lifecycleState !== "running") {
        reject(new Error(`${this.instanceNameWithSuffix} - MQTT proxy is ${this.lifecycleState}; publish rejected.`));
        return;
      }
      if (this.workerFailure) {
        reject(this.workerFailure);
        return;
      }
      const maxPendingPublishes = this.unsParameters.maxPendingPublishes;
      const pendingCount = this.pendingEnqueues.size + this.pendingPublishCompletions.size;
      if (maxPendingPublishes !== undefined && pendingCount >= maxPendingPublishes) {
        reject(new Error(`${this.instanceNameWithSuffix} - Publisher queue is full (${maxPendingPublishes}).`));
        return;
      }
      const id = `${Date.now()}-${Math.random()}`;
      this.pendingEnqueues.set(id, { resolve, reject });
      try {
        this.worker.postMessage({ command: "enqueue", id, topic, message, options });
      } catch (error) {
        this.pendingEnqueues.delete(id);
        reject(error);
      }
      this.notifyDrainWaiters();
    });
  }

  /**
   * Sets the publisher active state.
   *
   * @param batchSize - Optional batch size.
   * @param referenceHash - Optional reference hash.
   */
  public setPublisherActive(batchSize?: number, referenceHash?: string): void {
    this.worker.postMessage({ command: "setPublisherActive", batchSize, referenceHash });
  }

  /**
   * Sets the publisher to passive mode.
   * @returns A promise that resolves when the publisher is set to passive.
   */
  public setPublisherPassive(): Promise<UnsEvents["mqttWorker"]> {
    this.worker.postMessage({ command: "setPublisherPassive"});
    return new Promise((resolve) => {
      const handler = (msg: UnsEvents["mqttWorker"]) => {
        if (msg.command === "handover_publisher") {
          this.event.off("mqttWorker", handler);
          logger.info(`${this.instanceNameWithSuffix} - Publisher set to passive.`);
          resolve(msg);
        }
      };
      this.event.on("mqttWorker", handler);
    });
  }

  /**
   * Sets the subscriber active state.
   *
   * @param batchSize - Optional batch size.
   * @param referenceHash - Optional reference hash.
   */
  public setSubscriberActive(batchSize?: number, referenceHash?: string): void {
    this.worker.postMessage({ command: "setSubscriberActive", batchSize, referenceHash });
  }

  /**
   * Sets the subscriber to passive mode.
   * @returns A promise that resolves when the subscriber is set to passive.
   */
  public setSubscriberPassive(): Promise<UnsEvents["mqttWorker"]> {
    this.worker.postMessage({ command: "setSubscriberPassive"});
    return new Promise((resolve) => {
      const handler = (msg: UnsEvents["mqttWorker"]) => {
        if (msg.command === "handover_subscriber") {
          this.event.off("mqttWorker", handler);
          logger.info(`${this.instanceNameWithSuffix} - Subscriber set to passive.`);
          resolve(msg);
        }
      };
      this.event.on("mqttWorker", handler);
    });
  }


  /**
   * Sets the subscriber to passive mode and allows the publisher to run
   * until the queue is empty (all messages are processed).
   */
  public async setSubscriberPassiveAndDrainQueue(): Promise<UnsEvents["mqttWorker"]> {
    const mqttWorkerData = await this.setSubscriberPassive();
    await this.flush();
    logger.info(`${this.instanceNameWithSuffix} - Subscriber set to passive and queue drained.`);
    return mqttWorkerData;
  }

  public async drainPublishes(timeoutMs: number = UnsMqttProxy.DEFAULT_DRAIN_TIMEOUT_MS): Promise<void> {
    if (this.workerFailure) {
      throw this.workerFailure;
    }
    if (this.pendingEnqueues.size === 0 && this.pendingPublishCompletions.size === 0) {
      return;
    }
    await new Promise<void>((resolve, reject) => {
      const waiter = { resolve, reject, timer: undefined as NodeJS.Timeout | undefined };
      if (Number.isFinite(timeoutMs) && timeoutMs > 0) {
        waiter.timer = setTimeout(() => {
          this.removeDrainWaiter(waiter);
          reject(new Error(`Timed out waiting for MQTT publishes to drain after ${timeoutMs}ms.`));
        }, timeoutMs);
      }
      this.drainWaiters.push(waiter);
      this.notifyDrainWaiters();
    });
  }

  public async flush(timeoutMs: number = UnsMqttProxy.DEFAULT_DRAIN_TIMEOUT_MS): Promise<void> {
    await this.drainPublishes(timeoutMs);
  }

  /**
   * Processes and publishes MQTT messages based on the selected message mode.
   *
   * @param mqttMessage - The MQTT message object.
   * @param mode - The message mode. Delta and Both are deprecated because
   * producer-side delta calculation loses state across restarts.
   */
  public async publishMqttMessage(mqttMessage: IMqttPublishRequest | null, mode: MessageMode = MessageMode.Raw) {
    if (!mqttMessage) {
      logger.error(`${this.instanceNameWithSuffix} - Error publishing mqtt message: mqttMessage must be defined.`);
      return;
    }

    const attrs = Array.isArray(mqttMessage.attributes)
      ? mqttMessage.attributes
      : [mqttMessage.attributes];
    const {
      topic,
      asset,
      assetDescription,
      assetStableEntityId,
      assetDisplayName,
      assetIdentityProof,
      assetProviderIdentity,
      assetProviderIdentityProof,
      objectType,
      objectTypeDescription,
      objectId,
      virtualGroup: requestVirtualGroup,
    } = mqttMessage;
    const hasAssetIdentityMetadata = assetStableEntityId !== undefined
      || assetDisplayName !== undefined
      || assetIdentityProof !== undefined;
    const hasAssetProviderIdentityMetadata = assetProviderIdentity !== undefined
      || assetProviderIdentityProof !== undefined;
    if (hasAssetIdentityMetadata && hasAssetProviderIdentityMetadata) {
      throw new Error("Stable Asset identity metadata and provider candidate metadata are mutually exclusive.");
    }
    if (hasAssetIdentityMetadata) {
      if (
        typeof assetStableEntityId !== "string"
        || !assetStableEntityId.trim()
        || typeof assetIdentityProof !== "string"
        || !assetIdentityProof.trim()
        || (assetDisplayName !== undefined
          && (typeof assetDisplayName !== "string" || !assetDisplayName.trim()))
      ) {
        throw new Error("Asset identity metadata requires assetStableEntityId and assetIdentityProof; assetDisplayName must be non-empty when supplied.");
      }
    }
    if (hasAssetProviderIdentityMetadata) {
      const identity = assetProviderIdentity as Record<string, unknown> | undefined;
      if (
        !identity
        || typeof assetProviderIdentityProof !== "string"
        || !assetProviderIdentityProof.trim()
        || ["providerId", "externalSystem", "externalType", "externalId"].some(
          (key) => typeof identity[key] !== "string" || !(identity[key] as string).trim(),
        )
      ) {
        throw new Error("Provider Asset identity metadata requires providerId, externalSystem, externalType, externalId, and assetProviderIdentityProof.");
      }
    }
    for (const attrEntry of attrs) {
      const attrDescription = attrEntry.description ?? getAttributeDescription(attrEntry.attribute);
      const message: IUnsMessage =
        "message" in attrEntry && attrEntry.message
          ? attrEntry.message
          : "data" in attrEntry && attrEntry.data
            ? { data: attrEntry.data, createdAt: attrEntry.createdAt, expiresAt: attrEntry.expiresAt }
            : "table" in attrEntry && attrEntry.table
              ? { table: attrEntry.table, createdAt: attrEntry.createdAt, expiresAt: attrEntry.expiresAt }
              : (() => { throw new Error("Attribute entry must include exactly one of data/table/message"); })();
      const packet = await UnsPacket.unsPacketFromUnsMessage(message);
      const singleMsg: InternalMqttMessage = {
        topic,
        asset,
        assetDescription,
        ...(hasAssetIdentityMetadata ? {
          assetStableEntityId: assetStableEntityId!.trim().toLowerCase(),
          ...(assetDisplayName ? { assetDisplayName: assetDisplayName.trim() } : {}),
          assetIdentityProof: assetIdentityProof!.trim(),
        } : {}),
        ...(hasAssetProviderIdentityMetadata ? {
          assetProviderIdentity: {
            providerId: assetProviderIdentity!.providerId.trim().toLowerCase(),
            externalSystem: assetProviderIdentity!.externalSystem.trim().toLowerCase(),
            externalType: assetProviderIdentity!.externalType.trim().toLowerCase(),
            externalId: assetProviderIdentity!.externalId.trim(),
          },
          assetProviderIdentityProof: assetProviderIdentityProof!.trim(),
        } : {}),
        objectType,
        objectTypeDescription,
        objectId,
        virtualGroup: attrEntry.virtualGroup ?? requestVirtualGroup,
        attribute: attrEntry.attribute,
        description: attrDescription,
        tags: attrEntry.tags,
        attributeNeedsPersistence: attrEntry.attributeNeedsPersistence,
        valueType: attrEntry.valueType,
        presentationKind: attrEntry.presentationKind,
        defaultAggregation: attrEntry.defaultAggregation,
        counterResetPolicy: attrEntry.counterResetPolicy,
        tableColumns: attrEntry.tableColumns,
        systemRole: attrEntry.systemRole,
        relationshipEvidence: attrEntry.relationshipEvidence,
        lifecycle: attrEntry.lifecycle,
        ...(attrEntry.validityMode ? { validityMode: attrEntry.validityMode } : {}),
        ...(attrEntry.expectedIntervalMs ? { expectedIntervalMs: attrEntry.expectedIntervalMs } : {}),
        ...(attrEntry.lifecycleEndValue ? { lifecycleEndValue: attrEntry.lifecycleEndValue } : {}),
        packet,
      };

      // existing single-attribute flow
      const baseDescription =
        singleMsg.description ??
        getAttributeDescription(singleMsg.attribute) ??
        singleMsg.attribute;
      const mqttMessageWithDesc = { ...singleMsg, description: baseDescription };

      const time = UnsPacket.formatToISO8601(new Date());
      if (mode === MessageMode.Delta || mode === MessageMode.Both) {
        this.warnDeprecatedDeltaMode(mode);
      }
      switch (mode) {
        case MessageMode.Raw: {
          await this.processAndEnqueueMessage(mqttMessageWithDesc, time, false);
          break;
        }
        case MessageMode.Delta: {
          const deltaMessage = { ...mqttMessageWithDesc };
          deltaMessage.attribute = `${mqttMessageWithDesc.attribute}-delta`;
          deltaMessage.description = `${baseDescription ?? ""} (delta)`;
          await this.processAndEnqueueMessage(deltaMessage, time, true);
          break;
        }
        case MessageMode.Both: {
          await this.processAndEnqueueMessage(mqttMessageWithDesc, time, false);
          const deltaMessageBoth = { ...mqttMessageWithDesc };
          deltaMessageBoth.attribute = `${mqttMessageWithDesc.attribute}-delta`;
          deltaMessageBoth.description = `${baseDescription ?? ""} (delta)`;
          await this.processAndEnqueueMessage(deltaMessageBoth, time, true);
          break;
        }
      }
    }
    return;
  }

  private warnDeprecatedDeltaMode(mode: MessageMode): void {
    if (this.deltaModeDeprecationWarned) return;
    this.deltaModeDeprecationWarned = true;
    logger.warn(
      `${this.instanceNameWithSuffix} - MessageMode.${mode} is deprecated: producer-side delta calculation loses previous-value state across service restarts. Publish raw cumulative counter values and request delta/rate from Datahub history APIs.`,
    );
  }

  /**
   * Publishes a message to a specified topic.
   *
   * @param topic - The MQTT topic.
   * @param message - The message to publish.
   * @returns A promise that resolves when the message is accepted into the worker queue.
   */
  public publishMessage(topic: string, message: string, options?: IClientPublishOptions): Promise<void> {
    return this.enqueueMessageToWorkerQueue(topic, message, options);
  }

  /**
   * Parses an MQTT packet from a JSON string.
   *
   * @param mqttPacket - The MQTT packet string.
   * @returns A parsed IUnsPacket object or null.
   */
  public parseMqttPacket(mqttPacket: string): IUnsPacket | null {
    return UnsPacket.parseMqttPacket(mqttPacket, this.instanceNameWithSuffix);
  }

  /**
   * Subscribes asynchronously to one or more topics.
   *
   * @param topics - A topic or list of topics.
   */
  public subscribeAsync(topics: string | string[]): void {
    this.worker.postMessage({ command: "subscribeAsync", topics });
  }

  /**
   * Unsubscribes asynchronously from the given topics.
   *
   * @param topics - A list of topics.
   */
  public unsubscribeAsync(topics: string[]): void {
    this.worker.postMessage({ command: "unsubscribeAsync", topics });
  }

  /**
   * Processes and enqueues a message to the worker queue, including handling
   * sequencing, value differences, and tracking of unique topics.
   *
   * @param msg - The MQTT message to process.
   * @param time - The timestamp.
   * @param valueIsCumulative - Whether the value is cumulative.
   */
  private async processAndEnqueueMessage(msg: InternalMqttMessage, time: string, valueIsCumulative: boolean = false): Promise<void> {
    try {
      const attributeType =
        msg.packet.message.data ? UnsAttributeType.Data :
        msg.packet.message.table ? UnsAttributeType.Table : null;
      
      let dataGroup = "";
      if (attributeType == UnsAttributeType.Data)
        dataGroup = msg.packet.message.data.dataGroup ?? "";
      if (attributeType == UnsAttributeType.Table)
        dataGroup = msg.packet.message.table.dataGroup ?? "";

      const { objectType, objectId, asset } = this.resolveObjectIdentity(msg);
      const normalizedTopic = this.normalizeTopicWithObject(msg.topic);
      msg.topic = normalizedTopic;
      const description = msg.description ?? getAttributeDescription(msg.attribute as string) ?? "";
      const objectTypeDescription = msg.objectTypeDescription ?? (objectType ? getObjectTypeDescription(objectType) : undefined);

      // Runtime validation for validity fields
      if (msg.validityMode) {
        const validModes = new Set(["interval", "lifecycle", "static"]);
        if (!validModes.has(msg.validityMode)) {
          logger.warn(`${this.instanceNameWithSuffix} - Invalid validityMode "${msg.validityMode}" for attribute "${msg.attribute}". Expected: interval | lifecycle | static.`);
        }
        if (msg.expectedIntervalMs && msg.validityMode !== "interval") {
          logger.warn(`${this.instanceNameWithSuffix} - expectedIntervalMs is set but validityMode is "${msg.validityMode}" (only used with "interval") for attribute "${msg.attribute}".`);
        }
        if (msg.lifecycleEndValue && msg.validityMode !== "lifecycle") {
          logger.warn(`${this.instanceNameWithSuffix} - lifecycleEndValue is set but validityMode is "${msg.validityMode}" (only used with "lifecycle") for attribute "${msg.attribute}".`);
        }
        if (msg.validityMode === "interval" && !msg.expectedIntervalMs) {
          logger.debug(`${this.instanceNameWithSuffix} - validityMode "interval" without expectedIntervalMs for attribute "${msg.attribute}" — controller will use its default.`);
        }
      }

      this.registerUniqueTopic({
        timestamp: time,
        topic: msg.topic,
        attribute: msg.attribute,
        attributeType: attributeType,
        description,
        tags: msg.tags,
        attributeNeedsPersistence: msg.attributeNeedsPersistence,
        valueType: msg.valueType,
        presentationKind: msg.presentationKind,
        defaultAggregation: msg.defaultAggregation,
        counterResetPolicy: msg.counterResetPolicy,
        tableColumns: msg.tableColumns,
        systemRole: msg.systemRole,
        relationshipEvidence: msg.relationshipEvidence,
        lifecycle: msg.lifecycle,
        dataGroup,
        virtualGroup: msg.virtualGroup,
        asset,
        assetDescription: msg.assetDescription,
        assetStableEntityId: msg.assetStableEntityId,
        assetDisplayName: msg.assetDisplayName,
        assetIdentityProof: msg.assetIdentityProof,
        assetProviderIdentity: msg.assetProviderIdentity,
        assetProviderIdentityProof: msg.assetProviderIdentityProof,
        objectType,
        objectTypeDescription,
        objectId,
        ...(msg.validityMode ? { validityMode: msg.validityMode } : {}),
        ...(msg.expectedIntervalMs ? { expectedIntervalMs: msg.expectedIntervalMs } : {}),
        ...(msg.lifecycleEndValue ? { lifecycleEndValue: msg.lifecycleEndValue } : {}),
      });

      const publishTopic = `${msg.topic}${asset ? `${asset}/` : ""}${objectType ? `${objectType}/` : ""}${objectId ? `${objectId}/` : ""}${msg.attribute}`;
      const sequenceId = this.currentSequenceId.get(msg.topic) ?? 0;
      this.currentSequenceId.set(msg.topic, sequenceId + 1);
      msg.packet.sequenceId = sequenceId;

      if (msg.packet.message.data) {
        const newValue = msg.packet.message.data.value;
        const newUom: MeasurementUnit = msg.packet.message.data.uom;
        const lastValueEntry = this.lastValues.get(publishTopic);
        const currentTime = new Date(msg.packet.message.data.time);

        if (lastValueEntry) {
          const intervalBetweenMessages = currentTime.getTime() - lastValueEntry.timestamp.getTime();
          const lastValue = lastValueEntry.value;
          this.lastValues.set(publishTopic, { value: newValue, uom: newUom, timestamp: currentTime });
          // Compute the delta and manage cumulative resets
          if (valueIsCumulative == true && typeof newValue === "number" && typeof lastValue === "number") {
            // Skip if newValue is 0 (likely a glitch)
            if (newValue === 0) {
              return; // Don't process or enqueue
            }
            const delta = newValue - lastValue;
            msg.packet.message.data.value = delta < 0 ? newValue : delta;
          }
          msg.packet.interval = intervalBetweenMessages;
          await this.enqueueMessageToWorkerQueue(publishTopic, JSON.stringify(msg.packet));
        } else {
          this.lastValues.set(publishTopic, { value: newValue, uom: newUom, timestamp: currentTime });
          logger.debug(`${this.instanceNameWithSuffix} - Need one more packet to calculate interval on topic ${publishTopic}`);
          if (valueIsCumulative === false) {
            await this.enqueueMessageToWorkerQueue(publishTopic, JSON.stringify(msg.packet));
          } else {
            logger.debug(`${this.instanceNameWithSuffix} - Need one more packet to calculate difference on value in data for topic ${publishTopic}`);
          }
        }
      } else if (msg.packet.message.table) {
        await this.enqueueMessageToWorkerQueue(publishTopic, JSON.stringify(msg.packet));
      } else {
        logger.error(`${this.instanceNameWithSuffix} - Error publishing message to topic ${publishTopic}: packet.message must include data or table`);
      }
    } catch (error: any) {
      logger.error(`${this.instanceNameWithSuffix} - Error publishing message to topic ${msg.topic}${msg.attribute}: ${error.message}`);
      throw error;
    }
  }

  /**
   * Stops the UnsProxy instance and cleans up resources.
   */
  public stop(options: UnsMqttProxyStopOptions = {}): Promise<void> {
    if (this.stopPromise) {
      return this.stopPromise;
    }

    this.lifecycleState = "stopping";
    this.stopPromise = this.performStop(options);
    return this.stopPromise;
  }

  private async performStop(options: UnsMqttProxyStopOptions): Promise<void> {
    await super.stop();
    const drain = options.drain ?? true;
    const timeoutMs = options.timeoutMs ?? UnsMqttProxy.DEFAULT_DRAIN_TIMEOUT_MS;
    let stopError: Error | null = null;

    if (drain) {
      try {
        await this.flush(timeoutMs);
      } catch (error: any) {
        stopError = error instanceof Error ? error : new Error(String(error));
        logger.error(`${this.instanceNameWithSuffix} - Error draining publishes before stop: ${stopError.message}`);
      }
    }

    if (this.worker) {
      this.expectedWorkerExit = true;
      try {
        const exitCode = await this.worker.terminate();
        logger.info(`${this.instanceNameWithSuffix} - Worker terminated with exit code ${exitCode}`);
      } catch (error: any) {
        const terminationError = error instanceof Error ? error : new Error(String(error));
        logger.error(`${this.instanceNameWithSuffix} - Error terminating worker: ${terminationError.message}`);
        stopError ??= terminationError;
      }
    }

    this.lifecycleState = "stopped";
    this.rejectOutstandingPublishes(stopError ?? new Error("UnsProxy has been stopped"));

    if (stopError) {
      throw stopError;
    }
  }

  private failPendingPublishes(reason: Error): void {
    this.workerFailure = reason;
    this.rejectOutstandingPublishes(reason);
  }

  private rejectOutstandingPublishes(reason: Error): void {
    for (const pending of this.pendingEnqueues.values()) {
      pending.reject(reason);
    }
    this.pendingEnqueues.clear();
    this.pendingPublishCompletions.clear();

    const waiters = this.drainWaiters.splice(0);
    for (const waiter of waiters) {
      if (waiter.timer) {
        clearTimeout(waiter.timer);
      }
      waiter.reject(reason);
    }
  }

  private notifyDrainWaiters(): void {
    if (this.workerFailure) {
      const waiters = this.drainWaiters.splice(0);
      for (const waiter of waiters) {
        if (waiter.timer) {
          clearTimeout(waiter.timer);
        }
        waiter.reject(this.workerFailure);
      }
      return;
    }
    if (this.pendingEnqueues.size !== 0 || this.pendingPublishCompletions.size !== 0) {
      return;
    }
    const waiters = this.drainWaiters.splice(0);
    for (const waiter of waiters) {
      if (waiter.timer) {
        clearTimeout(waiter.timer);
      }
      waiter.resolve();
    }
  }

  private removeDrainWaiter(waiterToRemove: { resolve: () => void; reject: (reason?: any) => void; timer?: NodeJS.Timeout }): void {
    this.drainWaiters = this.drainWaiters.filter((waiter) => waiter !== waiterToRemove);
  }

}
