import logger from "../logger.js";
import { HandoverManagerEvents } from "../uns-mqtt/mqtt-interfaces.js";
import MqttProxy from "../uns-mqtt/mqtt-proxy.js";
import { MqttTopicBuilder } from "../uns-mqtt/mqtt-topic-builder.js";
import UnsMqttProxy from "../uns-mqtt/uns-mqtt-proxy.js";
import { HandoverManagerEventEmitter } from "./handover-manager-event-emitter.js";
import { ACTIVE_TIMEOUT, PACKAGE_INFO } from "./process-config.js";
import { UnsEvents } from "./uns-interfaces.js";

/**
 * HandoverManager is responsible for all handover-related logic,
 * including handling incoming MQTT messages, issuing handover requests,
 * and processing handover responses.
 */
export class HandoverManager {
  public event: HandoverManagerEventEmitter<HandoverManagerEvents> = new HandoverManagerEventEmitter<HandoverManagerEvents>();
  private processName: string;
  private processId: string;
  private mqttProxy: MqttProxy;
  private unsMqttProxies: UnsMqttProxy[];
  private requestingHandover: boolean = false;
  private handoverInProgress: boolean = false;
  private topicBuilder: MqttTopicBuilder;
  private activeTimeout: NodeJS.Timeout | undefined;
  private active: boolean = false;
  public handoverRequestEnabled: boolean = false;
  public handoverEnabled: boolean = true;
  public forceStartEnabled: boolean = false;

  constructor(
    processName: string,
    processId: string,
    mqttProxy: MqttProxy,
    unsMqttProxies: UnsMqttProxy[],
    handoverRequestEnabled: boolean,
    handoverEnabled: boolean,
    forceStartEnabled: boolean,
  ) {
    this.processName = processName;
    this.processId = processId;
    this.mqttProxy = mqttProxy;
    this.unsMqttProxies = unsMqttProxies;
    this.handoverRequestEnabled = handoverRequestEnabled;
    this.handoverEnabled = handoverEnabled;
    this.forceStartEnabled = forceStartEnabled;

    // Instantiate the topic builder.
    const packageName = PACKAGE_INFO.name;
    const version = PACKAGE_INFO.version;
    this.topicBuilder = new MqttTopicBuilder(
      `uns-infra/${MqttTopicBuilder.sanitizeTopicPart(packageName)}/${MqttTopicBuilder.sanitizeTopicPart(version)}/${MqttTopicBuilder.sanitizeTopicPart(this.processName)}/`,
    );

    // Set status as active after a timeout if no other active process are detected.
    this.activeTimeout = setTimeout(() => {
      logger.info(`${this.processName} - No active message received within timeout. Assuming no other process is running.`);
      this.active = true;
      this.event.emit("handoverManager", { active: this.active });
      // Activate all UNS proxy instance publishers and subscribers.
      this.unsMqttProxies.forEach((unsProxy) => {
        unsProxy.setPublisherActive();
        unsProxy.setSubscriberActive();
      });
    }, ACTIVE_TIMEOUT);
  }

  private getUserProperties(): Record<string, string> {
    return {
      processName: this.processName,
      processId: this.processId,
    };
  }

  private getSourceProcessId(event: UnsEvents["input"]): string | undefined {
    return event.packet?.properties?.userProperties?.processId;
  }

  private parseActiveValue(message: string): boolean | null {
    try {
      const parsed = JSON.parse(message);
      const value = parsed?.message?.data?.value ?? parsed?.data?.value;
      if (value === 1 || value === "1" || value === true || value === "true") {
        return true;
      }
      if (value === 0 || value === "0" || value === false || value === "false") {
        return false;
      }
      return null;
    } catch {
      return null;
    }
  }

  private isProcessActiveTopic(topic: string): boolean {
    const parts = topic.split("/");
    if (parts.length !== 5) return false;
    const [root, pkg, , process, tail] = parts;
    if (root !== "uns-infra" || tail !== "active") return false;
    const expectedPackage = MqttTopicBuilder.sanitizeTopicPart(PACKAGE_INFO.name);
    const expectedProcess = MqttTopicBuilder.sanitizeTopicPart(this.processName);
    return pkg === expectedPackage && process === expectedProcess;
  }

  /**
   * Main entry point for handling incoming MQTT messages.
   * It checks the topic and delegates to the corresponding handler.
   */
  public async handleMqttMessage(event: UnsEvents["input"]): Promise<void> {
    try {
      // Check if the packet is active messages from other processes and this process is not active.
      if (this.isProcessActiveTopic(event.topic) && this.requestingHandover === false && this.active === false && this.handoverInProgress === false) {
        const sourceProcessId = this.getSourceProcessId(event);
        if (sourceProcessId === this.processId) {
          return;
        }
        const activeValue = this.parseActiveValue(event.message.toString());
        if (activeValue !== true) {
          return;
        }
        const sourceInfo = sourceProcessId ? ` (processId=${sourceProcessId})` : "";
        logger.info(`${this.processName} - Another process is active${sourceInfo} on ${event.topic}.`);

        if (event.packet?.retain === true) {
          // A retained heartbeat can belong to a process that crashed before its
          // expiry. Wait for a fresh non-retained heartbeat before asking it to
          // hand over; otherwise a stale retained message would block startup.
          logger.info(`${this.processName} - Retained active heartbeat observed; waiting for a fresh heartbeat.`);
          this.activeTimeout?.refresh();
          return;
        }

        if (this.handoverRequestEnabled && this.handoverEnabled) {
          // Requester process
          // Publish a handover request message after 10 seconds to the handover topic.
          clearTimeout(this.activeTimeout); // Clear the active timeout if it exists - prevent this process from becoming active after a timeout.
          this.activeTimeout = undefined;
          this.event.emit("handoverManager", { active: this.active });
          this.requestingHandover = true;
          const eventHandoverTopic = new MqttTopicBuilder(MqttTopicBuilder.extractBaseTopic(event.topic)).getHandoverTopic();
          await this.mqttProxy.publish(eventHandoverTopic, JSON.stringify({ type: "handover_intent" }), {
            retain: false,
            properties: {
              userProperties: this.getUserProperties(),
            },
          });
          logger.info(`${this.processName} - Requesting handover in 10 seconds.`);
          setTimeout(async () => {
            logger.info(`${this.processName} - Requesting handover ${eventHandoverTopic}.`);
            this.handoverInProgress = true;
            await this.mqttProxy.publish(eventHandoverTopic, JSON.stringify({ type: "handover_request" }), {
              retain: false,
              properties: {
                responseTopic: this.topicBuilder.getHandoverTopic(),
                userProperties: this.getUserProperties(),
              },
            });
          }, 10000);
        } else {
          if (this.forceStartEnabled) {
            // Force start the process even if another process is active.
            logger.info(`${this.processName} - Force starting the process.`);
            logger.warn(`${this.processName} - Warning: Source and destination being the same may lead to duplicate messages.`);
            clearTimeout(this.activeTimeout); // Clear the active timeout if it exists - prevent this process from becoming active after a timeout.
            this.activeTimeout = undefined;
            this.active = true;
            this.event.emit("handoverManager", { active: this.active });
            // Activate all UNS proxy instance publishers and subscribers.
            this.unsMqttProxies.forEach((unsProxy) => {
              unsProxy.setPublisherActive();
              unsProxy.setSubscriberActive();
            });
          } else {
            logger.info(`${this.processName} - Waiting for the other process on topic ${event.topic} to become passive.`);
            this.activeTimeout?.refresh();
          }
        }
      }

      // Check if the packet is an handover message, sent to a handover topic.
      if (event.topic === this.topicBuilder.getHandoverTopic() && this.handoverEnabled) {
        const sourceProcessId = this.getSourceProcessId(event);
        if (sourceProcessId !== this.processId) {
          await this.handleHandover(event);
        }
      }
    } catch (error) {
      logger.error(`${this.processName} - Error processing MQTT message: ${error.message}`);
      return;
    }
  }

  /**
   * Handles handovers.
   */
  private async handleHandover(event: UnsEvents["input"]): Promise<void> {
    try {
      const response = JSON.parse(event.message.toString());
      // Responder process
      // Check if the message is a handover request and publish MULTIPLE handover_subscriber messages
      if (response.type === "handover_request") {
        logger.info(
          `${this.processName} - Received handover request from ${event.packet?.properties?.userProperties?.processName}. Accepting handover.`,
        );

        // Set all UNS proxy instance subscribers to passive and drain the queue.
        const mqttWorkerData: UnsEvents["mqttWorker"][] = [];
        for (let i = 0; i < this.unsMqttProxies.length; i++) {
          const unsProxy = this.unsMqttProxies[i];
          const workerData = await unsProxy.setSubscriberPassiveAndDrainQueue();
          mqttWorkerData.push(workerData);
        }
        logger.info(`${this.processName} - Handover request accepted. Sending handover_subscriber messages.`);

        // Publish handover_subscriber messages for each instance that has processed some data.
        for (let i = 0; i < mqttWorkerData.length; i++) {
          const workerData = mqttWorkerData[i];
          if (workerData.batchSize > 0) {
            await this.mqttProxy.publish(
              event.packet.properties?.responseTopic ?? "",
              JSON.stringify({
                type: "handover_subscriber",
                batchSize: workerData.batchSize,
                referenceHash: workerData.referenceHash,
                instanceName: workerData.instanceName,
              }),
              {
                retain: false,
                properties: {
                  responseTopic: this.topicBuilder.getHandoverTopic(),
                  userProperties: this.getUserProperties(),
                },
              },
            );
          }
        }
        logger.info(`${this.processName} - Handover subscriber messages sent.`);

        // Publish a single handover acknowledgment only when all
        // handover_subscriber messages have been sent
        this.active = false;
        this.event.emit("handoverManager", { active: this.active });
        await this.mqttProxy.publish(
          event.packet.properties?.responseTopic ?? "",
          JSON.stringify({
            type: "handover_fin",
          }),
          {
            retain: false,
            properties: {
              responseTopic: this.topicBuilder.getHandoverTopic(),
              userProperties: this.getUserProperties(),
            },
          },
        );
        logger.info(`${this.processName} - Handover fin message sent.`);

        this.handoverInProgress = false;
        this.requestingHandover = false;

        await Promise.all(this.unsMqttProxies.map((unsProxy: UnsMqttProxy) => unsProxy.stop()));
      }

      // Requestor process
      // Check if the message is one of the handover_subscriber message in response to handover_request
      // and publish a handover_ack message
      if (response.type === "handover_subscriber") {
        // Find correct unsProxy instance for handover_subscriber and set it active
        this.unsMqttProxies.forEach((unsProxy: UnsMqttProxy) => {
          if (unsProxy.instanceName === response.instanceName) {
            unsProxy.setSubscriberActive(response.batchSize, response.referenceHash);
          }
        });
      }

      // Requestor process
      // Check if the message is a handover_fin at the end of handover_subscriber messages
      if (response.type === "handover_fin") {
        logger.info(`${this.processName} - Received handover fin from ${event.packet?.properties?.userProperties?.processName}.`);

        // Maybe we should count the number of requests that were allrady made TODO
        // this.handoverInProgress = false;
        // this.requestingHandover = false;

        this.active = true;
        this.event.emit("handoverManager", { active: this.active });
        logger.info(`${this.processName} - Handover completed.`);
        // Activate all UNS proxy instance publishers and subscribers.
        this.unsMqttProxies.forEach((unsProxy) => {
          unsProxy.setPublisherActive();
          unsProxy.setSubscriberActive();
        });

        // Maybe we should reply with handover_ack.
        await this.mqttProxy.publish(
          event.packet.properties?.responseTopic ?? "",
          JSON.stringify({
            type: "handover_ack",
          }),
          {
            retain: false,
            properties: {
              responseTopic: this.topicBuilder.getHandoverTopic(),
              userProperties: this.getUserProperties(),
            },
          },
        );
        logger.info(`${this.processName} - Handover ack message sent.`);
      }

      // Responder process
      // Check if the message is a handover_ack at the end of handover_fin messages
      if (response.type === "handover_ack") {
        logger.info(`${this.processName} - Received handover ack from ${event.packet?.properties?.userProperties?.processName}.`);
        this.handoverInProgress = false;
        this.requestingHandover = false;

        logger.info(`${this.processName} - Handover completed. Exiting process.`);
        process.exit(0);
      }
    } catch (error) {
      logger.error(`${this.processName} - Error processing handover response: ${error.message}`);
    }
  }
}
