import { MqttPublishQueue } from "./publish-queue.js";
import { IClientPublishOptions } from "mqtt";
import { join } from "path";
import { parentPort } from "worker_threads";

import { basePath } from "../base-path.js";
import logger from "../logger.js";
import { IMqttWorkerData } from "./mqtt-interfaces.js";
import MqttProxy from "./mqtt-proxy.js";
import { ThrottledPublisher, ThrottledSubscriber } from "./throttled-queue.js";

export class MqttWorker {
  private mqttProxy: MqttProxy;
  protected publisher: ThrottledPublisher | MqttPublishQueue;
  protected subscriber: ThrottledSubscriber;
  private defaultPublishOptions?: IClientPublishOptions;

  constructor(workerData: IMqttWorkerData) {
    const publishThrottlingDelay = workerData.publishThrottlingDelay ?? 1;
    const publishConcurrency = workerData.publishConcurrency ?? 1;
    const maxPendingPublishes = workerData.maxPendingPublishes;
    const subscribeThrottlingDelay = workerData.subscribeThrottlingDelay ?? 1;
    const persistToDisk = workerData.persistToDisk ?? false;
    const mqttHost = workerData.mqttHost;
    const instanceName = workerData.instanceNameWithSuffix;
    const mqttParameters = workerData.mqttParameters;
    const publisherActive = workerData.publisherActive;
    const subscriberActive = workerData.subscriberActive;
    const defaultPublishOptions = workerData.defaultPublishOptions;
    this.defaultPublishOptions = defaultPublishOptions;

    // Initialize and start the MQTT proxy.
    this.mqttProxy = new MqttProxy(mqttHost, instanceName, mqttParameters, this);
    void this.mqttProxy.start()?.catch((): void => {});

    // Set up the event listener for incoming messages from the MQTT proxy.
    this.mqttProxy.event.on("input", (event) => {
      this.subscriber.enqueue(event.topic, event.message.toString());
    });

    // Set up the event listener for status messages from the MQTT proxy.
    this.mqttProxy.event.on("mqttProxyStatus", (event) => {
      parentPort?.postMessage({
        command: "mqttProxyStatus",
        event: event.event,
        value: event.value,
        statusTopic: event.statusTopic,
        uom: event.uom,
      });
    });

    // Define the publish function to be used by the ThrottledPublisher.
    const publishFunction = async (topic: string, message: string, id: string, options?: IClientPublishOptions): Promise<void> => {
      const publishOptions = options ?? defaultPublishOptions;
      try {
        await this.mqttProxy.publish(topic, message, publishOptions);
      } catch (reason: any) {
        logger.error(`${instanceName} - Error publishing message to topic ${topic}: ${reason?.message ?? String(reason)}`);
        throw reason;
      }
    };

    // Create an instance of ThrottledPublisher.
    if (workerData.publishQueue) {
      if (publishConcurrency !== 1 || persistToDisk) throw Error("MQTT output persistence requires serial publishing and cannot combine with legacy persistToDisk");
      if (!workerData.publishQueueIdentity) throw Error("MQTT outbox requires an explicit stable owner/destination identity");
      this.publisher = new MqttPublishQueue(workerData.publishQueue, workerData.publishQueueIdentity,
        record => this.mqttProxy.publishConfirmed(record.topic, record.message, record.options,
          workerData.publishQueue.publishTimeoutMs ?? 10000), publisherActive,
        status => parentPort?.postMessage({ command: "publishQueueStatus", status }));
    } else {
    this.publisher = new ThrottledPublisher(
      publishThrottlingDelay,
      publishFunction,
      persistToDisk,
      join(basePath, "/workerQueue/", "throttled-publisher-queue.json"),
      instanceName,
      publisherActive,
      publishConcurrency,
      maxPendingPublishes,
    );
    }

    // Define the message handler for incoming messages.
    const messageHandler = async (topic: string, message: string): Promise<void> => {
      parentPort?.postMessage({
        command: "input",
        topic: topic,
        message: message,
      });
    };

    // Create an instance of ThrottledSubscriber.
    this.subscriber = new ThrottledSubscriber(
      subscribeThrottlingDelay,
      messageHandler,
      persistToDisk,
      join(basePath, "/workerQueue/", "throttled-subscriber-queue.json"),
      instanceName,
      subscriberActive,
    );

    // Set up the message listener for commands from the main thread.
    this.initializeMessageListener();
  }

  /**
   * Listen for incoming messages from the main thread and process them.
   */
  private initializeMessageListener(): void {
    parentPort?.on("message", async (msg) => {
      if (msg && msg.command === "enqueue" && msg.id && msg.topic && msg.message !== undefined) {
        try {
          const publishPromise = this.publisher.enqueueOrThrow(msg.topic, msg.message, msg.id, msg.options ?? this.defaultPublishOptions);
          parentPort?.postMessage({
            command: "enqueueAccepted",
            id: msg.id,
            status: "accepted",
          });
          const { id, topic } = msg;
          void publishPromise
            .then(() => {
              parentPort?.postMessage({
                command: "publishResult",
                id,
                status: "success",
                topic,
              });
            })
            .catch((error: any) => {
              parentPort?.postMessage({
                command: "publishResult",
                id,
                status: "error",
                topic,
                error: error?.message ?? String(error),
              });
            });
        } catch (error: any) {
          parentPort?.postMessage({
            command: "enqueueAccepted",
            id: msg.id,
            status: "error",
            error: error?.message ?? String(error),
          });
        }
      } else if (msg?.command === "flushPublishes" && this.publisher instanceof MqttPublishQueue) {
        try {
          await this.publisher.waitForIdle(msg.timeoutMs);
          parentPort?.postMessage({ command: "publishQueueReply", id: msg.id, status: "success" });
        } catch {
          parentPort?.postMessage({ command: "publishQueueReply", id: msg.id, status: "error", error: "MQTT queue drain failed or timed out; pending work retained" });
        }
      } else if (msg?.command === "closePublisher" && this.publisher instanceof MqttPublishQueue) {
        try {
          await this.mqttProxy.stop({ force: true });
          await this.publisher.close();
          parentPort?.postMessage({ command: "publishQueueReply", id: msg.id, status: "success" });
        } catch {
          parentPort?.postMessage({ command: "publishQueueReply", id: msg.id, status: "error", error: "MQTT output could not be safely persisted/closed; worker retained" });
        }
      } else if (msg && msg.command === "subscribeAsync" && msg.topics) {
        this.mqttProxy.subscribeAsync(msg.topics);
      } else if (msg && msg.command === "unsubscribeAsync" && msg.topics) {
        this.mqttProxy.unsubscribeAsync(msg.topics);
      } else if (msg?.command === "prepareSubscriberResume" || msg?.command === "activatePreparedSubscriber") {
        try {
          if (msg.command === "prepareSubscriberResume") this.subscriber.prepareResume(msg.cursor);
          else this.subscriber.activatePreparedResume();
          parentPort?.postMessage({ command: "publishQueueReply", id: msg.id, status: "success" });
        } catch {
          parentPort?.postMessage({ command: "publishQueueReply", id: msg.id, status: "error", error: "MQTT subscriber resume not proven" });
        }
      } else if (msg && msg.command === "setPublisherActive") {
        try { this.publisher.becomeActive({batchSize: msg?.batchSize, referenceHash: msg?.referenceHash}); }
        catch { parentPort?.postMessage({ command: "publishQueueControlError", error: "Persistent output cannot resume using a legacy skip hash" }); }
      } else if (msg && msg.command === "setPublisherPassive") {
        const snapshot = await this.publisher.becomePassive(3);
        parentPort?.postMessage({
          command: "handover_publisher",
          ...snapshot
        });          
      } else if (msg && msg.command === "setSubscriberActive") {
        this.subscriber.becomeActive({batchSize: msg?.batchSize, referenceHash: msg?.referenceHash});
      } else if (msg && msg.command === "setSubscriberPassive") {
        const snapshot = await this.subscriber.becomePassive(3, msg.strict === true);
        parentPort?.postMessage({
          command: "handover_subscriber",
          ...snapshot
        });          
      }
    });
  }

  /**
   * Get the state of the publisher.
   */
  public getPublisherState(): boolean {
    return this.publisher.getState();
  }
  /**
   * Get the state of the subscriber.
   */
  public getSubscriberState(): boolean {
    return this.subscriber.getState();
  }

}
