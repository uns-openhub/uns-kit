import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { IClientPublishOptions } from "mqtt";
import { join } from "path";

import { basePath } from "../base-path.js";
import logger from "../logger.js";

/**
 * Abstract base class that encapsulates common functionality for managing
 * a throttled in‑memory queue with an optional disk persistence.
 */
abstract class ThrottledQueue<T> {
  protected queue: T[] = [];
  protected lastProcessedItems: T[] = [];
  protected isProcessing = false;
  protected inFlight = 0;
  protected maxConcurrency: number;
  public delay: number;
  protected persistToDisk: boolean;
  protected persistenceFilePath: string;
  protected previousLoggedSize = 0;
  protected instanceName: string;
  protected active: boolean = true;
  private inactiveLogSent = false;

  constructor(delay: number, persistToDisk: boolean, instanceName: string, maxConcurrency: number = 1) {
    this.delay = delay;
    this.persistToDisk = persistToDisk;
    this.instanceName = instanceName;
    this.maxConcurrency = Math.max(1, maxConcurrency);

    if (this.persistToDisk) {
      this.loadQueueFromDisk();
    }
  }

  /**
   * Process the queue items one at a time with a delay between each.
   */
  protected async processQueue(): Promise<void> {
    if (this.isProcessing) return;

    if (!this.active) {
      if (!this.inactiveLogSent) {
        logger.info(`${this.instanceName} - Queue is not active. Exiting processQueue.`);
        this.inactiveLogSent = true;
      }
      this.isProcessing = false;
      return;
    }
    this.isProcessing = true;
    // Reset the flag when processing actually starts.
    this.inactiveLogSent = false;

    try {
      while (this.queue.length > 0 && this.inFlight < this.maxConcurrency) {
        // Check if the queue is active
        if (!this.active) {
          logger.info(`${this.instanceName} - Queue processing paused.`);
          return; // Exit the loop and stop processing
        }

        const item = this.queue.shift();
        if (!item) {
          continue;
        }

        this.inFlight += 1;
        void this.processItemWithBookkeeping(item);

        if (this.persistToDisk) {
          this.saveQueueToDisk();
        }

        this.logQueueSize();

        if (this.delay > 0 && this.queue.length > 0 && this.inFlight < this.maxConcurrency) {
          await new Promise((resolve) => setTimeout(resolve, this.delay));
        }
      }
    } finally {
      this.isProcessing = false;
      if (this.active && this.queue.length > 0 && this.inFlight < this.maxConcurrency) {
        void this.processQueue();
      }
    }
  }

  private async processItemWithBookkeeping(item: T): Promise<void> {
    try {
      await this.processItem(item);
      // Only store 10 last processed items
      if (this.lastProcessedItems.length >= 10) {
        this.lastProcessedItems.shift();
      }
      // Store the last processed item
      this.lastProcessedItems.push(item);
    } catch (error) {
      // Specific error handling can be done in the subclass if needed.
      logger.error(`${this.instanceName} - Error processing item: ${(error as Error).message}`);
    } finally {
      this.inFlight = Math.max(0, this.inFlight - 1);

      if (this.persistToDisk) {
        this.saveQueueToDisk();
      }

      this.logQueueSize();

      // Check if the queue is active
      if (this.active && this.queue.length > 0) {
        void this.processQueue();
      }
    }
  }

  /**
   * Log changes to the queue size at significant thresholds.
   */
  protected logQueueSize(): void {
    const className = `${this.constructor.name.includes("Publisher") ? "Publisher" : this.constructor.name.includes("Subscriber") ? "Subscriber" : "Unknown"}`;
    const pendingCount = this.getPendingCount();
    if (
      pendingCount > 1
      && pendingCount > this.previousLoggedSize
      && Math.floor(pendingCount / 100) > Math.floor(this.previousLoggedSize / 100)
    ) {
      logger.info(`${this.instanceName} - ${className} queue size length is ${pendingCount}.`);
      this.previousLoggedSize = pendingCount;
    } else if (pendingCount === 0 && this.previousLoggedSize > 0) {
      logger.info(`${this.instanceName} - ${className} queue is empty.`);
      this.previousLoggedSize = 0;
    }
  }

  protected getPendingCount(): number {
    return this.queue.length + this.inFlight;
  }

  /**
   * Save the current queue to disk.
   */
  protected saveQueueToDisk(): void {
    try {
      const queueData = this.queue.map((item) => this.serializeItem(item));
      const dir = join(this.persistenceFilePath, "..");
      if (!existsSync(dir)) {
        mkdirSync(dir, { recursive: true });
      }
      writeFileSync(this.persistenceFilePath, JSON.stringify(queueData, null, 2));
      logger.debug(`${this.instanceName} - Queue saved to disk.`);
    } catch (error: any) {
      logger.error(`${this.instanceName} - Error saving queue to disk: ${error.message}`);
    }
  }

  /**
   * Load the queue from disk.
   */
  protected loadQueueFromDisk(): void {
    try {
      if (existsSync(this.persistenceFilePath)) {
        const queueData = JSON.parse(readFileSync(this.persistenceFilePath, "utf8"));
        this.queue = queueData.map((data: any) => this.deserializeItem(data));
        logger.info(`${this.instanceName} - Queue loaded from disk.`);
      }
    } catch (error: any) {
      logger.error(`${this.instanceName} - Error loading queue from disk: ${error.message}`);
    }
  }

  /**
   * Process a single queue item.
   */
  protected abstract processItem(item: T): Promise<void>;

  /**
   * Serialize a queue item for persistence.
   */
  protected abstract serializeItem(item: T): any;

  /**
   * Deserialize persisted data into a queue item.
   */
  protected abstract deserializeItem(data: any): T;
}

/**
 * Interface representing a queued publish item.
 */
interface PublisherQueueItem {
  topic: string;
  message: string;
  id: string;
  options?: IClientPublishOptions;
  resolve: () => void;
  reject: (err: any) => void;
}

/**
 * ThrottledPublisher: manages a queue of outgoing publish requests,
 * sending one message at a time using a provided publish function.
 */
export class ThrottledPublisher extends ThrottledQueue<PublisherQueueItem> {
  private publishFunction: (topic: string, message: string, id: string, options?: IClientPublishOptions) => Promise<void>;
  private maxPendingPublishes?: number;

  /**
   * @param delay Delay between messages in milliseconds.
   * @param publishFunction Function to perform the actual publish.
   * @param persistToDisk Whether to persist the queue to disk.
   * @param persistenceFilePath File path for queue persistence.
   * @param instanceName Unique instance name for logging.
   */
  constructor(
    delay: number,
    publishFunction: (topic: string, message: string, id: string, options?: IClientPublishOptions) => Promise<void>,
    persistToDisk: boolean = false,
    persistenceFilePath: string = join(basePath, "/publisherQueue/", "throttled-publisher-queue.json"),
    instanceName: string,
    active: boolean,
    maxConcurrency: number = 1,
    maxPendingPublishes?: number,
  ) {
    super(delay, persistToDisk, instanceName, maxConcurrency);
    this.persistenceFilePath = persistenceFilePath;
    this.publishFunction = publishFunction;
    this.active = active;
    this.maxPendingPublishes = maxPendingPublishes;
    if (active) {
      logger.info(`${this.instanceName} - Publisher is active.`);
    } else {
      logger.info(`${this.instanceName} - Publisher is passive.`);
    }
  }

  /**
   * Enqueue a publish request.
   */
  public enqueue(topic: string, message: string, id: string, options?: IClientPublishOptions): Promise<void> {
    try {
      this.assertCanAcceptPublish();
      return this.enqueueAcceptedPublish(topic, message, id, options);
    } catch (error) {
      return Promise.reject(error);
    }
  }

  /**
   * Enqueue a publish request and throw synchronously if the bounded queue is full.
   */
  public enqueueOrThrow(topic: string, message: string, id: string, options?: IClientPublishOptions): Promise<void> {
    this.assertCanAcceptPublish();
    return this.enqueueAcceptedPublish(topic, message, id, options);
  }

  private assertCanAcceptPublish(): void {
    if (this.maxPendingPublishes !== undefined && this.getPendingCount() >= this.maxPendingPublishes) {
      throw new Error(`${this.instanceName} - Publisher queue is full (${this.maxPendingPublishes}).`);
    }
  }

  private enqueueAcceptedPublish(topic: string, message: string, id: string, options?: IClientPublishOptions): Promise<void> {
    return new Promise((resolve, reject) => {
      this.queue.push({ topic, message, id, options, resolve, reject });

      // Trigger logging after a new message is added.
      this.logQueueSize();      

      if (this.persistToDisk) {
        this.saveQueueToDisk();
      }

      if (this.active) {
        this.processQueue();
      }
    });
  }

  private computeHashForBatch(messages: any[]): string {
    const data = JSON.stringify(messages);
    return createHash("sha256").update(data).digest("hex");
  }

  /**
   * Switch the publisher to a passive state and return a snapshot
   */
  public becomeActive(snapshot: { referenceHash: string; batchSize: number }): void {
    // If snapshot is empty (batchSize is zero), start processing from the beginning.
    if (snapshot.batchSize === 0 || snapshot.batchSize === undefined || snapshot.referenceHash === undefined) {
      this.active = true;
      logger.info(`${this.instanceName} - Publisher became active.`);
      this.processQueue();
      return;
    }

    const { referenceHash, batchSize } = snapshot;
    let resumeIndex = -1;

    // Use batchSize as the window length.
    for (let i = 0; i <= this.queue.length - batchSize; i++) {
      const currentBatch = this.queue.slice(i, i + batchSize);
      const currentHash = this.computeHashForBatch(currentBatch);
      if (currentHash === referenceHash) {
        resumeIndex = i + batchSize; // resume immediately after the matched batch
        break;
      }
    }

    if (resumeIndex !== -1) {
      // Adjust the queue so that processing resumes after the matching batch.
      this.queue = this.queue.slice(resumeIndex);
      logger.info(`${this.instanceName} - Publisher resumed from the matched snapshot position.`);
    } else if (this.queue.length > 0) {
      // Log info message and process the entire queue if no match is found.
      logger.info(`${this.instanceName} - No matching batch found during resume; processing the entire publisher's queue.`);
    }

    this.active = true;
    logger.info(`${this.instanceName} - Publisher became active.`);
    this.processQueue();
  }

  /**
   * Switch the publisher to a passive state and return a snapshot
   */
  public async becomePassive(n: number): Promise<{ referenceHash: string; batchSize: number; }> {
    this.active = false;
    logger.info(`${this.instanceName} - Publisher became passive.`);
    const batch = this.queue.slice(-n);
    const batchSize = batch.length;
    const referenceHash = this.computeHashForBatch(batch);
    if (batchSize > 0) {
      logger.info(`${this.instanceName} - Publisher batch reference hash: ${referenceHash}, size: ${batchSize}`);
    }
    return { referenceHash, batchSize };
  }

  public getState(): boolean {
    return this.active;
  }

  /**
   * Process a single publish request.
   */
  protected async processItem(item: PublisherQueueItem): Promise<void> {
    try {
      await this.publishFunction(item.topic, item.message, item.id, item.options);
      item.resolve();
    } catch (error) {
      item.reject(error);
    }
  }

  /**
   * Serialize a publish request for persistence.
   */
  protected serializeItem(item: PublisherQueueItem): any {
    return {
      topic: item.topic,
      message: item.message,
      id: item.id,
      options: item.options,
    };
  }

  /**
   * Deserialize persisted data into a publish request.
   */
  protected deserializeItem(data: any): PublisherQueueItem {
    return {
      topic: data.topic,
      message: data.message,
      id: data.id,
      options: data.options,
      resolve: () => {},
      reject: (err: any) => logger.error(`${this.instanceName} - Error processing queued publish item: ${err.message}`),
    };
  }
}

/**
 * Interface representing a queued subscribe item.
 */
interface SubscriberQueueItem {
  topic: string;
  message: string;
  timestamp: number;
}

/**
 * ThrottledSubscriber: manages a queue of incoming messages and processes
 * them one at a time using a provided message handler.
 */
export class ThrottledSubscriber extends ThrottledQueue<SubscriberQueueItem> {
  private messageHandler: (topic: string, message: string) => Promise<void>;

  /**
   * @param delay Delay between processing messages in milliseconds.
   * @param messageHandler Function to process incoming messages.
   * @param persistToDisk Whether to persist the queue to disk.
   * @param persistenceFilePath File path for queue persistence.
   * @param instanceName Unique instance name for logging.
   */
  constructor(
    delay: number,
    messageHandler: (topic: string, message: string) => Promise<void>,
    persistToDisk: boolean = false,
    persistenceFilePath: string = join(basePath, "/subscriberQueue/", "throttled-subscriber-queue.json"),
    instanceName: string,
    active: boolean,
  ) {
    super(delay, persistToDisk, instanceName);
    this.persistenceFilePath = persistenceFilePath;
    this.messageHandler = messageHandler;
    this.active = active;
    if (active) {
      logger.info(`${this.instanceName} - Subscriber is active.`);
    } else {
      logger.info(`${this.instanceName} - Subscriber is passive.`);
    }
  }

  /**
   * Enqueue subscribe messages for processing.
   */
  public enqueue(topic: string, message: string): void {
    this.queue.push({ topic, message, timestamp: Date.now() });
    
    // Trigger logging after a new message is added.
    this.logQueueSize();
    
    if (this.persistToDisk) {
      this.saveQueueToDisk();
    }

    if (this.active) {
      this.processQueue();
    }
  }

  /**
   * Process a single subscribe message.
   */
  protected async processItem(item: SubscriberQueueItem): Promise<void> {
    await this.messageHandler(item.topic, item.message);
  }

  /**
   * Serialize a subscribe message for persistence.
   */
  protected serializeItem(item: SubscriberQueueItem): any {
    return {
      message: item.message,
    };
  }

  /**
   * Deserialize persisted data into a subscribe message.
   */
  protected deserializeItem(data: any): SubscriberQueueItem {
    return {
      topic: data.topic,
      message: data.message,
      timestamp: data.timestamp,
    };
  }

  /**
   * Get the current state of the subscriber (active or passive).
   */
  public getState(): boolean {
    return this.active;
  }

  /**
   * Computes a SHA‑256 hash for a given batch of subscriber items.
   */
  private computeHashForBatch(messages: SubscriberQueueItem[], strict = false): string {
    const data = JSON.stringify(messages.map((item) => strict ? { topic: item.topic, message: item.message } : this.serializeItem(item)));
    return createHash("sha256").update(data).digest("hex");
  }

  /**
   * Switch the subscriber to a passive state and return a snapshot.
   */
  public async becomePassive(n: number, strict = false): Promise<{ referenceHash: string; batchSize: number; }> {
    this.active = false;
    logger.info(`${this.instanceName} - Subscriber is passive.`);
    while (this.inFlight > 0) await new Promise(resolve => setTimeout(resolve, 1));
    const batch = this.lastProcessedItems.slice(-n);
    const batchSize = batch.length;
    const referenceHash = this.computeHashForBatch(batch, strict);
    if (batchSize > 0) {
      logger.info(`${this.instanceName} - Subscriber batch reference hash: ${referenceHash}, size: ${batchSize}`);
    }
    return { referenceHash, batchSize };
  }

  /**
   * Switch the subscriber to an active state and resume processing.
   */
  private preparedResume: number | undefined;

  /** Validate a unique overlap without consuming input or activating this owner. */
  public prepareResume(snapshot: { referenceHash: string; batchSize: number }): void {
    if (this.active) throw Error("MQTT successor is not passive");
    if (snapshot.batchSize === 0) { this.preparedResume = 0; return; }
    const matches: number[] = [];
    for (let i = 0; i <= this.queue.length - snapshot.batchSize; i++) {
      if (this.computeHashForBatch(this.queue.slice(i, i + snapshot.batchSize), true) === snapshot.referenceHash)
        matches.push(i + snapshot.batchSize);
    }
    if (matches.length !== 1) throw Error("MQTT input overlap is missing or ambiguous; successor stays passive");
    this.preparedResume = matches[0];
  }

  public activatePreparedResume(): void {
    if (this.active || this.preparedResume === undefined) throw Error("MQTT resume was not prepared");
    this.queue = this.queue.slice(this.preparedResume);
    this.preparedResume = undefined;
    this.active = true;
    void this.processQueue();
  }

  public becomeActive(snapshot: { referenceHash: string; batchSize: number }): void {
    // If snapshot is empty (batchSize is zero), start processing from the beginning.
    if (snapshot.batchSize === 0 || snapshot.batchSize === undefined || snapshot.referenceHash === undefined) {
      this.active = true;
      logger.info(`${this.instanceName} - Subscriber became active.`);      
      this.processQueue();
      return;
    }

    const { referenceHash, batchSize } = snapshot;
    let resumeIndex = -1;

    // Use batchSize as the window length.
    for (let i = 0; i <= this.queue.length - batchSize; i++) {
      const currentBatch = this.queue.slice(i, i + batchSize);
      const currentHash = this.computeHashForBatch(currentBatch);
      if (currentHash === referenceHash) {
        resumeIndex = i + batchSize; // resume immediately after the matched batch
        break;
      }
    }

    if (resumeIndex !== -1) {
      // Adjust the queue so that processing resumes after the matching batch.
      const queueSlice = this.queue.slice(resumeIndex); 
      this.queue = queueSlice;
      logger.info(`${this.instanceName} - Subscriber resumed from the matched snapshot position.`);
    } else if (this.queue.length > 0) {
      // Log info message and process the entire queue if no match is found.
      logger.info(`${this.instanceName} - No matching batch found during resume; processing the entire subscriber's queue.`);
    }

    this.active = true;
    logger.info(`${this.instanceName} - Subscriber became active.`);      
    this.processQueue();
  }
}
