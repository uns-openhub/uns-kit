// status-monitor.ts
import logger from "../logger.js";
import type MqttProxy from "../uns-mqtt/mqtt-proxy.js";
import { ACTIVE_STATUS_EXPIRY_SECONDS, INACTIVE_STATUS_EXPIRY_SECONDS } from "./process-config.js";
import { IUnsMessage } from "./uns-interfaces.js";
import { DataSizeMeasurements } from "./uns-measurements.js";
import { UnsPacket } from "./uns-packet.js";

export class StatusMonitor {
  private memoryInterval?: NodeJS.Timeout;
  private statusInterval?: NodeJS.Timeout;

  /**
   * @param mqttProxy The MQTT proxy used to publish messages.
   * @param processStatusTopic The base topic for the process (ending with a slash).
   * @param activeSupplier A function returning the current active state (boolean).
   * @param memoryIntervalMs Interval in milliseconds for publishing memory status.
   * @param statusIntervalMs Interval in milliseconds for publishing active status.
   * @param processIdentity Optional identity to attach to active status messages.
   */
  constructor(
    private mqttProxy: MqttProxy,
    private processStatusTopic: string,
    private activeSupplier: () => boolean,
    private memoryIntervalMs: number,
    private statusIntervalMs: number,
    private processIdentity?: { processName: string; processId: string },
  ) {}

  /**
   * Starts the periodic memory and status updates.
   */
  public start(): void {
    this.memoryInterval = setInterval(() => this.publishMemoryUpdates(), this.memoryIntervalMs);
    this.statusInterval = setInterval(() => this.publishStatusUpdates(), this.statusIntervalMs);
  }

  /**
   * Stops the periodic updates.
   */
  public stop(): void {
    if (this.memoryInterval) {
      clearInterval(this.memoryInterval);
      this.memoryInterval = undefined;
    }
    if (this.statusInterval) {
      clearInterval(this.statusInterval);
      this.statusInterval = undefined;
    }
  }

  /**
   * Publishes memory usage updates via MQTT.
   * In this example, only the `heapUsed` metric is published.
   */
  private async publishMemoryUpdates(): Promise<void> {
    try {
      const time = UnsPacket.formatToISO8601(new Date());
      const memoryUsage = process.memoryUsage();
      const heapUsedMessage: IUnsMessage = { data: { time, value: Math.round(memoryUsage.heapUsed / 1048576), uom: DataSizeMeasurements.MegaByte } };
      let packet = await UnsPacket.unsPacketFromUnsMessage(heapUsedMessage);
      this.mqttProxy.publish(`${this.processStatusTopic}heap-used`, JSON.stringify(packet));

      const heapTotalMessage: IUnsMessage = {
        data: { time, value: Math.round(memoryUsage.heapTotal / 1048576), uom: DataSizeMeasurements.MegaByte },
      };
      packet = await UnsPacket.unsPacketFromUnsMessage(heapTotalMessage);
      this.mqttProxy.publish(`${this.processStatusTopic}heap-total`, JSON.stringify(packet));
    } catch (error: any) {
      logger.error(`StatusMonitor - Error publishing memory updates: ${error.message}`);
    }
  }

  /**
   * Publishes the process active status via MQTT.
   */
  private async publishStatusUpdates(): Promise<void> {
    await this.publishActiveStatus(this.activeSupplier(), ACTIVE_STATUS_EXPIRY_SECONDS);
  }

  /**
   * Replaces a retained active heartbeat during a graceful process shutdown.
   * The short expiry means a stopped process cannot look active to a later
   * handover candidate, while existing subscribers can still observe passive.
   */
  public async publishInactiveStatus(): Promise<void> {
    await this.publishActiveStatus(false, INACTIVE_STATUS_EXPIRY_SECONDS);
  }

  private async publishActiveStatus(active: boolean, messageExpiryInterval: number): Promise<void> {
    try {
      const time = UnsPacket.formatToISO8601(new Date());
      const activeMessage: IUnsMessage = {
        data: { time, value: Number(active) },
      };
      const packet = await UnsPacket.unsPacketFromUnsMessage(activeMessage);
      await this.mqttProxy.publish(`${this.processStatusTopic}active`, JSON.stringify(packet), {
        retain: true,
        properties: this.processIdentity
          ? {
              messageExpiryInterval,
              userProperties: {
                processName: this.processIdentity.processName,
                processId: this.processIdentity.processId,
              },
            }
          : { messageExpiryInterval },
      });
    } catch (error: any) {
      logger.error(`StatusMonitor - Error publishing status updates: ${error.message}`);
    }
  }
}
