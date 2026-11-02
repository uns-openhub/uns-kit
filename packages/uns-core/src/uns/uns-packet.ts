import * as crypto from "crypto";
import * as zlib from "zlib";
import logger from '../logger.js';
import {
  IUnsPacket,
  ISO8601,  
  IUnsData,
  IUnsMessage,
  isIOS8601Type,
  IUnsPackatParameters,
  IUnsExtendedMessage,
  IUnsExtendedData,
  IUnsTable,
  IUnsTableColumn,
  IUnsTableColumns,
  isQuestDbType,
  scalarDataValueType
} from "./uns-interfaces.js";

// Version of the MQTT packet wire contract emitted by this library.
const unsPacketVersion = "2.0.0";
const supportedLegacyPacketVersion = /^1\.\d+\.\d+(?:[-+].*)?$/;
const canonicalColumnNamePattern = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;
const reservedColumnNames = new Set(["__proto__", "prototype", "constructor"]);

type UnknownRecord = Record<string, unknown>;

const isRecord = (value: unknown): value is UnknownRecord =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isSupportedPacketVersion = (value: unknown): value is string =>
  typeof value === "string" &&
  (value === unsPacketVersion || supportedLegacyPacketVersion.test(value));

const assertColumnName = (name: string, requireCanonicalName: boolean): void => {
  if (!name.trim()) {
    throw new Error("Table column names must be non-empty strings");
  }
  if (reservedColumnNames.has(name)) {
    throw new Error(`Column name '${name}' is reserved`);
  }
  if (requireCanonicalName && !canonicalColumnNamePattern.test(name)) {
    throw new Error(
      `Column name '${name}' must match ${canonicalColumnNamePattern.source}`,
    );
  }
};

const normalizeColumn = (
  name: string,
  rawColumn: unknown,
  rejectEmbeddedName: boolean,
): IUnsTableColumn => {
  if (!isRecord(rawColumn)) {
    throw new Error(`Column '${name}' must be an object`);
  }
  if (rejectEmbeddedName && Object.hasOwn(rawColumn, "name")) {
    throw new Error(`Column '${name}' must not contain a name property`);
  }
  if (!Object.hasOwn(rawColumn, "type") || rawColumn.type === undefined || rawColumn.type === null || rawColumn.type === "") {
    throw new Error(`Column '${name}' is missing a QuestDB type`);
  }
  if (!isQuestDbType(rawColumn.type)) {
    throw new Error(`Column '${name}' has invalid QuestDB type '${String(rawColumn.type)}'`);
  }
  if (!Object.hasOwn(rawColumn, "value") || rawColumn.value === undefined) {
    throw new Error(`Column '${name}' is missing a value`);
  }
  const value = rawColumn.value;
  if (
    typeof value !== "number" &&
    typeof value !== "string" &&
    typeof value !== "boolean" &&
    value !== null
  ) {
    throw new Error(
      `Value for column '${name}' must be number, string, boolean, or null`,
    );
  }
  if (rawColumn.uom !== undefined && typeof rawColumn.uom !== "string") {
    throw new Error(`Column '${name}' uom must be a string when provided`);
  }

  return {
    type: rawColumn.type,
    value: value as IUnsTableColumn["value"],
    ...(typeof rawColumn.uom === "string"
      ? { uom: rawColumn.uom as IUnsTableColumn["uom"] }
      : {}),
  };
};

const normalizeCanonicalColumns = (input: unknown): IUnsTableColumns => {
  if (!isRecord(input) || Object.keys(input).length === 0) {
    throw new Error("Table.columns must be a non-empty object");
  }

  return Object.fromEntries(
    Object.entries(input).map(([name, rawColumn]) => {
      assertColumnName(name, true);
      return [name, normalizeColumn(name, rawColumn, true)];
    }),
  );
};

const normalizeInboundColumns = (input: unknown): IUnsTableColumns => {
  if (Array.isArray(input)) {
    if (input.length === 0) {
      throw new Error("Table.columns must be a non-empty array or object");
    }
    const names = new Set<string>();
    const entries = input.map((rawColumn, index): [string, IUnsTableColumn] => {
      if (!isRecord(rawColumn)) {
        throw new Error(`Column at index ${index} must be an object`);
      }
      const name = rawColumn.name;
      if (typeof name !== "string") {
        throw new Error(`Column at index ${index} is missing a name`);
      }
      assertColumnName(name, false);
      if (names.has(name)) {
        throw new Error(`Table.columns contains duplicate column name '${name}'`);
      }
      names.add(name);
      return [name, normalizeColumn(name, rawColumn, false)];
    });
    return Object.fromEntries(entries);
  }

  return normalizeCanonicalColumns(input);
};

const normalizeTable = (
  input: unknown,
  direction: "inbound" | "outbound",
): IUnsTable => {
  if (!isRecord(input)) {
    throw new Error("Table must be an object");
  }
  const columns = direction === "inbound"
    ? normalizeInboundColumns(input.columns)
    : normalizeCanonicalColumns(input.columns);
  return { ...input, columns } as unknown as IUnsTable;
};

export const tableColumnEntries = (
  columns: IUnsTableColumns,
): Array<[string, IUnsTableColumn]> => Object.entries(columns);

export class UnsPacket {
  /**
   * The given TypeScript function parseMqttPacket is used to parse an MQTT packet,
   * which is presumed to be in JSON format. The goal is to transform this string into
   * a structured object that adheres to the IUnsPacket interface, or return null
   * if parsing fails.
   *
   * @param mqttPacket
   * @param instanceName 
   * @returns IUnsPacket
   */
  public static parseMqttPacket(mqttPacket: string, instanceName?: string): IUnsPacket | null {
    try {
      const parsedMqttPacket: unknown = JSON.parse(mqttPacket);
      if (!isRecord(parsedMqttPacket)) {
        throw new Error("Packet must be an object");
      }
      if (!isSupportedPacketVersion(parsedMqttPacket.version)) {
        throw new Error(`Unsupported or missing packet version '${String(parsedMqttPacket.version)}'`);
      }
      if (!isRecord(parsedMqttPacket.message)) {
        throw new Error("Packet.message must be an object");
      }

      const rawMessage = parsedMqttPacket.message;
      const data = rawMessage.data as IUnsExtendedData | undefined;
      const table = rawMessage.table === undefined
        ? undefined
        : normalizeTable(rawMessage.table, "inbound");
      const expiresAt = rawMessage.expiresAt as ISO8601 | undefined;
      const createdAt = rawMessage.createdAt as ISO8601 | undefined;

      UnsPacket.validateMessageComponents(data, table);

      const message: IUnsExtendedMessage = {
        ...(data !== undefined && { data }),
        ...(table !== undefined && { table }),
        ...(expiresAt !== undefined && { expiresAt }),
        ...(createdAt !== undefined && { createdAt }),
      };

      return {
        message,
        version: parsedMqttPacket.version,
        ...(typeof parsedMqttPacket.messageSignature === "string"
          ? { messageSignature: parsedMqttPacket.messageSignature }
          : {}),
        ...(typeof parsedMqttPacket.interval === "number"
          ? { interval: parsedMqttPacket.interval }
          : {}),
      };
    } catch (error) {
      if (instanceName) {
        logger.error(`${instanceName} - Could not parse packet: ${error}`);
      } else {
        logger.error(`Could not parse packet: ${error}`);
      }
      return null;
    }
  }

  /**
   * Validates the data and table objects to ensure they have the required properties
   * and that those properties have the correct types.
   * 
   * @param data - The data object to validate
   * @param table - The table object to validate
   * @returns boolean | null
   */
  private static validateMessageComponents(data?: IUnsData, table?: IUnsTable): boolean | null {
    const isIsoOrEpoch = (value: any): boolean => {
      if (value === null || value === undefined) return true;
      if (typeof value === "number") return Number.isFinite(value);
      return isIOS8601Type(value);
    };

    if (data) {
      if (data.dataGroup) {
        if (!/^[A-Za-z0-9_.\-]+$/.test(data.dataGroup)) {
          throw new Error(`dataGroup must be a valid name (alphanumeric, underscores, hyphens and dots only)`);
        }
      }

      if (!data.time)
        throw new Error(`Time is not defined in data object`);
      if (!isIOS8601Type(data.time))
        throw new Error(`Time is not ISO8601`);
      if (data.value === undefined) {
        throw new Error(`Value is not defined in data object`);
      }
      scalarDataValueType(data.value, data.valueType);
      if (data.intervalStart !== undefined && !isIsoOrEpoch(data.intervalStart)) {
        throw new Error(`intervalStart is not ISO8601 or epoch ms`);
      }
      if (data.intervalEnd !== undefined && !isIsoOrEpoch(data.intervalEnd)) {
        throw new Error(`intervalEnd is not ISO8601 or epoch ms`);
      }
      if ((data as any).windowStart !== undefined && !isIsoOrEpoch((data as any).windowStart)) {
        throw new Error(`windowStart is not ISO8601 or epoch ms`);
      }
      if ((data as any).windowEnd !== undefined && !isIsoOrEpoch((data as any).windowEnd)) {
        throw new Error(`windowEnd is not ISO8601 or epoch ms`);
      }
      if (data.lastSeen !== undefined && !isIsoOrEpoch(data.lastSeen)) {
        throw new Error(`lastSeen is not ISO8601 or epoch ms`);
      }
      if (data.deletedAt !== undefined && !isIsoOrEpoch(data.deletedAt)) {
        throw new Error(`deletedAt is not ISO8601 or epoch ms`);
      }
      if (data.eventId && typeof data.eventId !== "string") {
        throw new Error(`eventId must be a string`);
      }
      if (data.deleted !== undefined && typeof data.deleted !== "boolean") {
        throw new Error(`deleted must be a boolean`);
      }
    }

    // Check table object
    if (table) {
      if (table.dataGroup) {
        if (!/^[A-Za-z0-9_.\-]+$/.test(table.dataGroup)) {
          throw new Error(`dataGroup must be a valid name (alphanumeric, underscores, hyphens and dots only)`);
        }
      }

      if (!table.time) {
        throw new Error(`Time is not defined in data object`);
      }
      if (!isIOS8601Type(table.time)) {
        throw new Error(`Time is not ISO8601`);
      }
      if (table.intervalStart !== undefined && !isIsoOrEpoch(table.intervalStart)) {
        throw new Error(`intervalStart is not ISO8601 or epoch ms`);
      }
      if (table.intervalEnd !== undefined && !isIsoOrEpoch(table.intervalEnd)) {
        throw new Error(`intervalEnd is not ISO8601 or epoch ms`);
      }
      if ((table as any).windowStart !== undefined && !isIsoOrEpoch((table as any).windowStart)) {
        throw new Error(`windowStart is not ISO8601 or epoch ms`);
      }
      if ((table as any).windowEnd !== undefined && !isIsoOrEpoch((table as any).windowEnd)) {
        throw new Error(`windowEnd is not ISO8601 or epoch ms`);
      }
      if (table.lastSeen !== undefined && !isIsoOrEpoch(table.lastSeen)) {
        throw new Error(`lastSeen is not ISO8601 or epoch ms`);
      }
      if (table.deletedAt !== undefined && !isIsoOrEpoch(table.deletedAt)) {
        throw new Error(`deletedAt is not ISO8601 or epoch ms`);
      }
      if (table.eventId && typeof table.eventId !== "string") {
        throw new Error(`eventId must be a string`);
      }
      if (table.deleted !== undefined && typeof table.deleted !== "boolean") {
        throw new Error(`deleted must be a boolean`);
      }

      if (!isRecord(table.columns) || Object.keys(table.columns).length === 0) {
        throw new Error("Table.columns must be a non-empty object");
      }
    }

    return true;
  }

  static async unsPacketFromUnsMessage(
    message: IUnsMessage,
    _unsPackatParameters?: IUnsPackatParameters,
  ): Promise<IUnsPacket> {
    const table = message.table === undefined
      ? undefined
      : normalizeTable(message.table, "outbound");

    // Validate the packet in message
    if (UnsPacket.validateMessageComponents(message.data, table)) {

        // HMAC
        // const algorithm = "sha256";
        // const key = "your-secret-key";
        // const packetMessage = JSON.stringify(message);
        // const hmacHash = this.generateHmac(algorithm, key, packetMessage);
  
        // const unsPacket: IUnsPacket = {
        //   message,
        //   messageSignature: hmacHash,
        //   version: unsPacketVersion
        // };

        let data: IUnsExtendedData | undefined;

        if (message.data && message.data.value !== undefined) {
          const valueType = scalarDataValueType(message.data.value, message.data.valueType);
          data = {
            ...message.data,
            valueType
          }
        };

        const extendedMessage: IUnsExtendedMessage = {
          ...(data !== undefined && { data }),
          ...(table !== undefined && { table }),
          ...(message.expiresAt !== undefined && { expiresAt: message.expiresAt }),
          ...(message.createdAt !== undefined && { createdAt: message.createdAt }),
        };

        const unsPacket: IUnsPacket = {
          message: extendedMessage,
          version: unsPacketVersion,
        };
        
        return unsPacket;
    }
    throw new Error("Could not create packet from message");
  }

  private static generateHmac(
    algorithm: string,
    key: string,
    data: string,
  ): string {
    const hmac = crypto.createHmac(algorithm, key);
    hmac.update(data);
    return hmac.digest("hex");
  }

  static formatToISO8601(date: Date): ISO8601 {
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, "0");
    const day = String(date.getUTCDate()).padStart(2, "0");
    const hours = String(date.getUTCHours()).padStart(2, "0");
    const minutes = String(date.getUTCMinutes()).padStart(2, "0");
    const seconds = String(date.getUTCSeconds()).padStart(2, "0");
    const milliseconds = String(date.getUTCMilliseconds()).padStart(3, "0");
    return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}.${milliseconds}Z`;
  }

  private static compressString(input: string): Promise<string> {
    return new Promise((resolve, reject) => {
      zlib.deflate(input, (err, buffer) => {
        if (err) {
          reject(err);
        } else {
          const base64String = buffer.toString("base64");
          resolve(base64String);
        }
      });
    });
  }

  // Function to decompress a Buffer back to a string
  private static decompressString(base64Compressed: string): Promise<string> {
    return new Promise((resolve, reject) => {
      // Decode the base64 string to a Buffer
      const compressedBuffer = Buffer.from(base64Compressed, "base64");

      zlib.inflate(compressedBuffer, (err, buffer) => {
        if (err) {
          reject(err);
        } else {
          resolve(buffer.toString());
        }
      });
    });
  }
}
