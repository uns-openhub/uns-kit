import { UnsAttributeType } from "../graphql/schema.js";
import { MeasurementUnit } from "./uns-measurements.js";
import { UnsTags } from "./uns-tags.js";
import type { UnsObjectId, UnsObjectType } from "./uns-object.js";
import type { UnsAsset } from "./uns-asset.js";
import type { IMqttConnectProperties, IMqttPublishOptions, IMqttServerConfig, MqttProtocol } from "../uns-mqtt/mqtt-interfaces.js";
import { UnsTopics } from "./uns-topics.js";
import { knownUnsAttributes, type KnownUnsAttributeName } from "./uns-attributes.js";

export type ISO8601 = `${number}-${string}-${string}T${string}:${string}:${string}.${string}Z`;
export function isIOS8601Type(value: string): value is ISO8601 {
  const iso8601Regex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
  return iso8601Regex.test(value);
}
// Known attribute names (with IntelliSense) while still allowing arbitrary strings.
export type UnsAttribute = KnownUnsAttributeName | (string & {});

export const valueTypes = ["string", "number"] as const;
export type ValueTypeString = (typeof valueTypes)[number];
export type ScalarValueType = string | number;
/** Scalar telemetry or a scalar array used by metadata such as relationship evidence. */
export type ValueType = ScalarValueType | ScalarValueType[];

export type CounterResetPolicy = "new-value" | "null" | "rollover" | (string & {});

export type UnsAttributeSystemRole =
  | "relationship-evidence"
  | "lifecycle-start"
  | "lifecycle-end"
  | "lifecycle-time-source"
  | (string & {});

export type UnsRelationshipEndpoint = "source" | "target";

export interface IUnsRelationshipEvidenceMetadata {
  relationshipKey: string;
  ownerEndpoint?: UnsRelationshipEndpoint;
  valueEndpoint?: UnsRelationshipEndpoint;
  /** Scalar values produce one edge; array values produce one edge per ObjectId value. */
  sourceObjectType?: UnsObjectType | (string & {});
  targetObjectType?: UnsObjectType | (string & {});
  sourceObjectIdFrom?: string;
  targetObjectIdFrom?: string;
  observedAtFrom?: string;
  status?: "suggested" | "accepted" | (string & {});
  defaultStatus?: "suggested" | "accepted" | (string & {});
}

export interface IUnsLifecycleMetadata {
  timestampFrom?: "value" | "packetTimestamp" | (string & {});
  startValues?: string[];
  endValues?: string[];
}

export interface IUnsTableColumnMetadata {
  name: string;
  valueType?: ValueTypeString | (string & {});
  presentationKind?: string;
  defaultAggregation?: string;
  counterResetPolicy?: CounterResetPolicy;
}

export const questDbPrimitiveTypes = [
  "boolean",
  "ipv4",
  "byte",
  "short",
  "char",
  "int",
  "float",
  "symbol",
  "varchar",
  "string",
  "long",
  "date",
  "timestamp",
  "timestamp_ns",
  "double",
  "uuid",
  "binary",
  "long256",
] as const;

const questDbPrimitiveTypeSet = new Set<string>(questDbPrimitiveTypes);
const questDbGeohashRegex = /^geohash\(\d+[bc]\)$/;
const questDbDecimalRegex = /^decimal\(\d+,\d+\)$/;
const questDbArrayRegex = /^array<[^>]+>$/;

export type QuestDbPrimitiveType = (typeof questDbPrimitiveTypes)[number];

// Supported QuestDB column types for UNS tables.
export type QuestDbType =
  | QuestDbPrimitiveType
  | `geohash(${number}${"b" | "c"})`
  | `decimal(${number},${number})`
  | `array<${string}>`;

export function isQuestDbType(value: unknown): value is QuestDbType {
  if (typeof value !== "string") {
    return false;
  }
  if (questDbPrimitiveTypeSet.has(value)) {
    return true;
  }
  return (
    questDbGeohashRegex.test(value) ||
    questDbDecimalRegex.test(value) ||
    questDbArrayRegex.test(value)
  );
}

export interface IUnsParameters {
  mqttSubToTopics?: string | string[];
  username?: string;
  password?: string;
  mqttSSL?: boolean;
  publishThrottlingDelay?: number; // Delay in milliseconds; default is 1ms
  publishConcurrency?: number; // Maximum concurrent outgoing publishes; default is 1
  maxPendingPublishes?: number; // Maximum pending publishes across the API boundary and worker; default is unlimited
  subscribeThrottlingDelay?: number; // Delay in milliseconds; default is 1ms
  rejectUnauthorized?: boolean;
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
  /**
   * Default MQTT publish options applied to all outgoing messages
   * (e.g., qos, retain, messageExpiryInterval).
   */
  defaultPublishOptions?: IMqttPublishOptions;
}

export interface IUnsProcessParameters {
  processName: string;
  /**
   * Optional controller-supplied correlation id for one cooperative handover.
   * Older peers ignore it, so it must never be required for normal startup.
   */
  handoverId?: string;
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
  rejectUnauthorized?: boolean;
  properties?: IMqttConnectProperties;
  ca?: string;
  cert?: string;
  key?: string;
  servername?: string;
}

export interface UnsEvents {
  // Emitters in MqttProxy, UnsMqttProxy
  input: { topic: string; message: string, packet: any };
  mqttProxyStatus: { event: string, value: number, uom: MeasurementUnit, statusTopic: string };
  
  // Emitters in MqttProxy
  error: { code: number; message: string };
  
  // Emitters in UnsMqttProxy
  mqttWorker: { command: string, instanceName: string; batchSize: number, referenceHash: string };

  // Emitter in UnsCronProxy
  cronEvent: { event?: string; cronExpression?: string };

  // Emitters in UnsApiProxy
  apiGetEvent: {req: any, res: any};
  apiPostEvent: {req: any, res: any};
  apiPutEvent: {req: any, res: any};
  apiPatchEvent: {req: any, res: any};
  apiDeleteEvent: {req: any, res: any};

  // Emitters in UnsProxy
  unsProxyProducedTopics: { producedTopics: ITopicObject[], statusTopic: string };

  // Emitters in UnsProxy
  unsProxyProducedApiEndpoints: { producedApiEndpoints: IApiObject[], statusTopic: string };

  // Emitters in UnsProxy
  unsProxyProducedServiceEndpoints: { producedServiceEndpoints: IApiObject[], statusTopic: string };

  // Emitters in UnsProxy
  unsProxyProducedDataOfferEndpoints: { producedDataOfferEndpoints: IApiObject[], statusTopic: string };

  // Emitters in UnsProxy
  unsProxyProducedApiCatchAll: { producedCatchall: IApiCatchallMapping[], statusTopic: string };

  // Emitters in UnsProxy / UnsApiProxy
  unsProxyProducedDataCatalogOffers: { producedDataCatalogOffers: unknown[], statusTopic: string };
}

export interface IUnsExtendedData extends IUnsData {
  valueType: ValueTypeString;
}

export interface IUnsData {
  /** Domain-specific evidence/context fields may accompany the canonical value. */
  [key: string]: unknown;
  time: ISO8601;
  value: ValueType;
  /** Storage/routing group for consumers such as archivers; not part of the UNS identity path. */
  dataGroup?: string;
  uom?: MeasurementUnit;
  foreignEventKey?: string;
  intervalStart?: ISO8601 | number;
  intervalEnd?: ISO8601 | number;
  windowStart?: ISO8601 | number;
  windowEnd?: ISO8601 | number;
  eventId?: string;
  deleted?: boolean;
  deletedAt?: ISO8601 | number;
  lastSeen?: ISO8601 | number;
}

export interface IUnsTableColumn {
  type: QuestDbType;
  value: string | number | boolean | null;
  uom?: MeasurementUnit;
}

/** MQTT table columns keyed by their stable UNS/QuestDB column name. */
export type IUnsTableColumns = Record<string, IUnsTableColumn>;

export interface IUnsTable {
  time: ISO8601;
  /** Storage/routing group for consumers such as archivers; not part of the UNS identity path. */
  dataGroup?: string;
  columns: IUnsTableColumns;
  intervalStart?: ISO8601 | number;
  intervalEnd?: ISO8601 | number;
  windowStart?: ISO8601 | number;
  windowEnd?: ISO8601 | number;
  eventId?: string;
  deleted?: boolean;
  deletedAt?: ISO8601 | number;
  lastSeen?: ISO8601 | number;
}

export interface IMqttAttributeMessage {
  attribute: UnsAttribute;
  description?: string;
  tags?: UnsTags[];
  attributeNeedsPersistence?: boolean | null;
  /** Optional controller/UI grouping hint for this ObjectId node; does not affect storage/table naming. */
  virtualGroup?: string;
  /** Scalar value shape for schema/catalog consumers, for example "number" or "string". */
  valueType?: ValueTypeString | (string & {});
  /**
   * Operator-facing presentation hint. Use "counter" for cumulative counter
   * state; delta/rate should be requested from Datahub history APIs.
   */
  presentationKind?: string;
  /** Preferred aggregation for sampled history views, for example "last". */
  defaultAggregation?: string;
  /** Reset handling hint for cumulative counters. Interpreted by Datahub APIs, not by the producer. */
  counterResetPolicy?: CounterResetPolicy;
  /** Field-level metadata for table columns that should behave as chartable series. */
  tableColumns?: IUnsTableColumnMetadata[];
  /** Optional schema-system role published into Datahub's attribute_schema.schema_json. */
  systemRole?: UnsAttributeSystemRole;
  /** Relationship materialization metadata for `systemRole: "relationship-evidence"`. */
  relationshipEvidence?: IUnsRelationshipEvidenceMetadata;
  /** Lifecycle metadata for lifecycle system roles. */
  lifecycle?: IUnsLifecycleMetadata;
  /**
   * How the controller determines if this attribute is live or stale.
   * Defaults to `"interval"` with the controller's default interval (~120s) when omitted.
   *
   * @example
   * // Sensor publishing every 2 seconds:
   * { validityMode: "interval", expectedIntervalMs: 2000 }
   *
   * // Status attribute publishing every 2 seconds (string values like "HEATING"):
   * { validityMode: "interval", expectedIntervalMs: 2000 }
   *
   * // Material location with ENTERED/EXITED lifecycle:
   * { validityMode: "lifecycle", lifecycleEndValue: "EXITED" }
   *
   * // Static configuration value:
   * { validityMode: "static" }
   */
  validityMode?: ValidityMode;
  /**
   * Expected publish interval in milliseconds. Only meaningful for `validityMode: "interval"`.
   * The controller marks the attribute as stale after ~2× this value without a heartbeat update.
   * Ignored for `"lifecycle"` and `"static"` modes.
   */
  expectedIntervalMs?: number;
  /**
   * The string value that marks a lifecycle as completed. Only meaningful for `validityMode: "lifecycle"`.
   * When the attribute's current value matches this, the controller considers the lifecycle done.
   * Example: `"EXITED"` for a location attribute with ENTERED/EXITED events.
   */
  lifecycleEndValue?: string;
}

type AttributePayload =
  | { message: IUnsMessage; data?: never; table?: never; createdAt?: never; expiresAt?: never }
  | { message?: never; data: IUnsData; table?: never; createdAt?: ISO8601; expiresAt?: ISO8601 }
  | { message?: never; data?: never; table: IUnsTable; createdAt?: ISO8601; expiresAt?: ISO8601 };

export type IMqttAttributeEntry = IMqttAttributeMessage & AttributePayload;

export type IAssetProviderIdentity = {
  providerId: string;
  externalSystem: string;
  externalType: string;
  externalId: string;
};

export type IAssetIdentityPublicationMetadata =
  | {
      assetStableEntityId?: never;
      assetDisplayName?: never;
      assetIdentityProof?: never;
      assetProviderIdentity?: never;
      assetProviderIdentityProof?: never;
    }
  | {
      /** Controller-issued stable Asset UUID. It identifies the Asset, not its current path. */
      assetStableEntityId: string;
      /** Optional operator-facing name; never used as an identity key. */
      assetDisplayName?: string;
      /** Short-lived controller-signed proof bound to this Asset UUID and exact candidate path. */
      assetIdentityProof: string;
      assetProviderIdentity?: never;
      assetProviderIdentityProof?: never;
    }
  | {
      assetStableEntityId?: never;
      assetDisplayName?: never;
      assetIdentityProof?: never;
      /** Reviewed provider namespace plus external identifier for a provisional no-ID Asset. */
      assetProviderIdentity: IAssetProviderIdentity;
      /** Short-lived controller-signed proof bound to the provider identifier and exact candidate path. */
      assetProviderIdentityProof: string;
    };

export type IMqttPublishRequest = {
  topic: UnsTopics;
  asset: UnsAsset;
  assetDescription?: string;
  objectType: UnsObjectType;
  objectTypeDescription?: string;
  objectId: UnsObjectId;
  /** Optional controller/UI grouping hint for ObjectId nodes; does not affect storage/table naming. */
  virtualGroup?: string;
  attributes: IMqttAttributeEntry | IMqttAttributeEntry[];
} & IAssetIdentityPublicationMetadata;

// This interface represents a packet for a UNS system
export interface IUnsPacket {
  // The message object of the packet
  message: IUnsExtendedMessage;
  
  // The HMAC signature of the message
  messageSignature?: string;

  // Automatically calculated interval between two packets in ms
  interval?: number;

  // Current library version
  readonly version: string;

  // Autogenerated sequence number
  sequenceId?: number;
}

export interface IUnsPackatParameters {

}

export interface IUnsMessage {
    data?: IUnsData;
    table?: IUnsTable;
    expiresAt?: ISO8601;
    createdAt?: ISO8601;
}

export interface IUnsExtendedMessage extends IUnsMessage {
  data?: IUnsExtendedData;
}

/**
 * Controls how the controller determines whether an attribute is live or stale.
 *
 * - `"interval"` — Attribute publishes periodically. Stale if no heartbeat within ~2× the expected interval.
 *   Use with `expectedIntervalMs`. Works for both numeric sensors (temperature every 2s)
 *   and string status attributes (status every 2s). Example:
 *   `{ validityMode: "interval", expectedIntervalMs: 2000 }`
 *
 * - `"lifecycle"` — Attribute represents a lifecycle with a start and end event.
 *   Use with `lifecycleEndValue` to mark the end state. Example: material location with "ENTERED" / "EXITED".
 *
 * - `"static"` — Attribute is set once and never changes (configuration, metadata). Always live.
 *
 * When omitted, defaults to `"interval"` with the controller's default interval (~120s).
 */
export type ValidityMode = "interval" | "lifecycle" | "static";

export interface ITopicObject {
  timestamp: string;
  attribute:string;
  attributeType: UnsAttributeType;
  topic:string;
  description:string;
  /** Storage/routing group observed on the latest data/table payload for this identity. */
  dataGroup:string;
  /** Optional controller/UI grouping hint for ObjectId nodes; does not affect storage/table naming. */
  virtualGroup?: string;
  tags:string[] | null;
  attributeNeedsPersistence: boolean | null;
  /** Scalar value shape for schema/catalog consumers, for example "number" or "string". */
  valueType?: ValueTypeString | (string & {});
  /** Operator-facing presentation hint, for example "counter" for cumulative counter state. */
  presentationKind?: string;
  /** Preferred aggregation for sampled history views, for example "last". */
  defaultAggregation?: string;
  /** Reset handling hint for cumulative counters. Interpreted by Datahub APIs, not by the producer. */
  counterResetPolicy?: CounterResetPolicy;
  /** Field-level metadata for table columns that should behave as chartable series. */
  tableColumns?: IUnsTableColumnMetadata[];
  /** Optional schema-system role published into Datahub's attribute_schema.schema_json. */
  systemRole?: UnsAttributeSystemRole;
  /** Relationship materialization metadata for `systemRole: "relationship-evidence"`. */
  relationshipEvidence?: IUnsRelationshipEvidenceMetadata;
  /** Lifecycle metadata for lifecycle system roles. */
  lifecycle?: IUnsLifecycleMetadata;
  asset: UnsAsset;
  assetDescription?: string;
  /** Optional stable Asset UUID; accepted by the controller only with a valid publication proof. */
  assetStableEntityId?: string;
  /** Optional operator-facing name; never used as an identity key. */
  assetDisplayName?: string;
  /** Short-lived controller-signed evidence for this exact Asset path. */
  assetIdentityProof?: string;
  /** Reviewed provider namespace plus external identifier for a provisional no-ID Asset. */
  assetProviderIdentity?: IAssetProviderIdentity;
  /** Short-lived controller-signed evidence for this provider identifier and exact Asset path. */
  assetProviderIdentityProof?: string;
  objectType: UnsObjectType;
  objectTypeDescription?: string;
  objectId: UnsObjectId;
  /** Liveliness validity mode — how the controller determines if this attribute is live/stale. */
  validityMode?: ValidityMode;
  /** For interval mode: expected publish interval in milliseconds (stale after ~2x this value). */
  expectedIntervalMs?: number;
  /** For lifecycle mode: the string value that marks the lifecycle as completed (e.g. "EXITED"). */
  lifecycleEndValue?: string;
}

// API Interfaces below
export interface IApiObject {
  timestamp: string;
  attribute:string;
  topic:string;
  attributeType: UnsAttributeType;
  routeOnly?: boolean;
  registryTopic?: "api-endpoints" | "service-endpoints" | "data-offer-endpoints";
  apiDescription?: string; // Optional description for the API endpoint
  apiHost: string; // Hostname of the service
  apiEndpoint: string; // API endpoint for virtual topics
  apiSwaggerEndpoint: string; // Swagger endpoint for API documentation
  apiMethod: "GET" | "POST" | "PUT" | "PATCH" | "DELETE"; // HTTP method for API endpoint
  apiQueryParams: QueryParamDef[]; // query parameters for the API endpoint
  serviceApi?: Record<string, unknown> | null;
  asset: UnsAsset;
  objectType: UnsObjectType;
  objectId: UnsObjectId;
  controllerName?: string;
  controllerHost?: string;
  controllerPort?: string;
  controllerPublicBase?: string;
}

export interface IApiCatchallMapping {
  topic: string;
  apiBase: string;
  apiBasePath: string;
  swaggerPath: string;
}

export interface QueryParamDef {
  name: string;
  type: "string" | "number" | "boolean";
  required?: boolean;
  description?: string;
  /**
   * Optional canonical chat alias used by assistant tooling.
   * Typical values: from, to, limit, topic, timezone, summaryOnly.
   */
  chatCanonical?: string;
  /**
   * Optional default query value. Included in generated OpenAPI metadata.
   */
  defaultValue?: string | number | boolean;
}

export interface ApiChatDefaults {
  from?: string;
  to?: string;
  limit?: number;
  topic?: string;
  timezone?: string;
  summaryOnly?: boolean;
  [key: string]: string | number | boolean | undefined;
}

export interface IApiProxyOptions {
  jwtSecret?: string;
  /**
   * Public origin advertised for generated API endpoint metadata.
   *
   * Use a hostname or origin without a port (for example `127.0.0.1` or
   * `http://service-name`) to keep the API server's actual listening port.
   * An explicitly supplied port is preserved. Defaults to the machine's
   * external IPv4 address for backwards compatibility.
   */
  publishedApiHost?: string;
  jwks?: {
    wellKnownJwksUrl: string;
    activeKidUrl?: string;
    cacheTtlMs?: number;
    algorithms?: ("RS256" | "RS384" | "RS512")[];
  };
  /**
   * Optional base prefixes to mount the API and swagger JSON under (e.g. "/archiver-3").
   * Defaults to "/api" when not provided.
   */
  apiBasePath?: string;
  swaggerBasePath?: string;
  /**
   * Skip mounting the default "/api" route. Useful when rebasing entirely under a custom prefix.
   */
  disableDefaultApiMount?: boolean;
}

export interface IGetEndpointOptions {
  apiDescription?: string;
  tags?: string[];
  queryParams?: QueryParamDef[];
  routeOnly?: boolean;
  registryTopic?: "api-endpoints" | "service-endpoints" | "data-offer-endpoints";
  serviceApi?: Record<string, unknown>;
  /**
   * Optional defaults consumed by chat tooling (published as OpenAPI vendor extension x-uns-chat.defaults).
   */
  chatDefaults?: ApiChatDefaults;
}

export interface IPostEndpointOptions {
  apiDescription?: string;
  tags?: string[];
  routeOnly?: boolean;
  registryTopic?: "api-endpoints" | "service-endpoints" | "data-offer-endpoints";
  serviceApi?: Record<string, unknown>;
  requestBody?: {
    description?: string;
    required?: boolean;
    schema?: Record<string, unknown>;
  };
}

export interface IPutEndpointOptions extends IPostEndpointOptions {}
export interface IPatchEndpointOptions extends IPostEndpointOptions {}
export interface IDeleteEndpointOptions extends IPostEndpointOptions {}
