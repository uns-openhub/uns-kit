import { IApiObject, ITopicObject, UnsEvents } from "./uns-interfaces.js";
import logger from "../logger.js";
import { UnsEventEmitter } from "./uns-event-emitter.js";
import { UnsPacket } from "./uns-packet.js";
import { UnsAsset } from "./uns-asset.js";
import { UnsObjectId, UnsObjectType } from "./uns-object.js";
import { IApiCatchallMapping } from "./uns-interfaces.js";
import { buildUnsIdentityPath } from "./uns-path.js";

export default class UnsProxy {
  private publishInterval: NodeJS.Timeout | null = null;
  public event: UnsEventEmitter<UnsEvents> = new UnsEventEmitter<UnsEvents>();
  protected instanceStatusTopic: string;
  protected instanceNameWithSuffix: string; //was prot
  private producedTopics: Map<string, ITopicObject> = new Map();
  private producedApiEndpoints: Map<string, IApiObject> = new Map();
  private producedServiceEndpoints: Map<string, IApiObject> = new Map();
  private producedDataOfferEndpoints: Map<string, IApiObject> = new Map();
  private producedApiCatchall: Map<string, IApiCatchallMapping> = new Map();
  private producedDataCatalogOffers: Map<string, unknown> = new Map();
  private readonly controllerNameEnv: string | undefined;
  private readonly controllerHostEnv: string | undefined;
  private readonly controllerPortEnv: string | undefined;
  private readonly controllerPublicBaseEnv: string | undefined;

  constructor() {
    this.controllerNameEnv = process.env["UNS_CONTROLLER_NAME"];
    this.controllerHostEnv = process.env["UNS_CONTROLLER_HOST"];
    this.controllerPortEnv = process.env["UNS_CONTROLLER_PORT"];
    this.controllerPublicBaseEnv =
      process.env["UNS_CONTROLLER_PUBLIC_BASE"] ?? process.env["UNS_PUBLIC_BASE"];
    // Set up interval to publish produced topics every 60 seconds.
    this.publishInterval = setInterval(() => {
      this.emitProducedTopics();
      this.emitProducedApiEndpoints();
      this.emitProducedServiceEndpoints();
      this.emitProducedDataOfferEndpoints();
      this.emitProducedApiCatchall();
      this.emitProducedDataCatalogOffers();
    }, 60000);    
  }
      
  /**
   * Publishes the list of produced topics to the MQTT broker.
   */
  private async emitProducedTopics(): Promise<void> {
    if (this.instanceStatusTopic !== "") {
      const topicsArray = [...this.producedTopics.values()];
      if (topicsArray.length > 0) {
        try {
          this.event.emit("unsProxyProducedTopics", { producedTopics: topicsArray, statusTopic: this.instanceStatusTopic + "topics" });
        } catch (error) {
          logger.error(`${this.instanceNameWithSuffix} - Error publishing produced topics: ${error.message}`);
        }
        logger.debug(`${this.instanceNameWithSuffix} - Published produced topics.`);
      }
    }
  }

  /**
   * Publishes the list of produced API endpoints to the MQTT broker.
   */
  private async emitProducedApiEndpoints(): Promise<void> {
    if (this.instanceStatusTopic !== "") {
      const apiEndpointsArray = [...this.producedApiEndpoints.values()];
      if (apiEndpointsArray.length > 0) {
        try {
          if (apiEndpointsArray.length > 0) {
            this.event.emit("unsProxyProducedApiEndpoints", { producedApiEndpoints: apiEndpointsArray, statusTopic: this.instanceStatusTopic + "api-endpoints" });
          }
        } catch (error) {
          logger.error(`${this.instanceNameWithSuffix} - Error publishing produced API endpoints: ${error.message}`);
        }
        logger.debug(`${this.instanceNameWithSuffix} - Published produced API endpoints.`);
      }
    }
  }

  /**
   * Publishes the list of catch-all API mappings to the MQTT broker.
   */
  private async emitProducedApiCatchall(): Promise<void> {
    if (this.instanceStatusTopic !== "") {
      const catchallArray = [...this.producedApiCatchall.values()];
      if (catchallArray.length > 0) {
        try {
          this.event.emit("unsProxyProducedApiCatchAll", {
            producedCatchall: catchallArray,
            statusTopic: this.instanceStatusTopic + "api-catchall",
          });
        } catch (error: any) {
          logger.error(`${this.instanceNameWithSuffix} - Error publishing catch-all API mappings: ${error.message}`);
        }
        logger.debug(`${this.instanceNameWithSuffix} - Published catch-all API mappings.`);
      }
    }
  }

  private async emitProducedServiceEndpoints(): Promise<void> {
    if (this.instanceStatusTopic !== "") {
      const serviceEndpointsArray = [...this.producedServiceEndpoints.values()];
      if (serviceEndpointsArray.length > 0) {
        try {
          this.event.emit("unsProxyProducedServiceEndpoints", {
            producedServiceEndpoints: serviceEndpointsArray,
            statusTopic: this.instanceStatusTopic + "service-endpoints",
          });
        } catch (error) {
          logger.error(`${this.instanceNameWithSuffix} - Error publishing produced service endpoints: ${error.message}`);
        }
        logger.debug(`${this.instanceNameWithSuffix} - Published produced service endpoints.`);
      }
    }
  }

  private async emitProducedDataOfferEndpoints(): Promise<void> {
    if (this.instanceStatusTopic !== "") {
      const dataOfferEndpointsArray = [...this.producedDataOfferEndpoints.values()];
      if (dataOfferEndpointsArray.length > 0) {
        try {
          this.event.emit("unsProxyProducedDataOfferEndpoints", {
            producedDataOfferEndpoints: dataOfferEndpointsArray,
            statusTopic: this.instanceStatusTopic + "data-offer-endpoints",
          });
        } catch (error) {
          logger.error(`${this.instanceNameWithSuffix} - Error publishing produced data offer endpoints: ${error.message}`);
        }
        logger.debug(`${this.instanceNameWithSuffix} - Published produced data offer endpoints.`);
      }
    }
  }

  private async emitProducedDataCatalogOffers(): Promise<void> {
    if (this.instanceStatusTopic !== "") {
      const offersArray = [...this.producedDataCatalogOffers.values()];
      if (offersArray.length > 0) {
        try {
          this.event.emit("unsProxyProducedDataCatalogOffers", {
            producedDataCatalogOffers: offersArray,
            statusTopic: this.instanceStatusTopic + "data-catalog-offers",
          });
        } catch (error: any) {
          logger.error(`${this.instanceNameWithSuffix} - Error publishing data catalog offers: ${error.message}`);
        }
        logger.debug(`${this.instanceNameWithSuffix} - Published data catalog offers.`);
      }
    }
  }

  /**
   * Registers a unique topic and keeps its timestamp current.
   *
   * On first call the full entry is created and immediately emitted so the
   * controller can persist the new schema node.  On subsequent calls only the
   * timestamp is updated in-place; the 60-second heartbeat interval will carry
   * the updated value to the controller, making the timestamp reflect the time
   * of the most recent data publication rather than the one-off registration
   * moment.
   *
   * @param topicObject - The object containing topic details.
   */
  protected registerUniqueTopic(topicObject: ITopicObject): void {
    if (this.instanceStatusTopic !== "") {
      const fullTopic = buildUnsIdentityPath(
        topicObject.topic,
        topicObject.asset,
        topicObject.objectType,
        topicObject.objectId,
        topicObject.attribute,
      );
      if (!this.producedTopics.has(fullTopic)) {
        this.producedTopics.set(fullTopic, {
          timestamp: topicObject.timestamp,
          topic: topicObject.topic,
          attribute: topicObject.attribute,
          attributeType: topicObject.attributeType,
          description: topicObject.description,
          tags: topicObject.tags,
          attributeNeedsPersistence: topicObject.attributeNeedsPersistence ?? true,
          ...(topicObject.valueType ? { valueType: topicObject.valueType } : {}),
          ...(topicObject.presentationKind ? { presentationKind: topicObject.presentationKind } : {}),
          ...(topicObject.defaultAggregation ? { defaultAggregation: topicObject.defaultAggregation } : {}),
          ...(topicObject.counterResetPolicy ? { counterResetPolicy: topicObject.counterResetPolicy } : {}),
          ...(topicObject.tableColumns?.length ? { tableColumns: topicObject.tableColumns } : {}),
          ...(topicObject.systemRole ? { systemRole: topicObject.systemRole } : {}),
          ...(topicObject.relationshipEvidence ? { relationshipEvidence: topicObject.relationshipEvidence } : {}),
          ...(topicObject.lifecycle ? { lifecycle: topicObject.lifecycle } : {}),
          dataGroup: topicObject.dataGroup ?? "",
          ...(topicObject.virtualGroup ? { virtualGroup: topicObject.virtualGroup } : {}),
          asset: topicObject.asset,
          assetDescription: topicObject.assetDescription,
          ...(topicObject.assetStableEntityId && topicObject.assetIdentityProof ? {
            assetStableEntityId: topicObject.assetStableEntityId,
            ...(topicObject.assetDisplayName ? { assetDisplayName: topicObject.assetDisplayName } : {}),
            assetIdentityProof: topicObject.assetIdentityProof,
          } : {}),
          ...(topicObject.assetProviderIdentity && topicObject.assetProviderIdentityProof ? {
            assetProviderIdentity: topicObject.assetProviderIdentity,
            assetProviderIdentityProof: topicObject.assetProviderIdentityProof,
          } : {}),
          objectType: topicObject.objectType,
          objectTypeDescription: topicObject.objectTypeDescription,
          objectId: topicObject.objectId,
          ...(topicObject.validityMode ? { validityMode: topicObject.validityMode } : {}),
          ...(topicObject.expectedIntervalMs ? { expectedIntervalMs: topicObject.expectedIntervalMs } : {}),
          ...(topicObject.lifecycleEndValue ? { lifecycleEndValue: topicObject.lifecycleEndValue } : {}),
        });
        this.emitProducedTopics();
        logger.info(`${this.instanceNameWithSuffix} - Registered new topic: ${fullTopic}`);
      } else {
        // Already registered — refresh only the timestamp so the periodic
        // heartbeat reflects actual data flow rather than frozen startup time.
        const existing = this.producedTopics.get(fullTopic)!;
        existing.timestamp = topicObject.timestamp;
        const identityChanged = existing.assetStableEntityId !== topicObject.assetStableEntityId
          || existing.assetDisplayName !== topicObject.assetDisplayName
          || existing.assetIdentityProof !== topicObject.assetIdentityProof
          || JSON.stringify(existing.assetProviderIdentity) !== JSON.stringify(topicObject.assetProviderIdentity)
          || existing.assetProviderIdentityProof !== topicObject.assetProviderIdentityProof;
        if (topicObject.assetStableEntityId && topicObject.assetIdentityProof) {
          existing.assetStableEntityId = topicObject.assetStableEntityId;
          existing.assetIdentityProof = topicObject.assetIdentityProof;
          if (topicObject.assetDisplayName) existing.assetDisplayName = topicObject.assetDisplayName;
          else delete existing.assetDisplayName;
        } else {
          delete existing.assetStableEntityId;
          delete existing.assetDisplayName;
          delete existing.assetIdentityProof;
        }
        if (topicObject.assetProviderIdentity && topicObject.assetProviderIdentityProof) {
          existing.assetProviderIdentity = topicObject.assetProviderIdentity;
          existing.assetProviderIdentityProof = topicObject.assetProviderIdentityProof;
        } else {
          delete existing.assetProviderIdentity;
          delete existing.assetProviderIdentityProof;
        }
        if (identityChanged) this.emitProducedTopics();
      }
    }
  }

  /**
   * Registers an API endpoint to handle requests for a specific topic and attribute.
   */
  protected registerApiEndpoint(apiObject: IApiObject): void {
    if (this.instanceStatusTopic !== "") {
      const fullTopic = buildUnsIdentityPath(
        apiObject.topic,
        apiObject.asset,
        apiObject.objectType,
        apiObject.objectId,
        apiObject.attribute,
      );
      const targetRegistry =
        apiObject.registryTopic === "service-endpoints"
          ? this.producedServiceEndpoints
          : apiObject.registryTopic === "data-offer-endpoints"
            ? this.producedDataOfferEndpoints
          : this.producedApiEndpoints;
      if (!targetRegistry.has(fullTopic)) {
        const time = UnsPacket.formatToISO8601(new Date());
        targetRegistry.set(fullTopic, {
          timestamp: time,
          topic: apiObject.topic,
          attribute: apiObject.attribute,
          ...(apiObject.routeOnly === true ? { routeOnly: true } : {}),
          ...(apiObject.registryTopic ? { registryTopic: apiObject.registryTopic } : {}),
          apiHost: apiObject.apiHost,
          apiEndpoint: apiObject.apiEndpoint,
          apiMethod: apiObject.apiMethod,
          apiQueryParams: apiObject.apiQueryParams,
          apiDescription: apiObject.apiDescription,
          ...(apiObject.serviceApi ? { serviceApi: apiObject.serviceApi } : {}),
          attributeType: apiObject.attributeType,
          apiSwaggerEndpoint: apiObject.apiSwaggerEndpoint,
          asset: apiObject.asset,
          objectType: apiObject.objectType,
          objectId: apiObject.objectId,
          ...(this.controllerNameEnv ? { controllerName: this.controllerNameEnv } : {}),
          ...(this.controllerHostEnv ? { controllerHost: this.controllerHostEnv } : {}),
          ...(this.controllerPortEnv ? { controllerPort: this.controllerPortEnv } : {}),
          ...(this.controllerPublicBaseEnv ? { controllerPublicBase: this.controllerPublicBaseEnv } : {}),
        });
        if (apiObject.registryTopic === "service-endpoints") {
          this.emitProducedServiceEndpoints();
          logger.info(`${this.instanceNameWithSuffix} - Registered new service endpoint: /${fullTopic}`);
        } else if (apiObject.registryTopic === "data-offer-endpoints") {
          this.emitProducedDataOfferEndpoints();
          logger.info(`${this.instanceNameWithSuffix} - Registered new data offer endpoint: /${fullTopic}`);
        } else {
          this.emitProducedApiEndpoints();
          logger.info(`${this.instanceNameWithSuffix} - Registered new api endpoint: /${fullTopic}`);
        }
      }
    }
  }

  protected registerApiCatchAll(mapping: IApiCatchallMapping): void {
    if (this.instanceStatusTopic !== "") {
      const key = mapping.topic.replace(/\/+$/, "");
      if (!this.producedApiCatchall.has(key)) {
        this.producedApiCatchall.set(key, mapping);
      } else {
        this.producedApiCatchall.set(key, { ...this.producedApiCatchall.get(key)!, ...mapping });
      }
      this.emitProducedApiCatchall();
      logger.info(`${this.instanceNameWithSuffix} - Registered catch-all API mapping: ${key}`);
    }
  }

  protected unregisterApiEndpoint(topic: string, asset:UnsAsset, objectType: UnsObjectType, objectId: UnsObjectId, attribute: string): void {
    const fullTopic = buildUnsIdentityPath(topic, asset, objectType, objectId, attribute);
    let removed = false;
    if (this.producedApiEndpoints.has(fullTopic)) {
      this.producedApiEndpoints.delete(fullTopic);
      this.emitProducedApiEndpoints();
      removed = true;
    }
    if (this.producedServiceEndpoints.has(fullTopic)) {
      this.producedServiceEndpoints.delete(fullTopic);
      this.emitProducedServiceEndpoints();
      removed = true;
    }
    if (this.producedDataOfferEndpoints.has(fullTopic)) {
      this.producedDataOfferEndpoints.delete(fullTopic);
      this.emitProducedDataOfferEndpoints();
      removed = true;
    }
    if (removed) {
      logger.info(`${this.instanceNameWithSuffix} - Unregistered API endpoint: ${fullTopic}`);
    }
  }

  protected registerDataCatalogOffer(offer: { offerId: string } & Record<string, unknown>): void {
    if (this.instanceStatusTopic === "") {
      return;
    }
    const offerId = typeof offer.offerId === "string" ? offer.offerId.trim() : "";
    if (!offerId) {
      return;
    }
    this.producedDataCatalogOffers.set(offerId, offer);
    this.emitProducedDataCatalogOffers();
    logger.info(`${this.instanceNameWithSuffix} - Registered data catalog offer: ${offerId}`);
  }
  public async stop() {
    // Clear the publishing interval.
    if (this.publishInterval) {
      clearInterval(this.publishInterval);
      this.publishInterval = null;
    }

  }

}
