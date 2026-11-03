import { withServiceApiRequestBody } from "./service-api-request-body.js";
import { Router, type Response } from "express";
import { readFileSync } from "fs";
import jwt from "jsonwebtoken";
import * as path from "path";
import { createPublicKey } from "crypto";

import { basePath } from "@uns-kit/core/base-path.js";
import { UnsAttributeType } from "@uns-kit/core/graphql/schema.js";
import logger from "@uns-kit/core/logger.js";
import { MqttTopicBuilder } from "@uns-kit/core/uns-mqtt/mqtt-topic-builder.js";
import { UnsAttribute } from "@uns-kit/core/uns/uns-interfaces.js";
import { UnsPacket } from "@uns-kit/core/uns/uns-packet.js";
import UnsProxy from "@uns-kit/core/uns/uns-proxy.js";
import { UnsTags } from "@uns-kit/core/uns/uns-tags.js";
import { UnsTopicMatcher } from "@uns-kit/core/uns/uns-topic-matcher.js";
import { UnsTopics } from "@uns-kit/core/uns/uns-topics.js";
import {
  IApiObject,
  IApiProxyOptions,
  IGetEndpointOptions,
  IPostEndpointOptions,
} from "@uns-kit/core/uns/uns-interfaces.js";
import { DataSizeMeasurements, PhysicalMeasurements } from "@uns-kit/core/uns/uns-measurements.js";
import { buildUnsRoutePath } from "@uns-kit/core/uns/uns-path.js";
import App from "./app.js";
import { OperatorApiAuthority, operatorEndpointDeclarations, markOperatorNotApplied } from "./operator-api-authority.js";
import { OPERATOR_API_EXTENSION, OPERATOR_AUTH_SCHEME, OPERATOR_DELEGATION_CLAIM, OperatorApiError, type OperatorOperation } from "@uns-kit/core/runtime/operator-api.js";
import { OperatorJwtVerifier } from "./operator-jwt.js";
import { RuntimeRouteRegistration, runtimeRouteRegistrationEnvironment, type RuntimeRouteRegistrationEnvironment } from "./runtime-route-registration.js";
import { UnsAsset } from "@uns-kit/core/uns/uns-asset.js";
import { UnsObjectType, UnsObjectId } from "@uns-kit/core/uns/uns-object.js";
import type {
  DataCatalogOfferRegistration,
  DataCatalogOperationRegistration,
  DataCatalogParameterRegistration,
  DataCatalogRequestBodyRegistration,
  DataCatalogResponseRegistration,
  DataCatalogSchemaFieldRegistration,
  DataCatalogSchemaRegistration,
} from "./api-interfaces.js";

const packageJsonPath = path.join(basePath, "package.json");
const packageJson = JSON.parse(readFileSync(packageJsonPath, "utf8"));
const normalizeBasePrefix = (value?: string | null): string => {
  if (!value) return "";
  const trimmed = value.trim();
  if (!trimmed) return "";
  const withLeading = trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
  return withLeading.replace(/\/+$/, "");
};
const buildSwaggerPath = (base: string, processName: string, instanceName: string): string => {
  const processSegment = `/${processName}`;
  let baseWithProcess = base || "/";
  if (!baseWithProcess.endsWith(processSegment)) {
    baseWithProcess = `${baseWithProcess}${processSegment}`;
  }
  return `${baseWithProcess}/${instanceName}/swagger.json`.replace(/\/{2,}/g, "/");
};
const normalizeStringArray = (value: unknown): string[] =>
  Array.isArray(value)
    ? Array.from(
        new Set(
          value
            .map((entry) => (typeof entry === "string" ? entry.trim() : ""))
            .filter((entry) => entry.length > 0),
        ),
      )
    : [];
const normalizePathValue = (value: unknown): string | null => {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  return trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
};

export default class UnsApiProxy extends UnsProxy {
  public instanceName: string;
  private topicBuilder: MqttTopicBuilder;
  private processName: string;
  protected processStatusTopic: string;
  private app: App;
  private options: IApiProxyOptions;
  private apiBasePrefix: string;
  private swaggerBasePrefix: string;
  private jwksCache?: { keys: any[]; fetchedAt: number };
  private catchAllRouteRegistered = false;
  private startedAt: number;
  private statusInterval: NodeJS.Timeout | null = null;
  private readonly statusIntervalMs = 10_000;
  private readonly dataCatalogOffers = new Map<string, Record<string, unknown>>();
  private readonly operatorAuthority: OperatorApiAuthority;
  private readonly operatorJwt: OperatorJwtVerifier;
  private operatorRouteRegistration?: RuntimeRouteRegistration;

  private registerOperatorRoute(environment: RuntimeRouteRegistrationEnvironment | undefined): void {
    if (!environment || this.operatorRouteRegistration) return;
    this.operatorRouteRegistration = new RuntimeRouteRegistration({ ...environment, processName: this.processName,
      apiHost: this.getPublishedApiHost(), onState: ready => {
        if (!ready) logger.warn("Managed operator route registration unavailable; operator commands remain protected");
      } });
    this.operatorRouteRegistration.start();
  }

  constructor(processName: string, instanceName: string, options: IApiProxyOptions) {
    super();
    this.options = options;
    const controllerBase = process.env.UNS_CONTROLLER_PUBLIC_BASE;
    const statusUrl = options.operatorAuthorizationStatusUrl ?? (controllerBase ? new URL("/api/auth/token-status", controllerBase).href : undefined);
    this.operatorJwt = new OperatorJwtVerifier(options.operatorJwksUrl ?? (controllerBase ? new URL("/api/auth/operator-jwks.json", controllerBase).href : undefined));
    this.operatorAuthority = new OperatorApiAuthority({ ...(statusUrl ? { statusUrl } : {}), deployment: () => ({
      controllerName: process.env.UNS_ROUTE_CONTROLLER_NAME!, rttNode: process.env.RTT_NODE!,
      version: process.env.version ?? packageJson.version, instanceId: process.env.RTT_INSTANCE_ID!, launchId: process.env.UNS_ROUTE_LAUNCH_ID!,
    }) });
    this.apiBasePrefix =
      normalizeBasePrefix(options.apiBasePath ?? process.env.UNS_API_BASE_PATH) || "/api";
    const rawSwaggerBase =
      normalizeBasePrefix(options.swaggerBasePath ?? process.env.UNS_SWAGGER_BASE_PATH) || this.apiBasePrefix;
    this.swaggerBasePrefix = rawSwaggerBase.endsWith("/api")
      ? rawSwaggerBase.replace(/\/api\/?$/, "") || "/"
      : rawSwaggerBase;
    this.app = new App(0, processName, instanceName, undefined, {
      apiBasePrefix: this.apiBasePrefix,
      swaggerBasePrefix: this.swaggerBasePrefix,
      disableDefaultApiMount: options.disableDefaultApiMount ?? false,
    });
    this.app.start();
    this.startedAt = Date.now();

    this.instanceName = instanceName;
    this.processName = processName;
    // Create the topic builder using packageJson values and the processName.
    this.topicBuilder = new MqttTopicBuilder(`uns-infra/${MqttTopicBuilder.sanitizeTopicPart(packageJson.name)}/${MqttTopicBuilder.sanitizeTopicPart(packageJson.version)}/${MqttTopicBuilder.sanitizeTopicPart(processName)}/`);

    // Generate the processStatusTopic using the builder.
    this.processStatusTopic = this.topicBuilder.getProcessStatusTopic();
    
    // Derive the instanceStatusTopic by appending the instance name.
    this.instanceStatusTopic = this.processStatusTopic + instanceName + "/";

    // Concatenate processName with instanceName for the worker identification.
    this.instanceNameWithSuffix = `${processName}-${instanceName}`;

    this.registerHealthEndpoint();
    // Emit once after listeners are attached in the plugin, then on the regular cadence.
    setTimeout(() => this.emitStatusMetrics(), 0);
    this.statusInterval = setInterval(() => this.emitStatusMetrics(), this.statusIntervalMs);
  }

  public setOwnershipGate(gate: () => boolean): void { this.app.setOwnershipGate(gate); }
  public async drainRequests(): Promise<void> { await this.app.drainRequests(); }

  /** Track an explicitly read-only handler through completion, including client disconnects. */
  public async runReadOnlyRequest<T>(res: Response, operation: () => T | Promise<T>): Promise<T> {
    return this.app.runReadOnlyRequest(res, operation);
  }

  public getProcessName(): string {
    return this.processName;
  }

  private async operatorAuthorized(req: any, res: any, decoded: Record<string, unknown>, declarations: OperatorOperation[]): Promise<boolean> {
    try {
      const delegatedScheme = String(req.headers.authorization ?? "").startsWith(OPERATOR_AUTH_SCHEME + " ");
      if (delegatedScheme !== (decoded[OPERATOR_DELEGATION_CLAIM] !== undefined)) throw new OperatorApiError("operator_jwt_purpose_mismatch", 403);
      const normalizedAuthorization = delegatedScheme ? "Bearer " + req.headers.authorization.slice(OPERATOR_AUTH_SCHEME.length + 1) : req.headers.authorization;
      await this.operatorAuthority.authorize({ claims: decoded, declarations, payload: req.method === "GET" ? {} : req.body,
        authorization: normalizedAuthorization, query: req.query });
      // Existing application handlers inspect an already-verified bearer.
      req.headers.authorization = normalizedAuthorization;
      return true;
    } catch (error) {
      const status = error instanceof OperatorApiError ? error.status : 503;
      markOperatorNotApplied(res);
      res.status(status).json({ error: error instanceof OperatorApiError ? error.code : "operator_authorization_unavailable" });
      return false;
    }
  }

  private getPublishedApiHost(): string {
    const addressInfo = this.app.server.address();
    const port = addressInfo && typeof addressInfo === "object" ? addressInfo.port : undefined;
    const configuredHost =
      this.options.publishedApiHost?.trim() || process.env.UNS_PUBLISHED_API_HOST?.trim();

    if (configuredHost) {
      const origin = new URL(
        /^[a-z][a-z\d+.-]*:\/\//i.test(configuredHost)
          ? configuredHost
          : `http://${configuredHost}`,
      );
      if (!origin.port && port !== undefined) {
        origin.port = String(port);
      }
      origin.pathname = "";
      origin.search = "";
      origin.hash = "";
      return origin.origin;
    }

    return port === undefined
      ? `http://${App.getExternalIPv4()}`
      : `http://${App.getExternalIPv4()}:${port}`;
  }

  /**
   * Unregister endpoint
   * @param topic - The API topic
   * @param attribute - The attribute for the topic.
   * @param method - The HTTP method (e.g., "GET", "POST", "PUT", "DELETE").
   */
  public async unregister(
    topic: UnsTopics,
    asset: UnsAsset,
    objectType: UnsObjectType,
    objectId: UnsObjectId,
    attribute: UnsAttribute,
    method:  "GET" | "POST" | "PUT" | "DELETE" 
  ): Promise<void> {
    const fullPath = buildUnsRoutePath(topic, asset, objectType, objectId, attribute);
    const apiPath = `${this.apiBasePrefix}${fullPath}`.replace(/\/{2,}/g, "/");
    const methodKey = method.toLowerCase(); // Express stores method keys in lowercase

    // Remove route from router
    if (this.app.router?.stack) {
      this.app.router.stack = this.app.router.stack.filter((layer: any) => {
        return !(
          layer.route &&
          layer.route.path === fullPath &&
          layer.route.methods[methodKey]
        );
      });
    }

    // Remove from Swagger spec if path exists
    if (this.app.swaggerSpec?.paths?.[apiPath]) {
      delete this.app.swaggerSpec.paths[apiPath][methodKey];
      // If no methods remain for the path, delete the whole path
      if (Object.keys(this.app.swaggerSpec.paths[apiPath]).length === 0) {
        delete this.app.swaggerSpec.paths[apiPath];
      }
    }

    // Unregister from internal endpoint tracking
    this.unregisterApiEndpoint(topic, asset, objectType, objectId, attribute);
  }
  
  /**
   * Register a GET endpoint with optional JWT path filter.
   * @param topic - The API topic
   * @param attribute - The attribute for the topic.
   * @param options.description - Optional description.
   * @param options.tags - Optional tags.
   */
  public async get(topic: UnsTopics, asset:UnsAsset, objectType: UnsObjectType, objectId: UnsObjectId, attribute: UnsAttribute, options?: IGetEndpointOptions): Promise<void> {
    // Wait until the API server is started
    while (this.app.server.listening === false) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    const time = UnsPacket.formatToISO8601(new Date());
    const fullPath = buildUnsRoutePath(topic, asset, objectType, objectId, attribute);
    const apiPath = `${this.apiBasePrefix}${fullPath}`.replace(/\/{2,}/g, "/");
    const swaggerPath = buildSwaggerPath(this.swaggerBasePrefix, this.processName, this.instanceName);
    const operatorDeclarations = operatorEndpointDeclarations(options?.operatorOperations, "GET", fullPath,
      Boolean(this.options.jwks?.wellKnownJwksUrl || this.options.jwtSecret));
    const routeEnvironment = operatorDeclarations.length ? runtimeRouteRegistrationEnvironment(process.env) : undefined;
    try {
      this.registerApiEndpoint({
        timestamp: time,
        topic: topic,
        attribute: attribute,
        routeOnly: (options as { routeOnly?: boolean } | undefined)?.routeOnly === true,
        registryTopic:
          (options as { registryTopic?: "api-endpoints" | "service-endpoints" | "data-offer-endpoints" } | undefined)?.registryTopic ??
          "api-endpoints",
        apiHost: this.getPublishedApiHost(),
        apiEndpoint: apiPath,
        apiMethod: "GET",
        apiQueryParams: options?.queryParams ?? [],
        apiDescription: options?.apiDescription,
        serviceApi:
          (options as { serviceApi?: Record<string, unknown> } | undefined)?.serviceApi ?? null,
        attributeType: UnsAttributeType.Api,
        apiSwaggerEndpoint: swaggerPath,
        asset,
        objectType,
        objectId
      } as IApiObject);

      const handler = (req: any, res: any) => {
        // Query param validation
        if (options?.queryParams) {
          const missingParams = options.queryParams.filter((p) => p.required && req.query[p.name] === undefined).map((p) => p.name);

          if (missingParams.length > 0) {
            return res.status(400).json({ error: `Missing query params: ${missingParams.join(", ")}` });
          }

          // Optional: cast types (basic)
          for (const param of options.queryParams) {
            const value = req.query[param.name];
            if (value !== undefined) {
              switch (param.type) {
                case "number":
                  if (isNaN(Number(value))) {
                    return res.status(400).json({ error: `Query param ${param.name} must be a number` });
                  }
                  break;
                case "boolean":
                  if (!["true", "false", "1", "0"].includes(String(value))) {
                    return res.status(400).json({ error: `Query param ${param.name} must be boolean` });
                  }
                  break;
                // string: no check
              }
            }
          }
        }

        this.event.emit("apiGetEvent", { req, res });
      };

      // JWT or JWKS or open
      if (this.options?.jwks?.wellKnownJwksUrl) {
        this.app.router.get(fullPath, async (req: any, res: any) => {
          try {
            const token = this.extractBearerToken(req, res);
            if (!token) return; // response already sent

            const decoded: any = await this.verifyRequestToken(req, token);

            const accessRules: string[] | undefined = Array.isArray(decoded?.accessRules)
              ? decoded.accessRules
              : (typeof decoded?.pathFilter === "string" && decoded.pathFilter.length > 0
                ? [decoded.pathFilter]
                : undefined);

            const allowed = Array.isArray(accessRules)
              ? accessRules.some((rule) => UnsTopicMatcher.matches(rule, fullPath))
              : false;

            if (!allowed) {
              return res.status(403).json({ error: "Path not allowed by token access rules" });
            }

            if (!await this.operatorAuthorized(req, res, decoded, operatorDeclarations)) return;
            handler(req, res);
          } catch (err: any) {
            if (err instanceof OperatorApiError) { markOperatorNotApplied(res); return res.status(err.status).json({ error: err.code }); }
            return res.status(401).json({ error: "Invalid token" });
          }
        });
      } else if (this.options?.jwtSecret) {
        this.app.router.get(fullPath, async (req: any, res: any) => {
          const token = this.extractBearerToken(req, res);
          if (!token) return;
          try {
            const decoded: any = await this.verifyRequestToken(req, token);

            const accessRules: string[] | undefined = Array.isArray(decoded?.accessRules)
              ? decoded.accessRules
              : (typeof decoded?.pathFilter === "string" && decoded.pathFilter.length > 0
                ? [decoded.pathFilter]
                : undefined);

            const allowed = Array.isArray(accessRules)
              ? accessRules.some((rule) => UnsTopicMatcher.matches(rule, fullPath))
              : false;

            if (!allowed) {
              return res.status(403).json({ error: "Path not allowed by token access rules" });
            }

            if (!await this.operatorAuthorized(req, res, decoded, operatorDeclarations)) return;
            handler(req, res);
          } catch (err) {
            if (err instanceof OperatorApiError) { markOperatorNotApplied(res); return res.status(err.status).json({ error: err.code }); }
            return res.status(401).json({ error: "Invalid token" });
          }
        });
      } else {
        this.app.router.get(fullPath, handler);
      }

      if (this.app.swaggerSpec) {
        const queryParams = options?.queryParams || [];
        const canonicalParams = queryParams.reduce((acc, param) => {
          if (typeof param.chatCanonical === "string" && param.chatCanonical.trim().length) {
            acc[param.chatCanonical.trim()] = param.name;
          }
          return acc;
        }, {} as Record<string, string>);
        const chatDefaults: Record<string, string | number | boolean> = {};
        for (const param of queryParams) {
          if (param.defaultValue !== undefined) {
            chatDefaults[param.name] = param.defaultValue;
          }
        }
        const optionDefaults = options?.chatDefaults ?? {};
        for (const [key, value] of Object.entries(optionDefaults)) {
          if (value !== undefined) {
            chatDefaults[key] = value;
          }
        }
        const unsChatMeta =
          Object.keys(canonicalParams).length || Object.keys(chatDefaults).length
            ? {
                canonicalParams,
                defaults: chatDefaults,
              }
            : null;

        this.app.swaggerSpec.paths = this.app.swaggerSpec.paths || {};
        this.app.swaggerSpec.paths[apiPath] = {
          get: {
            summary: options?.apiDescription || "No description",
            tags: options?.tags || [],
            parameters: queryParams.map((p) => ({
              name: p.name,
              in: "query",
              required: !!p.required,
              schema: {
                type: p.type,
                ...(p.defaultValue !== undefined ? { default: p.defaultValue } : {}),
              },
              description: p.description,
              ...(p.chatCanonical ? { "x-uns-chat-canonical": p.chatCanonical } : {}),
            })),
            ...(unsChatMeta ? { "x-uns-chat": unsChatMeta } : {}),
            responses: {
              "200": { description: "OK" },
              "400": { description: "Bad Request" },
              "401": { description: "Unauthorized" },
              "403": { description: "Forbidden" },
            },
          },
        };
      }
      if (operatorDeclarations.length && this.app.swaggerSpec?.paths?.[apiPath]?.get) {
        this.app.swaggerSpec.paths[apiPath].get[OPERATOR_API_EXTENSION] = operatorDeclarations;
        this.registerOperatorRoute(routeEnvironment);
      }
    } catch (error) {
      logger.error(`${this.instanceNameWithSuffix} - Error publishing message to route ${fullPath}: ${error.message}`);
    }
  }

  /**
   * Register a catch-all API mapping for a topic prefix (e.g., "sij/acroni/#").
   * Does not create individual API attribute nodes; the controller treats this as a fallback.
   *
   * This is intended for use by the uns-api-global microservice, which acts as a
   * catch-all gateway for an entire topic namespace. Regular microservices should
   * NOT call this — use registerGetEndpoint() for individual attribute endpoints instead.
   */
  public async registerCatchAll(
    topicPrefix: string,
    options?: {
      apiBase?: string;
      apiBasePath?: string;
      swaggerPath?: string;
      swaggerDoc?: Record<string, unknown>;
      apiDescription?: string;
      tags?: string[];
      queryParams?: IGetEndpointOptions["queryParams"];
    }
  ): Promise<void> {
    while (this.app.server.listening === false) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    const finalOptions = options ?? {};
    const topicNormalized = topicPrefix.endsWith("/") ? topicPrefix : `${topicPrefix}`;

    const apiBase =
      typeof finalOptions?.apiBase === "string" && finalOptions.apiBase.length
        ? finalOptions.apiBase
        : this.getPublishedApiHost();
    const apiBasePath =
      typeof finalOptions?.apiBasePath === "string" && finalOptions.apiBasePath.length
        ? finalOptions.apiBasePath
        : "/api";
    const swaggerPath =
      typeof finalOptions?.swaggerPath === "string" && finalOptions.swaggerPath.length
        ? finalOptions.swaggerPath
        : `/${this.processName}/${this.instanceName}/catchall-swagger.json`;

    const normalizedSwaggerPath = swaggerPath.startsWith("/") ? swaggerPath : `/${swaggerPath}`;

    const swaggerDoc =
      finalOptions.swaggerDoc ||
      {
        openapi: "3.0.0",
        info: {
          title: "Catch-all API",
          version: "1.0.0",
        },
        paths: {
          "/api/{topicPath}": {
            get: {
              summary: finalOptions.apiDescription || "Catch-all handler",
              tags: finalOptions.tags || [],
              parameters: [
                {
                  name: "topicPath",
                  in: "path",
                  required: true,
                  schema: { type: "string" },
                  description: "Resolved UNS topic path",
                },
                ...(finalOptions.queryParams || []).map((p) => ({
                  name: p.name,
                  in: "query",
                  required: !!p.required,
                  schema: { type: p.type },
                  description: p.description,
                })),
              ],
              responses: {
                "200": { description: "OK" },
                "400": { description: "Bad Request" },
                "401": { description: "Unauthorized" },
                "403": { description: "Forbidden" },
              },
            },
          },
        },
      };

    this.app.registerSwaggerDoc(normalizedSwaggerPath, swaggerDoc);
    logger.info(
      `${this.instanceNameWithSuffix} - Catch-all Swagger available at ${normalizedSwaggerPath} (target ${apiBase.replace(/\/+$/, "")}${normalizedSwaggerPath})`
    );

    if (!this.catchAllRouteRegistered) {
      this.app.router.use((req: any, res: any) => {
        const topicPath = (req.path ?? "").replace(/^\/+/, "");
        req.params = { ...(req.params || {}), topicPath };
        this.event.emit("apiGetEvent", { req, res });
      });
      this.catchAllRouteRegistered = true;
    }

    this.registerApiCatchAll({
      topic: topicNormalized,
      apiBase,
      apiBasePath,
      swaggerPath,
    });
  }

  public async post(topic: UnsTopics, asset: UnsAsset, objectType: UnsObjectType, objectId: UnsObjectId, attribute: UnsAttribute, options?: IPostEndpointOptions): Promise<void> {
    await this.registerMutationEndpoint("POST", topic, asset, objectType, objectId, attribute, options);
  }

  public async put(topic: UnsTopics, asset: UnsAsset, objectType: UnsObjectType, objectId: UnsObjectId, attribute: UnsAttribute, options?: IPostEndpointOptions): Promise<void> {
    await this.registerMutationEndpoint("PUT", topic, asset, objectType, objectId, attribute, options);
  }

  public async patch(topic: UnsTopics, asset: UnsAsset, objectType: UnsObjectType, objectId: UnsObjectId, attribute: UnsAttribute, options?: IPostEndpointOptions): Promise<void> {
    await this.registerMutationEndpoint("PATCH", topic, asset, objectType, objectId, attribute, options);
  }

  public async delete(topic: UnsTopics, asset: UnsAsset, objectType: UnsObjectType, objectId: UnsObjectId, attribute: UnsAttribute, options?: IPostEndpointOptions): Promise<void> {
    await this.registerMutationEndpoint("DELETE", topic, asset, objectType, objectId, attribute, options);
  }

  private async registerMutationEndpoint(
    method: "POST" | "PUT" | "PATCH" | "DELETE",
    topic: UnsTopics,
    asset: UnsAsset,
    objectType: UnsObjectType,
    objectId: UnsObjectId,
    attribute: UnsAttribute,
    options?: IPostEndpointOptions,
  ): Promise<void> {
    while (this.app.server.listening === false) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    const time = UnsPacket.formatToISO8601(new Date());
    const fullPath = buildUnsRoutePath(topic, asset, objectType, objectId, attribute);
    const apiPath = `${this.apiBasePrefix}${fullPath}`.replace(/\/{2,}/g, "/");
    const swaggerPath = buildSwaggerPath(this.swaggerBasePrefix, this.processName, this.instanceName);
    const methodKey = method.toLowerCase() as "post" | "put" | "patch" | "delete";
    const operatorDeclarations = operatorEndpointDeclarations(options?.operatorOperations, method, fullPath,
      Boolean(this.options.jwks?.wellKnownJwksUrl || this.options.jwtSecret));
    const routeEnvironment = operatorDeclarations.length ? runtimeRouteRegistrationEnvironment(process.env) : undefined;

    try {
      this.registerApiEndpoint({
        timestamp: time,
        topic,
        attribute,
        routeOnly: (options as { routeOnly?: boolean } | undefined)?.routeOnly === true,
        registryTopic:
          (options as { registryTopic?: "api-endpoints" | "service-endpoints" | "data-offer-endpoints" } | undefined)?.registryTopic ??
          "api-endpoints",
        apiHost: this.getPublishedApiHost(),
        apiEndpoint: apiPath,
        apiMethod: method as any,
        apiQueryParams: [],
        apiDescription: options?.apiDescription,
        serviceApi: withServiceApiRequestBody(options?.serviceApi, options?.requestBody),
        attributeType: UnsAttributeType.Api,
        apiSwaggerEndpoint: swaggerPath,
        asset,
        objectType,
        objectId,
      } as IApiObject);

      const handler = (req: any, res: any) => {
        this.event.emit(this.getApiEventName(method) as any, { req, res });
      };

      const routerMethod = this.app.router[methodKey].bind(this.app.router) as (path: string, handler: any) => void;
      if (this.options?.jwks?.wellKnownJwksUrl) {
        routerMethod(fullPath, async (req: any, res: any) => {
          try {
            const token = this.extractBearerToken(req, res);
            if (!token) return;

            const decoded: any = await this.verifyRequestToken(req, token);

            const accessRules: string[] | undefined = Array.isArray(decoded?.accessRules)
              ? decoded.accessRules
              : (typeof decoded?.pathFilter === "string" && decoded.pathFilter.length > 0
                ? [decoded.pathFilter]
                : undefined);

            const allowed = Array.isArray(accessRules)
              ? accessRules.some((rule) => UnsTopicMatcher.matches(rule, fullPath))
              : false;

            if (!allowed) {
              return res.status(403).json({ error: "Path not allowed by token access rules" });
            }

            if (!await this.operatorAuthorized(req, res, decoded, operatorDeclarations)) return;
            handler(req, res);
          } catch (_err: any) {
            if (_err instanceof OperatorApiError) { markOperatorNotApplied(res); return res.status(_err.status).json({ error: _err.code }); }
            return res.status(401).json({ error: "Invalid token" });
          }
        });
      } else if (this.options?.jwtSecret) {
        routerMethod(fullPath, async (req: any, res: any) => {
          const token = this.extractBearerToken(req, res);
          if (!token) return;
          try {
            const decoded: any = await this.verifyRequestToken(req, token);

            const accessRules: string[] | undefined = Array.isArray(decoded?.accessRules)
              ? decoded.accessRules
              : (typeof decoded?.pathFilter === "string" && decoded.pathFilter.length > 0
                ? [decoded.pathFilter]
                : undefined);

            const allowed = Array.isArray(accessRules)
              ? accessRules.some((rule) => UnsTopicMatcher.matches(rule, fullPath))
              : false;

            if (!allowed) {
              return res.status(403).json({ error: "Path not allowed by token access rules" });
            }

            if (!await this.operatorAuthorized(req, res, decoded, operatorDeclarations)) return;
            handler(req, res);
          } catch (_err) {
            if (_err instanceof OperatorApiError) { markOperatorNotApplied(res); return res.status(_err.status).json({ error: _err.code }); }
            return res.status(401).json({ error: "Invalid token" });
          }
        });
      } else {
        routerMethod(fullPath, handler);
      }

      if (this.app.swaggerSpec) {
        const requestBody = options?.requestBody;
        this.app.swaggerSpec.paths = this.app.swaggerSpec.paths || {};
        this.app.swaggerSpec.paths[apiPath] = this.app.swaggerSpec.paths[apiPath] || {};
        this.app.swaggerSpec.paths[apiPath][methodKey] = {
          summary: options?.apiDescription || "No description",
          tags: options?.tags || [],
          ...(requestBody
            ? {
                requestBody: {
                  description: requestBody.description,
                  required: requestBody.required ?? true,
                  content: {
                    "application/json": {
                      schema: requestBody.schema ?? { type: "object" },
                    },
                  },
                },
              }
            : {}),
          responses: {
            "200": { description: "OK" },
            "400": { description: "Bad Request" },
            "401": { description: "Unauthorized" },
            "403": { description: "Forbidden" },
          },
        };
      }
      if (operatorDeclarations.length && this.app.swaggerSpec?.paths?.[apiPath]?.[methodKey]) {
        this.app.swaggerSpec.paths[apiPath][methodKey][OPERATOR_API_EXTENSION] = operatorDeclarations;
        this.registerOperatorRoute(routeEnvironment);
      }
    } catch (error) {
      logger.error(`${this.instanceNameWithSuffix} - Error registering ${method} route ${fullPath}: ${error.message}`);
    }
  }

  public registerDataOffer(input: DataCatalogOfferRegistration): void {
    const offerId = typeof input.offerId === "string" ? input.offerId.trim() : "";
    const displayName = typeof input.displayName === "string" ? input.displayName.trim() : "";
    if (!offerId || !displayName) {
      throw new Error("Data catalog offer requires non-empty offerId and displayName.");
    }

    const offerSchemas = (Array.isArray(input.schemas) ? input.schemas : []).map((schema) => this.normalizeDataOfferSchema(schema));
    const offerSchemaIndex = new Map(offerSchemas.map((schema) => [schema.id, schema] as const));
    const normalizedOperations = (Array.isArray(input.operations) ? input.operations : [])
      .map((operation, index) => this.normalizeDataOfferOperation(operation, offerId, index, offerSchemaIndex))
      .filter((operation): operation is ReturnType<UnsApiProxy["normalizeDataOfferOperation"]> => Boolean(operation));

    if (!normalizedOperations.length) {
      throw new Error(`Data catalog offer ${offerId} requires at least one operation.`);
    }

    const basePaths = Array.from(
      new Set(
        [
          ...(Array.isArray(input.basePaths) ? input.basePaths.map((entry) => normalizePathValue(entry)) : []),
          ...normalizedOperations.map((operation) => {
            const segments = operation.path.split("/").filter(Boolean);
            if (segments.length <= 1) {
              return operation.path;
            }
            return `/${segments.slice(0, -1).join("/")}`;
          }),
        ].filter((value): value is string => Boolean(value)),
      ),
    );

    this.publishDataCatalogOffer({
      offerId,
      displayName,
      description: typeof input.description === "string" ? input.description.trim() || null : null,
      owner: typeof input.owner === "string" ? input.owner.trim() || null : null,
      status: typeof input.status === "string" && input.status.trim() ? input.status.trim() : "available",
      tags: normalizeStringArray(input.tags),
      categories: normalizeStringArray(input.categories),
      microserviceName: typeof input.microserviceName === "string" && input.microserviceName.trim()
        ? input.microserviceName.trim()
        : this.processName,
      version: typeof input.version === "string" && input.version.trim() ? input.version.trim() : packageJson.version,
      swaggerPath: normalizePathValue(input.swaggerPath),
      basePaths,
      operations: normalizedOperations,
      schemas: offerSchemas,
      metadata: input.metadata ?? null,
      packageName: packageJson.name,
      processName: this.processName,
      processVersion: packageJson.version,
      instanceName: this.instanceName,
      controllerName: process.env["UNS_CONTROLLER_NAME"] ?? null,
      controllerPublicBase: process.env["UNS_CONTROLLER_PUBLIC_BASE"] ?? process.env["UNS_PUBLIC_BASE"] ?? null,
    });
  }

  private emitStatusMetrics(): void {
    const uptimeMinutes = Math.round((Date.now() - this.startedAt) / 60000);
    // Process-level status
    this.event.emit("mqttProxyStatus", {
      event: "uptime",
      value: uptimeMinutes,
      uom: PhysicalMeasurements.Minute,
      statusTopic: this.processStatusTopic + "uptime",
    });
    this.event.emit("mqttProxyStatus", {
      event: "alive",
      value: 1,
      uom: DataSizeMeasurements.Bit,
      statusTopic: this.processStatusTopic + "alive",
    });
    // Instance-level status
    this.event.emit("mqttProxyStatus", {
      event: "uptime",
      value: uptimeMinutes,
      uom: PhysicalMeasurements.Minute,
      statusTopic: this.instanceStatusTopic + "uptime",
    });
    this.event.emit("mqttProxyStatus", {
      event: "alive",
      value: 1,
      uom: DataSizeMeasurements.Bit,
      statusTopic: this.instanceStatusTopic + "alive",
    });
    this.emitDataCatalogOffers();
  }

  private registerHealthEndpoint() {
    const routePath = "/status";
    this.app.router.get(routePath, (_req: any, res: any) => {
      res.json({
        alive: true,
        processName: this.processName,
        instanceName: this.instanceName,
        package: packageJson.name,
        version: packageJson.version,
        startedAt: new Date(this.startedAt).toISOString(),
        uptimeMs: Date.now() - this.startedAt,
        timestamp: new Date().toISOString(),
      });
    });

    if (this.app.swaggerSpec) {
      this.app.swaggerSpec.paths = this.app.swaggerSpec.paths || {};
      const swaggerPath = `${this.apiBasePrefix}${routePath}`.replace(/\/{2,}/g, "/");
      this.app.swaggerSpec.paths[swaggerPath] = {
        get: {
          summary: "Health status",
          responses: {
            "200": { description: "OK" },
          },
        },
      };
    }
  }

  private getApiEventName(method: "GET" | "POST" | "PUT" | "PATCH" | "DELETE") {
    switch (method) {
      case "GET":
        return "apiGetEvent";
      case "POST":
        return "apiPostEvent";
      case "PUT":
        return "apiPutEvent";
      case "PATCH":
        return "apiPatchEvent";
      case "DELETE":
        return "apiDeleteEvent";
    }
  }

  private async verifyRequestToken(req: any, token: string): Promise<Record<string, unknown>> {
    if (String(req.headers.authorization ?? "").startsWith(OPERATOR_AUTH_SCHEME + " ")) return this.operatorJwt.verify(token);
    if (this.options.jwks?.wellKnownJwksUrl) {
      const key = await this.getPublicKeyFromJwks(token);
      return jwt.verify(token, key, { algorithms: this.options.jwks.algorithms || ["RS256"] }) as Record<string, unknown>;
    }
    return jwt.verify(token, process.env.JWT_SECRET || this.options.jwtSecret!) as Record<string, unknown>;
  }

  private extractBearerToken(req: any, res: any): string | undefined {
    const authHeader = req.headers["authorization"];
    if (typeof authHeader !== "string" || !(authHeader.startsWith("Bearer ") || authHeader.startsWith(OPERATOR_AUTH_SCHEME + " "))) {
      res.status(401).json({ error: "Missing or invalid Authorization header" });
      return undefined;
    }
    return authHeader.startsWith("Bearer ") ? authHeader.slice(7) : authHeader.slice(OPERATOR_AUTH_SCHEME.length + 1);
  }

  private async getPublicKeyFromJwks(token: string): Promise<string> {
    // Decode header to get kid
    const decoded: any = jwt.decode(token, { complete: true });
    const kid: string | undefined = decoded?.header?.kid;

    const keys = await this.fetchJwksKeys();

    let jwk = kid ? keys.find((k: any) => k.kid === kid) : undefined;

    // If no kid match and activeKidUrl configured, try that
    if (!jwk && this.options?.jwks?.activeKidUrl) {
      try {
        const resp = await fetch(this.options.jwks.activeKidUrl);
        if (resp.ok) {
          const activeKid = await resp.text();
          jwk = keys.find((k: any) => k.kid === activeKid.trim());
        }
      } catch (_) {
        // ignore and fall through
      }
    }

    // If still not found but only one key, use it
    if (!jwk && keys.length === 1) {
      jwk = keys[0];
    }

    if (!jwk) {
      throw new Error("Signing key not found in JWKS");
    }

    // Prefer x5c certificate if provided
    if (Array.isArray(jwk.x5c) && jwk.x5c.length > 0) {
      return this.certFromX5c(jwk.x5c[0]);
    }

    // Build PEM from JWK (RSA)
    if (jwk.kty === "RSA" && jwk.n && jwk.e) {
      const keyObj = createPublicKey({ key: { kty: "RSA", n: jwk.n, e: jwk.e }, format: "jwk" } as any);
      return keyObj.export({ type: "spki", format: "pem" }).toString();
    }

    throw new Error("Unsupported JWK format");
  }

  private async fetchJwksKeys(): Promise<any[]> {
    const ttl = this.options?.jwks?.cacheTtlMs ?? 5 * 60 * 1000; // default 5 minutes
    const now = Date.now();
    if (this.jwksCache && now - this.jwksCache.fetchedAt < ttl) {
      return this.jwksCache.keys;
    }

    const url = this.options.jwks.wellKnownJwksUrl;
    const resp = await fetch(url);
    if (!resp.ok) {
      throw new Error(`Failed to fetch JWKS (${resp.status})`);
    }
    const body: any = await resp.json();
    const keys = Array.isArray(body?.keys) ? body.keys : [];
    this.jwksCache = { keys, fetchedAt: now };
    return keys;
  }

  private certFromX5c(x5cFirst: string): string {
    const pemBody = x5cFirst.match(/.{1,64}/g)?.join("\n") ?? x5cFirst;
    return `-----BEGIN CERTIFICATE-----\n${pemBody}\n-----END CERTIFICATE-----\n`;
  }

  public async stop(): Promise<void> {
    this.operatorRouteRegistration?.stop();
    if (this.statusInterval) {
      clearInterval(this.statusInterval);
      this.statusInterval = null;
    }
    await this.app.stop();
    await super.stop();
  }

  private publishDataCatalogOffer(offer: { offerId: string } & Record<string, unknown>): void {
    const offerId = typeof offer.offerId === "string" ? offer.offerId.trim() : "";
    if (!offerId) {
      return;
    }
    this.dataCatalogOffers.set(offerId, offer);
    this.emitDataCatalogOffers();
    logger.info(`${this.instanceNameWithSuffix} - Registered data catalog offer: ${offerId}`);
  }

  private emitDataCatalogOffers(): void {
    if (this.instanceStatusTopic === "" || this.dataCatalogOffers.size === 0) {
      return;
    }
    (this.event as any).emit("unsProxyProducedDataCatalogOffers", {
      producedDataCatalogOffers: [...this.dataCatalogOffers.values()],
      statusTopic: this.instanceStatusTopic + "data-catalog-offers",
    });
  }

  private normalizeDataOfferOperation(
    operation: DataCatalogOperationRegistration,
    offerId: string,
    index: number,
    offerSchemaIndex: Map<string, ReturnType<UnsApiProxy["normalizeDataOfferSchema"]>>,
  ) {
    const method = typeof operation.method === "string" ? operation.method.trim().toUpperCase() : "";
    const path = normalizePathValue(operation.path);
    if (!method || !path) {
      return null;
    }
    return {
      id: typeof operation.id === "string" && operation.id.trim() ? operation.id.trim() : `${offerId}-${method.toLowerCase()}-${index + 1}`,
      method,
      path,
      summary: typeof operation.summary === "string" ? operation.summary.trim() || null : null,
      description: typeof operation.description === "string" ? operation.description.trim() || null : null,
      tags: normalizeStringArray(operation.tags),
      deprecated: operation.deprecated === true,
      parameters: (Array.isArray(operation.parameters) ? operation.parameters : []).map((parameter) =>
        this.normalizeDataOfferParameter(parameter),
      ),
      headers: (Array.isArray(operation.headers) ? operation.headers : []).map((parameter) =>
        this.normalizeDataOfferParameter(parameter),
      ),
      requestBody: operation.requestBody ? this.normalizeDataOfferRequestBody(operation.requestBody, offerSchemaIndex) : null,
      responses: (Array.isArray(operation.responses) ? operation.responses : []).map((response) =>
        this.normalizeDataOfferResponse(response, offerSchemaIndex),
      ),
    };
  }

  private normalizeDataOfferParameter(parameter: DataCatalogParameterRegistration) {
    return {
      name: typeof parameter.name === "string" ? parameter.name.trim() : "",
      in: typeof parameter.in === "string" ? parameter.in.trim() : "query",
      required: parameter.required === true,
      description: typeof parameter.description === "string" ? parameter.description.trim() || null : null,
      type: typeof parameter.type === "string" && parameter.type.trim() ? parameter.type.trim() : "string",
      format: typeof parameter.format === "string" ? parameter.format.trim() || null : null,
      nullable: parameter.nullable === true,
      example: parameter.example ?? null,
      enumValues: normalizeStringArray(parameter.enumValues),
      schema: parameter.schema ?? null,
    };
  }

  private normalizeDataOfferRequestBody(
    requestBody: DataCatalogRequestBodyRegistration,
    offerSchemaIndex: Map<string, ReturnType<UnsApiProxy["normalizeDataOfferSchema"]>>,
  ) {
    return {
      required: requestBody.required === true,
      description: typeof requestBody.description === "string" ? requestBody.description.trim() || null : null,
      contentType: typeof requestBody.contentType === "string" ? requestBody.contentType.trim() || null : null,
      schemas: this.resolveOfferSchemas(requestBody.schemas, requestBody.schemaIds, offerSchemaIndex),
    };
  }

  private normalizeDataOfferResponse(
    response: DataCatalogResponseRegistration,
    offerSchemaIndex: Map<string, ReturnType<UnsApiProxy["normalizeDataOfferSchema"]>>,
  ) {
    return {
      statusCode: typeof response.statusCode === "string" ? response.statusCode.trim() : "",
      description: typeof response.description === "string" ? response.description.trim() || null : null,
      contentType: typeof response.contentType === "string" ? response.contentType.trim() || null : null,
      schemas: this.resolveOfferSchemas(response.schemas, response.schemaIds, offerSchemaIndex),
      examplePayloads: Array.isArray(response.examplePayloads) ? response.examplePayloads : [],
      headers: (Array.isArray(response.headers) ? response.headers : []).map((parameter) =>
        this.normalizeDataOfferParameter(parameter),
      ),
    };
  }

  private normalizeDataOfferSchema(schema: DataCatalogSchemaRegistration) {
    return {
      id: typeof schema.id === "string" ? schema.id.trim() : "",
      title: typeof schema.title === "string" ? schema.title.trim() : "",
      kind: typeof schema.kind === "string" && schema.kind.trim() ? schema.kind.trim() : "schema",
      source: typeof schema.source === "string" && schema.source.trim() ? schema.source.trim() : "registered",
      contentType: typeof schema.contentType === "string" ? schema.contentType.trim() || null : null,
      rootType: typeof schema.rootType === "string" && schema.rootType.trim() ? schema.rootType.trim() : "object",
      nullable: schema.nullable === true,
      description: typeof schema.description === "string" ? schema.description.trim() || null : null,
      fields: (Array.isArray(schema.fields) ? schema.fields : []).map((field) =>
        this.normalizeDataOfferSchemaField(field),
      ),
      examplePayloads: Array.isArray(schema.examplePayloads) ? schema.examplePayloads : [],
    };
  }

  private normalizeDataOfferSchemaField(field: DataCatalogSchemaFieldRegistration) {
    const defaultName = typeof field.name === "string" ? field.name.trim() : "";
    const path = typeof field.path === "string" && field.path.trim() ? field.path.trim() : defaultName;
    return {
      path,
      name: defaultName || (path.split(".").at(-1) ?? path),
      type: typeof field.type === "string" && field.type.trim() ? field.type.trim() : "string",
      format: typeof field.format === "string" ? field.format.trim() || null : null,
      nullable: field.nullable === true,
      required: field.required === true,
      description: typeof field.description === "string" ? field.description.trim() || null : null,
      enumValues: normalizeStringArray(field.enumValues),
      example: field.example ?? null,
    };
  }

  private resolveOfferSchemas(
    inlineSchemas: DataCatalogSchemaRegistration[] | undefined,
    schemaIds: string[] | undefined,
    offerSchemaIndex: Map<string, ReturnType<UnsApiProxy["normalizeDataOfferSchema"]>>,
  ) {
    const result = (Array.isArray(inlineSchemas) ? inlineSchemas : []).map((schema) => this.normalizeDataOfferSchema(schema));
    for (const schemaId of Array.isArray(schemaIds) ? schemaIds : []) {
      const normalizedId = typeof schemaId === "string" ? schemaId.trim() : "";
      if (!normalizedId) {
        continue;
      }
      const resolved = offerSchemaIndex.get(normalizedId);
      if (resolved && !result.some((schema) => schema.id === resolved.id)) {
        result.push(resolved);
      }
    }
    return result;
  }
}
