/**
 * Module dependencies.
 */
import express, { type Application, type NextFunction, type Response, type Router } from "express";
import * as http from "http";
import * as path from "path";
import cookieParser from "cookie-parser";
import { basePath } from "@uns-kit/core/base-path.js";
import logger from "@uns-kit/core/logger.js";
import os from 'os';
import { logRequestContext } from "./request-log.js";

type MountConfig = {
  apiBasePrefix?: string;
  swaggerBasePrefix?: string;
  disableDefaultApiMount?: boolean;
};

type AcceptedRequest = {
  transportEnded: boolean;
  readOnly: boolean;
  workSettled: boolean;
  complete: () => void;
};

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

export default class App {
  private expressApplication: Application;
  private ownershipGate: (() => boolean) | undefined;
  private readonly acceptedRequests = new Set<Promise<void>>();
  private readonly requestWork = new WeakMap<Response, AcceptedRequest>();
  private requestFailed = false;

  public setOwnershipGate(gate: () => boolean): void { this.ownershipGate = gate; }

  /**
   * Declare application-owned read-only work before its first asynchronous step.
   * HTTP method names are not proof of safety: only use this for work without
   * external effects. A disconnected reader still waits for actual work to settle.
   * This does not authenticate, cancel, retry or send the response.
   */
  public async runReadOnlyRequest<T>(res: Response, operation: () => T | Promise<T>): Promise<T> {
    const request = this.requestWork.get(res);
    if (!request && this.ownershipGate) throw new Error("HTTP request was not admitted by this runtime");
    if (request) {
      if (request.transportEnded || request.readOnly) throw new Error("HTTP work must be declared once before the response ends");
      request.readOnly = true;
      request.workSettled = false;
    }
    try {
      return await operation();
    } finally {
      if (request) {
        request.workSettled = true;
        if (request.transportEnded) request.complete();
      }
    }
  }
  public async drainRequests(): Promise<void> {
    while (this.acceptedRequests.size) await Promise.all([...this.acceptedRequests]);
    if (this.requestFailed) throw new Error("An accepted HTTP request ended without confirmation");
  }
  public async stop(): Promise<void> {
    await this.drainRequests();
    if (this.server.listening) await new Promise<void>((resolve, reject) => {
      this.server.close(error => error ? reject(error) : resolve());
      this.server.closeIdleConnections?.();
    });
  }
  public server;
  private port;
  public router: Router;
  private processName: string;
  private instanceName: string;
  private apiBasePrefix: string;
  private swaggerBasePrefix: string;
  public swaggerSpec: {
    openapi: string;
    info: { title: string; version: string };
    paths: Record<string, any>;
    servers?: Array<{ url: string }>;
  };
  private swaggerDocs: Map<string, Record<string, unknown>> = new Map();



  constructor(
    port: number,
    processName: string,
    instanceName: string,
    appContext?: any,
    mountConfig?: MountConfig
  ) {
    this.router = express.Router();
    this.port = port;
    this.expressApplication = express();
    this.server = http.createServer(this.expressApplication);
    this.expressApplication.use((_req, res, next) => {
      if (!this.ownershipGate) return next();
      if (!this.ownershipGate()) {
        res.setHeader("Retry-After", "1");
        res.status(503).json({ error: "RUNTIME_PASSIVE", message: "Runtime ownership is being transferred. Retry the request." });
        return;
      }
      let finish!: () => void;
      const accepted = new Promise<void>(resolve => { finish = resolve; });
      this.acceptedRequests.add(accepted);
      const request: AcceptedRequest = {
        transportEnded: false, readOnly: false, workSettled: true,
        complete: () => { this.acceptedRequests.delete(accepted); finish(); },
      };
      this.requestWork.set(res, request);
      const endTransport = () => {
        request.transportEnded = true;
        if (!res.writableFinished && !request.readOnly) this.requestFailed = true;
        if (request.workSettled) request.complete();
      };
      res.once("finish", endTransport);
      res.once("close", endTransport);
      next();
    });
    this.processName = processName;
    this.instanceName = instanceName;
    this.apiBasePrefix =
      normalizeBasePrefix(mountConfig?.apiBasePrefix ?? process.env.UNS_API_BASE_PATH) || "/api";
    const rawSwaggerBase =
      normalizeBasePrefix(mountConfig?.swaggerBasePrefix ?? process.env.UNS_SWAGGER_BASE_PATH) ||
      this.apiBasePrefix;
    // If someone passed ".../api" as swagger base, strip that to avoid duplicated segments
    this.swaggerBasePrefix = rawSwaggerBase.endsWith("/api")
      ? rawSwaggerBase.replace(/\/api\/?$/, "") || "/"
      : rawSwaggerBase;

    // Add context
    this.expressApplication.use((req: any, _res: Response, next: NextFunction) => {
      req.appContext = appContext;
      next();
    });

    // Body parser (req.body)
    this.expressApplication.use(express.json());
    this.expressApplication.use(express.urlencoded({ extended: false }));

    // Add cookie parser
    this.expressApplication.use(cookieParser());

    // Static / public folder
    const publicHome =
      process.env.PUBLIC_HOME === null || process.env.PUBLIC_HOME === undefined
        ? "public"
        : process.env.PUBLIC_HOME;
    this.expressApplication.use(
      express.static(path.join(basePath, publicHome))
    );

    // Map routes
    this.router.use(logRequestContext);
    if (!mountConfig?.disableDefaultApiMount) {
      this.expressApplication.use(this.apiBasePrefix, this.router);
    }

    // Swagger specs
    this.swaggerSpec = {
      openapi: "3.0.0",
      info: {
        title: "UNS API",
        version: "1.0.0",
      },
      paths: {},
      servers: this.swaggerBasePrefix ? [{ url: this.swaggerBasePrefix }] : undefined,
    };    
  }

  public static getExternalIPv4(): string | null {
    const interfaces = os.networkInterfaces();
    for (const name of Object.keys(interfaces)) {
      for (const iface of interfaces[name] || []) {
        if (iface.family === 'IPv4' && !iface.internal) {
          return iface.address;
        }
      }
    }
    return null;
  }


  public getSwaggerSpec() {
    return this.swaggerSpec;
  }

  public registerSwaggerDoc(path: string, doc: Record<string, unknown>): void {
    const normalizedPath = path.startsWith("/") ? path : `/${path}`;
    this.swaggerDocs.set(normalizedPath, doc);
    this.expressApplication.get(normalizedPath, (_req: any, res: any) => res.json(doc));
  }

  public async start() {
    // Listen on provided port, on all network interfaces.
    this.server.listen(this.port);
    this.server.on("error", (error: { syscall: string; code: any }) => {
      if (error.syscall !== "listen") {
        throw error;
      }

      const bind =
        typeof this.port === "string"
          ? `Pipe ${this.port}`
          : `Port ${this.port}`;

      // handle specific listen errors with friendly messages
      switch (error.code) {
        case "EACCES":
          logger.error(`${bind} requires elevated privileges`);
          process.exit(1);
          break;
        case "EADDRINUSE":
          logger.error(`${bind} is already in use`);
          process.exit(1);
          break;
        default:
          throw error;
      }
    });
    this.server.on("listening", () => {
      App.bind(this);
      const addressInfo = this.server.address();
      let ip: string | undefined;
      let port: number | string | undefined;
      if (addressInfo && typeof addressInfo === "object") {
        ip = App.getExternalIPv4();
        port = addressInfo.port;
      } else if (typeof addressInfo === "string") {
        ip = App.getExternalIPv4();
        port = "";
      }
      logger.info(
        `API listening on http://${ip}:${port}${this.apiBasePrefix}`
      );
      const swaggerPath = buildSwaggerPath(this.swaggerBasePrefix, this.processName, this.instanceName);
      logger.info(`Swagger openAPI on http://${ip}:${port}${swaggerPath}`);

      this.expressApplication.get(swaggerPath, (req: any, res: any) =>
        res.json(this.getSwaggerSpec())
      );
    });
  }

}
