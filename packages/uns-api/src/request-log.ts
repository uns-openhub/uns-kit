import type { NextFunction, Request, Response } from "express";
import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import logger from "@uns-kit/core/logger.js";

// Capability marker for applications that must reject the older unsafe logger.
export const SAFE_REQUEST_LOGGING_VERSION = 1;

// This is server-local metadata, not an HTTP header a caller can forge.
const requestIdSymbol = Symbol.for("uns.http.request-id");
const methods = new Set(["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]);

export const logRequestContext = (req: Request, res: Response, next: NextFunction): void => {
  const requestId = randomUUID();
  (req as unknown as Record<symbol, unknown>)[requestIdSymbol] = requestId;
  res.setHeader("x-request-id", requestId);
  const method = methods.has(req.method) ? req.method : "OTHER";
  const startedAt = performance.now();
  // URLs carry UNS topics/query parameters; decoded JWTs are not authenticated identities.
  // Downstream authenticated handlers can attach verified caller IDs to this request ID.
  logger.info({ timestamp: new Date().toISOString(), event: "http.received", requestId, method, message: "Request received" });
  let completed = false;
  const finish = (outcome: string): void => {
    if (completed) return;
    completed = true;
    logger.info({ timestamp: new Date().toISOString(), event: "http.transport.completed", requestId, method, status: res.statusCode, outcome, durationMs: Math.round(performance.now() - startedAt), message: "Request completed" });
  };
  res.once("finish", () => finish("completed"));
  res.once("close", () => finish(res.writableFinished ? "completed" : "disconnected"));
  next();
};
