import { EventEmitter } from "node:events";
import type { Request, Response } from "express";
import { describe, expect, it, vi } from "vitest";
import logger from "../packages/uns-core/dist/logger.js";
const log = vi.spyOn(logger, "info").mockImplementation(() => logger);
import { logRequestContext } from "../packages/uns-api/src/request-log.js";

describe("request log privacy", () => {
  it("uses a server ID, omits URL/token/email, and records completion once", () => {
    log.mockClear();
    const req = { method: "GET", originalUrl: "/private/topic?secret=value", headers: { authorization: "Bearer private-token", "x-request-id": "forged-id" } } as unknown as Request;
    const res = Object.assign(new EventEmitter(), { statusCode: 200, writableFinished: true, setHeader: vi.fn() }) as unknown as Response;
    const next = vi.fn();
    logRequestContext(req, res, next);
    const requestId = (req as unknown as Record<symbol, string>)[Symbol.for("uns.http.request-id")];
    expect(requestId).toMatch(/^[0-9a-f-]{36}$/);
    expect(res.setHeader).toHaveBeenCalledWith("x-request-id", requestId);
    res.emit("finish"); res.emit("close");
    expect(next).toHaveBeenCalledOnce();
    expect(log).toHaveBeenCalledTimes(2);
    expect(log.mock.calls[1]?.[0]).toMatchObject({ requestId, outcome: "completed", status: 200 });
    expect(JSON.stringify(log.mock.calls)).not.toMatch(/private|secret|value|forged/);
  });
  it("labels premature disconnects without inventing a successful response", () => {
    log.mockClear();
    const req = { method: "CUSTOM-secret", headers: {} } as unknown as Request;
    const res = Object.assign(new EventEmitter(), { statusCode: 200, writableFinished: false, setHeader: vi.fn() }) as unknown as Response;
    logRequestContext(req, res, vi.fn()); res.emit("close");
    expect(log.mock.calls[1]?.[0]).toMatchObject({ method: "OTHER", outcome: "disconnected" });
  });
});
