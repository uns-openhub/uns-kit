import { describe, expect, it } from "vitest";

import {
  OPERATOR_DELEGATION_CLAIM,
  assertOperatorDelegation,
  assertOperatorPayload,
  operatorDigest,
  operatorJson,
  operatorOperation,
  operatorOperationFromWire,
  operatorRequestSchema,
} from "../packages/uns-core/src/runtime/operator-api.js";

const requestId = "10000000-0000-4000-8000-000000000001";
const deployment = { controllerName: "worker-b", rttNode: "fixture-runtime", version: "1.2.3", instanceId: "instance-b", launchId: requestId };
const schema = {
  type: "object",
  additionalProperties: false,
  required: ["action", "requestId", "expectedRevision"],
  properties: {
    action: { const: "hold", type: "string" },
    requestId: { type: "string", format: "uuid" },
    expectedRevision: { type: "integer", minimum: 0 },
  },
};
const definition = {
  protocol: 1,
  id: "maintenance.hold",
  title: "Hold a target",
  kind: "command",
  writeDomain: "maintenance",
  replay: "request-id-v1",
  requestSchema: schema,
};
const operation = () => operatorOperation(definition, "POST", "/api/factory/fixture/control");
const payload = () => ({ action: "hold", requestId, expectedRevision: 7 });
function claims() {
  return {
    sub: requestId,
    machineIdentityId: requestId,
    identityType: "machine",
    workloadIdentityType: "CONTROLLER_MCP",
    serviceToken: true,
    tokenVersion: 2,
    role: "operator",
    iat: 100,
    exp: 130,
    accessRules: [operation().path],
    [OPERATOR_DELEGATION_CLAIM]: {
      protocol: 1,
      operationId: operation().id,
      operationDigest: operatorDigest(operation()),
      requestDigest: operatorDigest(payload()),
      target: deployment,
      tool: "controller_execute_runtime_operator_command",
    },
  };
}

describe("shared registered operator API protocol", () => {
  it("uses the same declaration contract for unrelated application names and paths", () => {
    for (const [id, path] of [
      ["targets.pause", "/api/one/target-control"],
      ["maintenance.hold", "/api/another/maintenance"],
    ]) {
      const op = operatorOperation({ ...definition, id }, "POST", path);
      assertOperatorPayload(op, payload());
      expect(operatorOperationFromWire(JSON.parse(JSON.stringify(op)))).toEqual(op);
    }
  });
  it("canonicalizes field order and binds exact values without lossy JSON", () => {
    expect(operatorDigest({ b: 2, a: [1] })).toBe(operatorDigest({ a: [1], b: 2 }));
    expect(operatorDigest({ value: 2 })).not.toBe(operatorDigest({ value: 1 }));
    for (const value of [undefined, NaN, Infinity, { a: undefined }, new Date(), 1n]) expect(() => operatorJson(value)).toThrow();
  });
  it("rejects unbounded JSON and unsafe object keys", () => {
    expect(() => operatorJson("x".repeat(65536))).toThrow();
    expect(() => operatorJson(Array.from({ length: 1025 }, () => 1))).toThrow();
    expect(() => operatorJson(JSON.parse('{"__proto__":{}}'))).toThrow();
    let input: unknown = {};
    for (let i = 0; i < 25; i++) input = { nested: input };
    expect(() => operatorJson(input)).toThrow();
  });
  it.each(["http://elsewhere/api", "//elsewhere/api", "/api/../keys", "/api/%2e%2e/keys", "/api/a?x=1", "/api/a#x", "/api/./x"])(
    "rejects non-exact API path %s",
    (path) => {
      expect(() => operatorOperation(definition, "POST", path)).toThrow();
    },
  );
  it("commands explicitly require POST and application receipt support", () => {
    expect(() => operatorOperation(definition, "GET", "/api/one")).toThrow();
    expect(() => operatorOperation({ ...definition, replay: undefined }, "POST", "/api/one")).toThrow();
    expect(() => operatorOperation({ ...definition, kind: "read" }, "POST", "/api/one")).toThrow();
  });
  it("validates exact body schema and request ID before dispatch", () => {
    assertOperatorPayload(operation(), payload());
    for (const invalid of [
      { ...payload(), action: "resume" },
      { ...payload(), requestId: "not-an-id" },
      { ...payload(), expectedRevision: -1 },
      { ...payload(), unexpected: true },
    ]) {
      expect(() => assertOperatorPayload(operation(), invalid)).toThrow();
    }
  });
  it("accepts generated UUID constraints without executable regex", () => {
    expect(operatorRequestSchema({ type: "string", format: "uuid", pattern: "a generated uuid regex" })).not.toHaveProperty("pattern");
    expect(() => operatorRequestSchema({ type: "string", pattern: "(a+)+$" })).toThrow();
  });
  it.each([
    { $ref: "https://elsewhere/schema" },
    { type: "object", additionalProperties: true },
    { type: "object", properties: { x: {} } },
    { type: "string", format: "not-supported" },
  ])("rejects unsupported/open schemas", (schema) => {
    expect(() => operatorRequestSchema(schema)).toThrow();
  });
  it("accepts only a short-lived, exact launch/request/operation delegation", () => {
    expect(assertOperatorDelegation(claims(), operation(), payload(), deployment, 110).tool).toBe("controller_execute_runtime_operator_command");
    expect(() => assertOperatorDelegation(claims(), operation(), { ...payload(), expectedRevision: 8 }, deployment, 110)).toThrow();
    expect(() =>
      assertOperatorDelegation(claims(), operation(), payload(), { ...deployment, launchId: "20000000-0000-4000-8000-000000000002" }, 110),
    ).toThrow();
    expect(() => assertOperatorDelegation(claims(), { ...operation(), path: "/api/another" }, payload(), deployment, 110)).toThrow();
  });
  it.each([
    { role: "user" },
    { accessRules: ["#"] },
    { accessRules: [operation().path, "/elsewhere"] },
    { exp: 131 },
    { exp: 110 },
    { iat: 120 },
    { serviceToken: false },
    { tokenVersion: 0 },
  ])("rejects invalid delegated authority", (change) => {
    expect(() => assertOperatorDelegation({ ...claims(), ...change }, operation(), payload(), deployment, 110)).toThrow();
  });
});
