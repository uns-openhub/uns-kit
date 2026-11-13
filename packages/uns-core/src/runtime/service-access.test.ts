import { expect, it } from "vitest";

import { parseServiceAccessDeclaration, serviceAccessCeiling, serviceAccessDigest } from "./service-access.js";
import { serviceRuleWithin } from "./service-access.js";

const background = { protocol: 1, permissions: { scopes: ["read:uns"], accessRules: ["factory/sensors/#"] } };
it("supports an unrelated background service without catalog or HTTP", () => {
  const d = parseServiceAccessDeclaration(background);
  expect(d.publications).toBeUndefined();
  expect(serviceAccessCeiling(d)).toEqual(background.permissions);
});
it.each(["admin:all", "catalog:sources:read", "write:anything"])("rejects package-requested privileged or unknown scope %s", (scope) => {
  expect(() => parseServiceAccessDeclaration({ ...background, permissions: { ...background.permissions, scopes: [scope] } })).toThrow();
});
it.each([
  { protocol: 2 },
  { unknown: true },
  { permissions: { scopes: ["read:uns"], accessRules: [] } },
  { permissions: { scopes: [], accessRules: [] } },
  { permissions: { scopes: ["read:uns"], accessRules: ["factory/#/escape"] } },
])("fails closed on malformed declaration %j", (patch) => expect(() => parseServiceAccessDeclaration({ ...background, ...patch })).toThrow());
it("normalizes duplicate requirements for stable review digests", () =>
  expect(
    serviceAccessDigest(
      parseServiceAccessDeclaration({
        ...background,
        permissions: { scopes: ["read:uns", "read:uns"], accessRules: ["factory/sensors/#", "factory/sensors/#"] },
      }),
    ),
  ).toBe(serviceAccessDigest(parseServiceAccessDeclaration(background))));

it.each([
  ["plant/a", "plant/#", true],
  ["plant/+", "plant/#", true],
  ["plant/#", "plant/+", false],
  ["#", "plant/#", false],
  ["other/a", "plant/#", false],
])("validates resource narrowing %s within %s", (r, a, expected) => expect(serviceRuleWithin(r as string, a as string)).toBe(expected));
it("does not combine publication sources with raw background rules", () => {
  const d = parseServiceAccessDeclaration({
    ...background,
    publications: {
      protocol: 1,
      operations: [
        {
          id: "summary",
          title: "Summary",
          method: "GET",
          path: "/api/derived/summary",
          mode: "approved-service",
          requestSchema: {
            type: "object",
            additionalProperties: false,
            required: ["from", "to"],
            properties: { from: { type: "string", format: "date-time" }, to: { type: "string", format: "date-time" } },
          },
          responseSchema: { type: "integer" },
          sources: [{ id: "history", kind: "history", topic: "private/published" }],
        },
      ],
    },
  });
  expect(serviceAccessCeiling(d)).toEqual({ scopes: ["catalog:sources:read", "read:uns"], accessRules: ["factory/sensors/#"] });
});
