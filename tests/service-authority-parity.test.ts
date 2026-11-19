import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { OperatorApiAuthority } from "../packages/uns-api/src/operator-api-authority.js";

const cases = JSON.parse(readFileSync(new URL("./fixtures/service-authority-v1.json", import.meta.url), "utf8"));

describe("shared Python/TypeScript current service authority", () => {
  for (const fixture of cases) {
    it(fixture.name, async () => {
      let probes = 0;
      const authority = new OperatorApiAuthority({
        deployment: () => { throw Error("No Operator delegation in ordinary service APIs"); },
        ...(!fixture.noStatusUrl ? { statusUrl: "http://localhost/fixture-status" } : {}),
        fetch: (async () => {
          probes++;
          return new Response(JSON.stringify(fixture.body), { status: fixture.httpStatus ?? 200 });
        }) as typeof fetch,
      });
      let status = 200;
      try {
        await authority.authorize({ declarations: [], claims: fixture.claims, authorization: "Bearer fixture", payload: {} });
      } catch (error) {
        status = (error as { status: number }).status;
      }
      expect(status).toBe(fixture.expectedStatus);
      expect(probes).toBe(fixture.expectedProbes ?? 1);
    });
  }
});
