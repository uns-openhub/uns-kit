import { afterEach, describe, expect, it } from "vitest";
import UnsApiProxy from "../packages/uns-api/src/uns-api-proxy.js";
import { buildServiceApiInteractions } from "../packages/uns-api/src/api-interfaces.js";
import { withServiceApiRequestBody } from "../packages/uns-api/src/service-api-request-body.js";

const schema = { type: "object", additionalProperties: false, properties: { action: { type: "string", const: "preview" } }, required: ["action"] };
const proxies: any[] = [];
afterEach(async () => { for (const proxy of proxies.splice(0)) { await proxy.stop(); if (proxy.app.server.listening) await new Promise<void>(resolve => proxy.app.server.close(resolve)); } });
describe("Service APIs body descriptor matches registered mutation body", () => {
  it("repairs missing metadata without mutating shared definitions or inventing service endpoints", () => {
    const definition = { id: "control", requestBody: null, schemas: [] };
    const result = withServiceApiRequestBody(definition, { schema, description: "JSON command" });
    expect(result?.requestBody).toEqual({ required: true, description: "JSON command", contentType: "application/json", schemas: [] });
    expect(definition.requestBody).toBeNull();
    expect(withServiceApiRequestBody(undefined, { schema })).toBeNull();
    expect(withServiceApiRequestBody(definition, undefined)).toBe(definition);
  });
  it("preserves typed catalog schemas/examples and synchronizes effective required/description", () => {
    const schemas = [{ id: "request", title: "Request", fields: [], examplePayloads: [{ action: "preview" }] }];
    const previous = { requestBody: { required: true, contentType: "wrong/type", description: "old", schemas } };
    const body: any = withServiceApiRequestBody(previous, { required: false, description: "effective", schema })?.requestBody;
    expect(body).toEqual({ required: false, contentType: "application/json", description: "effective", schemas });
    expect(previous.requestBody.contentType).toBe("wrong/type");
  });
  it("publishes a body descriptor for every shared mutation method and forwards exact JSON for unrelated services", async () => {
    for (const service of ["any-worker", "different-worker"]) {
      const proxy: any = new UnsApiProxy(service, "bodyMetadata", {}); proxies.push(proxy);
      while (!proxy.app.server.listening) await new Promise(resolve => setTimeout(resolve, 5));
      const definition = buildServiceApiInteractions(service, { control: { topic: "example", asset: "worker", objectType: "service", objectId: "main", attribute: "control", method: "POST", description: "Preview input", handler: () => {} } });
      for (const method of ["POST", "PUT", "PATCH", "DELETE"] as const) {
        const attr = method.toLowerCase(); const options: any = { ...definition.control.options, requestBody: { required: true, description: "Exact JSON preview", schema } };
        await proxy[attr]("example", "worker", "service", "main", attr, options);
        proxy.event.on(`api${attr[0].toUpperCase()+attr.slice(1)}Event`, ({ req, res }: any) => res.json(req.body));
        const endpoint = proxy.producedServiceEndpoints.get(`example/worker/service/main/${attr}`);
        expect(endpoint.serviceApi.requestBody).toMatchObject({ required: true, contentType: "application/json", schemas: [] });
        expect(proxy.app.swaggerSpec.paths[`/api/example/worker/service/main/${attr}`][attr].requestBody.content["application/json"].schema).toEqual(schema);
        const input = { action: "preview", selector: { tables: ["example_data"] } };
        const response = await fetch(`http://127.0.0.1:${proxy.app.server.address().port}/api/example/worker/service/main/${attr}`, { method, headers: { "content-type": "application/json" }, body: JSON.stringify(input) });
        expect(response.status).toBe(200); expect(await response.json()).toEqual(input);
      }
    }
  });
});
