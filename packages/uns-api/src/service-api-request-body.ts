import type { IPostEndpointOptions } from "@uns-kit/core/uns/uns-interfaces.js";

/** Service APIs and OpenAPI must agree when a caller overrides requestBody on
 * api.post/put/patch/delete after buildServiceApiInteractions(). A body editor
 * needs the body descriptor even when no flattened catalog schema was supplied.
 * The full JSON Schema stays authoritative in the existing OpenAPI document. */
export function withServiceApiRequestBody(
  serviceApi: Record<string, unknown> | null | undefined,
  requestBody: IPostEndpointOptions["requestBody"],
): Record<string, unknown> | null {
  if (!serviceApi || !requestBody) return serviceApi ?? null;
  const raw = serviceApi["requestBody"];
  const previous = raw && typeof raw === "object" && !Array.isArray(raw) ? raw as Record<string, unknown> : {};
  return {
    ...serviceApi,
    requestBody: {
      ...previous,
      required: requestBody.required ?? true,
      description: requestBody.description ?? previous["description"] ?? null,
      contentType: "application/json",
      schemas: Array.isArray(previous["schemas"]) ? previous["schemas"] : [],
    },
  };
}
