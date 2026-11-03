# Service API request-body metadata

When a registered mutation overrides `requestBody` after
`buildServiceApiInteractions()`, the API publisher keeps the Service API body
descriptor aligned with the effective OpenAPI body. This applies to POST, PUT,
PATCH and DELETE for every service; no application-specific registration is needed.

```ts
await api.post(topic, asset, objectType, objectId, attribute, {
  ...interactions.control.options,
  requestBody: {
    required: true,
    description: "Preview the selected work without applying a change",
    schema: requestSchema,
  },
});
```

The caller supplies its own reviewed `requestSchema` and existing interaction
definition. The published Service API descriptor carries JSON content type,
required/description metadata and any existing catalog schemas/examples. The full
JSON Schema remains in OpenAPI; absent flattened catalog fields are not invented.
The shared definition is not mutated. Endpoints without Service API metadata do
not become registered service endpoints implicitly.

This fixes clients that show their existing JSON editor only when the body
descriptor is present. It neither grants command access nor changes application
authorization, payload validation, idempotency or HTTP handler behavior.

Local verification: three tests, including eight actual HTTP requests across all
four methods and two unrelated services; API package build passed. Base source
is SDK 3.0.41 (`f131c48d`), branch `fix/service-api-request-body-metadata`.
This correction is not yet published.
