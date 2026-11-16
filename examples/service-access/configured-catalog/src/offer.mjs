import { defineDataCatalogField, defineDataCatalogOfferSource, defineDataCatalogQueryParam, defineDataCatalogSchema } from "@uns-kit/api";
import { catalogPublicationHandler } from "@uns-kit/api/catalog-publication-handler.js";

export function createSummaryOffer(client) {
  return defineDataCatalogOfferSource({
    offerId: "sample-count",
    displayName: "Temperature sample count",
    description: "Count of samples in an approved interval; raw measurements are not returned.",
    topic: "example/public/line-1",
    asset: "equipment",
    objectType: "dataset",
    objectId: "sample-count",
    attribute: "json",
    method: "GET",
    queryParams: ["from", "to"].map((name) =>
      defineDataCatalogQueryParam(name, "ISO 8601 with timezone; at most one hour.", { required: true, format: "date-time" }),
    ),
    response: { statusCode: "200", contentType: "application/json" },
    schema: defineDataCatalogSchema({
      id: "sample-count",
      title: "Sample count",
      contentType: "application/json",
      fields: [
        defineDataCatalogField("count", "integer", "Number of returned samples"),
        defineDataCatalogField("hasMore", "boolean", "More source samples exist than the bounded result contains"),
      ],
    }),
    handler: ({ req, res }) =>
      catalogPublicationHandler(client, "sample-count", async ({ history }) => {
        const rows = await history("temperature");
        if (!Array.isArray(rows.data) || typeof rows.truncated !== "boolean") throw new Error("Unexpected source response.");
        return { count: rows.data.length, hasMore: rows.truncated };
      })(req, res),
  });
}
