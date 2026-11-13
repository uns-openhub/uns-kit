import { CatalogPublicationClient } from "@uns-kit/core/runtime/catalog-publication-client.js";
import { CatalogAccessError } from "@uns-kit/core/runtime/catalog-publication.js";
import type { RequestHandler } from "express";

/** Endpoint auth is checked by the Controller on every execution. This helper
 * adds no service-to-caller fallback and never exposes broker errors or raw data. */
export function catalogPublicationHandler<T>(
  client: CatalogPublicationClient,
  operationId: string,
  transform: (context: { signal: AbortSignal; input: Record<string, unknown>; history: (sourceId: string) => Promise<unknown> }) => Promise<T>,
): RequestHandler {
  return async (req, res) => {
    res.setHeader("Cache-Control", "no-store");
    try {
      if (req.method !== "GET") throw new CatalogAccessError("catalog_method_invalid", 405);
      const header = req.headers.authorization;
      if (!header?.startsWith("Bearer ")) throw new CatalogAccessError("catalog_authentication_required", 401);
      const input: Record<string, unknown> = {};
      for (const [key, value] of Object.entries(req.query)) {
        if (typeof value !== "string") throw new CatalogAccessError("catalog_request_invalid");
        input[key] = value;
      }
      const value = await client.execute(operationId, input, header.slice(7), (ctx) => transform({ ...ctx, input }));
      res.json(value);
    } catch (error) {
      const e = error instanceof CatalogAccessError ? error : new CatalogAccessError("catalog_execution_unavailable", 503);
      res.status(e.status).json({ error: e.code });
    }
  };
}
