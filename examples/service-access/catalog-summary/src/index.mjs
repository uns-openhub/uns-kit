import "@uns-kit/api";
import { registerApiCatalog } from "@uns-kit/api";
import { ConfigFile, UnsProxyProcess } from "@uns-kit/core";
import { CatalogPublicationClient } from "@uns-kit/core/runtime/catalog-publication-client.js";
import { parseServiceAccessDeclaration } from "@uns-kit/core/runtime/service-access.js";
import { readFile } from "node:fs/promises";

import { createSummaryOffer } from "./offer.mjs";

const required = (key) => {
  const value = process.env[key];
  if (!value) throw new Error(`Missing managed runtime value: ${key}`);
  return value;
};
const declaration = parseServiceAccessDeclaration(JSON.parse(await readFile(new URL("../service-access.json", import.meta.url), "utf8")));
const client = new CatalogPublicationClient({
  controllerBase: required("UNS_CONTROLLER_PUBLIC_BASE"),
  rttNode: required("RTT_NODE"),
  instanceId: required("RTT_INSTANCE_ID"),
  version: required("version"),
  manifest: declaration.publications,
});
const config = await ConfigFile.loadConfig();
if (!config.uns.jwksWellKnownUrl) throw new Error("Configure the Controller JWKS URL.");
const runtime = new UnsProxyProcess(config.infra.host, {
  processName: config.uns.processName,
  username: config.mqtt?.username,
  password: config.mqtt?.password,
});
let stopping;
const shutdown = () => (stopping ??= runtime.shutdown());
for (const signal of ["SIGINT", "SIGTERM"])
  process.once(signal, () => {
    void shutdown().then(
      () => process.exit(0),
      () => {
        process.exitCode = 1;
      },
    );
  });
try {
  await runtime.prepareHandover(config.uns.instanceMode, config.uns.handover);
  const api = await runtime.createApiProxy("catalog", { jwks: { wellKnownJwksUrl: config.uns.jwksWellKnownUrl } });
  api.registerCatalogRoute();
  await registerApiCatalog(api, { dataOfferSources: { sampleCount: createSummaryOffer(client) }, context: undefined });
} catch {
  console.error("Catalog startup failed; check configuration, Service access and runtime logs.");
  await shutdown();
  process.exitCode = 1;
}
