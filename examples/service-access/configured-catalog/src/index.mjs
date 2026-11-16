import "@uns-kit/api";
import { registerApiCatalog } from "@uns-kit/api";
import { ConfigFile, UnsProxyProcess } from "@uns-kit/core";
import { CatalogPublicationClient } from "@uns-kit/core/runtime/catalog-publication-client.js";
import { loadConfiguredServiceAccessDeclaration } from "@uns-kit/core/runtime/service-access-package.js";
import { resolveConfiguredCatalogPublicationManifest } from "@uns-kit/core/runtime/configured-service-access.js";

import { createSummaryOffer } from "./offer.mjs";

const required = (key) => {
  const value = process.env[key];
  if (!value) throw new Error(`Missing managed runtime value: ${key}`);
  return value;
};
const declaration = await loadConfiguredServiceAccessDeclaration(new URL("../package.json", import.meta.url));
if (!declaration?.publications) throw new Error("Package must declare its catalog operations.");
// Bind the literal loaded configuration, before resolving secrets: a secret
// reference must never become an implicitly approved topic.
const manifest = resolveConfiguredCatalogPublicationManifest(declaration.publications, ConfigFile.loadRawConfig());
const client = new CatalogPublicationClient({
  controllerBase: required("UNS_CONTROLLER_PUBLIC_BASE"),
  rttNode: required("RTT_NODE"),
  instanceId: required("RTT_INSTANCE_ID"),
  version: required("version"),
  manifest,
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
