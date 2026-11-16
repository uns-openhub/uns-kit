# Configured background reader

This Node 22+ example declares an exact JSON Pointer `/source/topic` in
`package.json.unsDatahub.serviceAccess`. `config-example.json` supplies the
concrete source. The worker and the review helper read that same field. The
example declaration limits the requested topic to `example/configured/#`.

**Controller support is not implemented yet.** The SDK only derives a proposal;
it does not create an identity or grant access. Do not deploy this protocol-2
example to Controller 2.2.98 or assume a future version supports it without
explicit capability negotiation and administrator review. The indicated
controller compatibility is a minimum exclusion, not a capability guarantee.

After a supporting Controller is available, set your own allowed source in
`config.json`, let the administrator review the requested scope/path, and start
only after the effective Service access matches the loaded configuration. The
Controller manages the token; `ServiceTokenProvider` reads it from its supplied
location. MQTT authentication and broker ACLs remain separate. Do not commit
`config.json` or `.env`.

Install and build with `pnpm install && pnpm run build`. The worker reads up to
100 history records every 30 seconds; SIGINT/SIGTERM aborts the request and
shuts the runtime down. Tests use synthetic loopback services, not production.
