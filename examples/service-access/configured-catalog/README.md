# Configured catalog source

This Node 22+ example publishes a bounded sample count. `package.json` declares
`/source/topic` as its catalog source binding and `config-example.json` supplies
the exact source. It has no direct `read:uns` permission. `history("temperature")`
uses the source ID; callers cannot supply a topic.

**Controller support is not yet available.** The SDK resolves a request; only a
supporting Controller can review and approve the resolved operation, its audience,
source and configuration revision. It must reject a stale loaded configuration.
Do not deploy until the Controller advertises this capability. Existing protocol-1
services are unchanged.

Copy `config-example.json` to `config.json`, set your own endpoints, and install
and build with `pnpm install && pnpm run build`. Never commit `config.json`, `.env`
or credentials. The Controller supplies the service token; a reader needs access
to the operation and does not receive direct access to the private source.
MQTT credentials and broker ACLs are separate.

The startup helper resolves the **raw** config, before secret resolution. Missing,
secret references, wildcards and non-exact source topics are rejected. `SIGINT`
and `SIGTERM` still shut down the runtime without Controller participation.
