# catalog-summary

A complete Node.js 22+ example using published SDK APIs from 3.0.47 onward.
Read the [Service access walkthrough](https://github.com/uns-openhub/uns-kit/blob/master/docs/catalog-publication-access.md) for the declaration, Controller review and acceptance steps.

```sh
pnpm install
pnpm run build
cp config-example.json config.json
```

The example configuration targets a local development broker/Controller. Select
your own endpoints and secret references before deployment. The Controller owns
instance configuration when deployed. `MQTT_USERNAME`/`MQTT_PASSWORD` concern broker
ACLs only and grant no OpenHub API rights. Do not commit `config.json` or `.env`.

Deploy the package, apply its exact-version Service access policy, and then Start.
The Controller supplies managed identity/credential paths. Do not manufacture RTT
launch variables or paste a human token into configuration.

This service requires a managed Controller launch for signed catalog route
publication. Its workload has no direct UNS access. Apply catalog:sources:read
and approve the publication for the intended audience. Give a test reader access
to the operation path, not the private source path. Execute a timezone-qualified
window of at most one hour. Expect {count, hasMore}; raw source fields are excluded.
A direct source request with the reader or service token must return 403.
