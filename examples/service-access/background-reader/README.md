# background-reader

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

This worker has no API or catalog. It polls five minutes of history, at most
100 rows, every 30 seconds while active. Provision the exact declared source in
your own test environment; zero rows is a valid result. Its token can read that
source, while another source must return 403. SIGINT/SIGTERM aborts the current
request and stops polling before runtime cleanup.
