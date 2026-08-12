# UNS Python App Template

## Setup
```bash
poetry install
poetry run python src/main.py
```

## Recommended publishing pattern
For UNS topic outputs, the default and recommended application pattern is:
- `UnsProxyProcess`
- `await process.create_mqtt_proxy(...)`
- `await output.publish_mqtt_message(...)`
- `await output.flush()` before shutdown when you need to wait for broker publish completion

Avoid ad-hoc direct MQTT publishing in normal service code unless there is a special reason. The scaffolded app is intended to be extended from that proxy-based pattern so published UNS topics are also reflected in the retained `.../topics` registry.

`publish_mqtt_message()` resolves when the message is accepted into the local publish queue. Queue-full is reported immediately to the caller. Broker publish failures are emitted later on the proxy `error` event, so call `await output.flush()` before shutdown or before assuming accepted messages have finished publishing.

## Validity / Liveliness

UNS attributes can declare how the controller decides whether they are live or stale; in most apps this is primarily used to drive UI liveliness/activity indicators. In app-level modeling we use two modes only:

- `interval`: continuously refreshed values (stale after ~2× `expectedIntervalMs`)
- `lifecycle`: event-driven activity that stays active until a defined end value (`lifecycleEndValue`)

Example:

```python
await output.publish_mqtt_message({
    "topic": "raw/data/",
    "asset": "line-1",
    "objectType": "motor",
    "objectId": "main",
    "attributes": {
        "attribute": "status",
        "data": {"time": "2025-01-01T00:00:00Z", "value": "RUNNING"},
        "validityMode": "lifecycle",
        "lifecycleEndValue": "STOPPED",
    },
})
```

## Configure (Optional)
```bash
poetry run uns-kit-py configure-vscode .
poetry run uns-kit-py configure-devops .
poetry run uns-kit-py configure-api .
poetry run uns-kit-py configure-cron .
```

`configure-api` copies a service-API scaffold around `src/main.py` and `src/api_routes.py`, and updates `pyproject.toml` to install `uns-kit` with the `api` extra.
`configure-cron` copies an APScheduler starter and updates `pyproject.toml` to install `uns-kit` with the `cron` extra.
The generated feature examples use `UnsProxyProcess.create_api_proxy(...)` and `create_cron_proxy(...)` so the runtime shape matches the TypeScript packages.
Use `configure-data-offer` when you want a TypeScript-style service/data-offer layout around `src/main.py`, `src/api_routes.py`, `src/data_offers/*.py`, and `src/data_offers/sql/`. The scaffold keeps `register_api_catalog(...)` in `src/main.py`, uses `src/api_routes.py` as the aggregator, and includes both a JSON offer and a Parquet export example inside `src/data_offers/`.

## Publish/Subscribe Helpers
```bash
poetry run uns-kit-py publish --host localhost:1883 --topic raw/data/ --value 1
poetry run uns-kit-py subscribe --host localhost:1883 --topic uns-infra/#
```
These CLI helpers are useful for diagnostics and low-level checks. For application UNS topic outputs, prefer the proxy-based pattern in `src/main.py`.

## Datahub client (last value)

`UnsClient` provides a minimal REST client for the UNS OpenHub API, including the batch last-value endpoint. For service-to-service access, prefer the controller-managed token file or a development machine token. Use `AuthClient` only when you need user login/refresh behavior.

```python
from pathlib import Path
from uns_kit.core import ConfigFile, ServiceTokenProvider, UnsClient

cfg = ConfigFile.load_config(Path("config.json"))
token_provider = ServiceTokenProvider(config_token=cfg["uns"].get("token"))
client = UnsClient(cfg["uns"]["rest"], api_base_path="/api", token_provider=token_provider)

values = client.last_value([
    "raw/data/line-1/motor/main/temperature",
    "raw/data/line-1/motor/main/status",
])
print(values)
```

## Status topics
The default `src/main.py` starts an `UnsProxyProcess`, creates an output proxy, and publishes a sample UNS topic through that proxy.

## Configuration

The generated project contains three committed, credential-free runtime
profiles. Copy the appropriate profile to untracked `config.json` before
starting the process:

| Profile | Use it when | MQTT | Service credential |
|---|---|---|---|
| `config-development-host.json` | Running directly on the host | `localhost` | `UNS_SERVICE_TOKEN` in the process environment |
| `config-development-podman.json` | Deploying through a local Podman controller | `mosquitto` | Controller-managed `UNS_SERVICE_TOKEN_FILE` |
| `config-production.json` | Creating a production controller instance | `mosquitto` | Controller-managed token file or approved secret provider |

Do not put a service token, user email, password, MQTT password, or customer
endpoint into a committed profile. A controller-managed token file is read on
every request by `ServiceTokenProvider`, so rotations do not require a restart.

## Extend the config schema
Edit `src/config/project_config_extension.py` and run:

```bash
poetry run uns-kit-py generate-config-schema
```

This regenerates `config.schema.json` so editors can show completions, required-field errors, and validation in `config.json`.
