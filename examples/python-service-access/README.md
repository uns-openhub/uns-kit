# Python background Service access examples

These two independent protocol-1 readers use the same Python 0.2.8 helpers with
different package identities and source paths. They do not request catalog,
Operator, broker or database permissions. The package declarations request
authority; a compatible Controller and administrator review grant it separately.

Install the locally built wheel before publication, or a published 0.2.8+ SDK:

```sh
python -m pip install ../../packages/uns-py/dist/uns_kit-0.2.10-py3-none-any.whl
```

Run from this directory, using a private service token file supplied by your
approved local setup. Do not substitute human credentials or log the token:

```sh
export UNS_SERVICE_TOKEN_FILE=/path/to/private/local-service-token
python temperature-reader/main.py --controller-base http://localhost:3200 --from-time 2026-01-01T00:00:00Z --to-time 2026-01-01T01:00:00Z
python warehouse-reader/main.py --controller-base http://localhost:3200 --from-time 2026-01-01T00:00:00Z --to-time 2026-01-01T01:00:00Z
```

Each invocation loads its package declaration without importing application
code for discovery, requires exactly one declared source, reads the current
credential and makes a bounded history request. Missing/revoked credentials,
403 responses and malformed data fail rather than trying another identity.
These are finite reference readers, not complete installed process/service bundles.
No real endpoint or credential is included. The Python tests execute these same
entry functions against a synthetic loopback API; TS validates their manifests.

See [the service access guide](../../docs/python-service-access-async.md) for
async cancellation, database affinity, receive-side authorization and limits.
