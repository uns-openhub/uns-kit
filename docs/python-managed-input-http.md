# Python managed MQTT input and HTTP request drain

Prepared in Python SDK **0.2.9**. This extends the 0.2.7 lifecycle and 0.2.8
authority/async IO slices. TypeScript comparison baseline is source 3.0.54.
Publication, application adoption and host deployment are separate steps.

## One local owner, prepared before input

```python
from uns_kit import UnsProxyProcess, UnsProcessParameters

runtime = UnsProxyProcess(infra_host, UnsProcessParameters(
    process_name="example-transformer",
    managed_ownership=True,
))
await runtime.start()
output = await runtime.create_uns_mqtt_proxy(output_host, "observations")
source = await runtime.create_uns_mqtt_proxy(input_host, "frames", uns_parameters={
    "managedInput": True,
    "mqttSubToTopics": ["example/inspection/frames"],
    "maxPendingInputs": 64,
    "maxInputBytes": 8 * 1024 * 1024,
    "maxInputMessageBytes": 1024 * 1024,
})

async def handle(message):
    # Application parsing, mappings, topics and units stay here.
    await output.publish_message("example/inspection/derived", message.payload)

source.set_input_handler(handle)
api = await runtime.create_api_proxy("api", options=api_options)
# Register API handlers and other application resources here.
runtime.set_active(True)
await source.start_input()  # subscribe ACK, then readiness; input QoS defaults to 1
```

`managed_ownership` is an explicit **local** admission gate. It suppresses the
legacy automatic activation delay and starts passive until the application calls
`set_active(True)`. Process-owned HTTP routes return 503 while passive. A managed
MQTT source cannot start without that local ownership. Standalone API proxies can
instead call `set_ownership_gate(predicate)` directly.

This does not implement election, a lease, hot handover, state transfer or any
Controller authorization. A local boolean never grants platform permissions.
Ownership predicates must be synchronous and return an actual boolean; an async
predicate is not an active owner.
Setting a managed owner inactive closes HTTP admission and starts releasing its
managed MQTT sources. Released MQTT sources are not restarted on reactivation;
await their drain and prepare a new source/owner deliberately. Never activate
a successor merely because the source's local active flag changed.

Without `managed_ownership`, normal standalone startup retains its existing
activation behavior; process Stop still closes new HTTP/managed MQTT admission.
Application signals remain application-owned. Await `runtime.stop()` from the
existing signal/finally path; Controller presence is not required.

## Managed MQTT input

Enable `managed_input=True` on a standalone `UnsMqttProxy`, or `managedInput: true`
in factory `UnsParameters`. `set_input_handler()` (alias `setInputHandler`) allows
one asynchronous handler. Register it before `start_input(topics, qos=1)`.
Factory `mqtt_sub_to_topics`/`mqttSubToTopics` supplies default topics. One source
cannot change its admitted filters or QoS after startup.

Only one handler executes at a time. Every effect it starts must be awaited.
Fire-and-forget tasks are not automatically owned by the SDK. Each received input
remains owned until the handler settles. The handler receives a copy; failure
keeps the original input for local review without running the handler again.
`client.messages()` and `resilient_messages()` reject competing manual readers
on a managed proxy. Separate proxies/clients can still serve unrelated sources.

Admission bounds count active plus queued input, not only queue depth. The default
limits are 64 inputs, 8 MiB admitted payload/topic bytes and 1 MiB per input.
These limits do not bound every allocation inside the broker or MQTT library.
Oversize/overflow raises a latched failure and prevents a clean stop. Excess
unadmitted data cannot all be retained in bounded RAM; reconcile with the source.
Overflow is never reported as successful processing or silently cleared.

`get_input_readiness()` means the current subscription was acknowledged and its
managed handler is available under the local gate. `get_input_status()` returns
bounded status/counters, without payloads or raw backend errors.
`get_failed_input()` returns a copy of the retained failed input for application
review. Do not publish/log private payloads as diagnostics.

`drain_input_for_stop()` first awaits unsubscribe acknowledgement (or an observed
closed transport), then seals admission and drains already received work. A
drain timeout or cancelled waiter does not cancel a handler or resubmit its
input. A retry awaits the same drain. Input failures remain blocked and retained.
`drain=False` does not bypass managed input drain.

Reconnect preserves already admitted RAM work and resubscribes the same source;
it does not replay an application handler. Broker QoS/session settings govern
unreceived or redelivered messages. A transport redelivery can still duplicate
a business event: application idempotency and durable checkpoints remain separate.
This is not a persistent input journal or an exactly-once guarantee.
The managed-input flow uses the async runtime. The sync facade rejects this
opt-in explicitly rather than exposing a managed source without handler APIs.

## Output admission and errors

Process-owned proxies now default to a 1000-item output queue, 32 bounded capacity
waiters and strict publish drain. Explicit existing nonpositive queue limits keep
legacy unbounded behavior; use a positive limit for bounded pipelines.
Standalone proxies can opt into `max_pending_publishes`, `publish_backpressure`
and `strict_publish_drain` explicitly. A managed-input proxy always uses strict
publish drain for its own output.

Capacity waiting happens before that publication enters the queue; it does not
repeat a publish. `publish_backpressure=32` is the Python form; factory mappings
also accept `publishBackpressure: {"maxWaiting": 32}`. Only a bounded number of
waiters is allowed. Avoid creating an unbounded application `gather()` backlog.

Strict drain latches asynchronous broker errors even if queue counters reach
zero. It cannot certify clean Stop after such a failure, including with
`drain=False`. A publication method returning still means local admission;
flush/broker completion and downstream database commits are separate evidence.
Output stops accepting new calls when output teardown begins, after managed
inputs and HTTP/application work have drained.

## HTTP ownership and accepted work

All SDK HTTP requests are tracked through the ASGI application, including
catch-all routes, authentication and streaming responses. The middleware changes
no JWT/JWKS/service authority rules. A passive/releasing runtime rejects new
requests with 503 and `Retry-After: 1`, before application handlers. Capacity is
bounded by `ApiProxyOptions.max_pending_requests` (mapping `maxPendingRequests`),
default 256.

`api.drain_requests(timeout=30.0)` waits for accepted application work. Close the
ownership gate first when using it independently; `api.stop()` and process Stop
close admission themselves. Response completion alone does not release an
application task that is still running. A disconnected/cancelled transport does
not cancel the accepted handler. No HTTP method is assumed read-only.

An ordinary request disconnected before its response finished, or an undeclared
handler failure, latches an unknown outcome. Drain waits for its work to settle
and then raises `HttpRequestDrainError`. A later drain does not clear that
uncertainty or replay the handler. Reconcile the same durable business operation
before any external recovery action; the SDK provides no automatic reset/receipt.

For work **without external effects**, declare ownership before its first async
step using the request/context supplied by the SDK:

```python
async def history_handler(event):
    result = await api.run_read_only_request(event, read_bounded_history)
    event.res.json(result)
```

The helper also accepts a Starlette request. Declare once, before the response
ends. Duplicate, foreign and late declarations fail before invoking the factory.
A failed or disconnected declared read still waits for its owned callback to
settle, then can drain safely. This is not permission to label a mutating GET or
POST as a read. Await all work started by the callback; an arbitrary detached
task cannot be inspected or cancelled safely by the SDK.

API servers use another thread/event loop; drain completion uses thread-safe
ownership tickets. Closing a server joins its thread asynchronously and requires
confirmed exit. Timeout preserves ownership for retry rather than reporting a
successful close. Draining from inside that same accepted handler is rejected
instead of waiting on itself.

## Process stop order and limits

1. Make local ownership inactive and close new HTTP/managed input admission.
2. Run synchronous application release hooks.
3. Await managed MQTT unsubscribe/input drain and HTTP request drain.
4. Run application drain/resource hooks.
5. Close APIs/cron, flush and close output, then close process telemetry/client.

Outputs and application resources remain available while accepted handlers finish.
Unknown input/HTTP/output results block later cleanup stages. Repeated Stop calls
share the same work and preserve failed state. Timeouts are in seconds and bound
the relevant drain; they are not a new global process deadline or protection
against SIGKILL. Controller cooperative-stop receipts and process-group protocols
are not declared or implemented by this slice.

## Verification

Tests use a temporary localhost-only Mosquitto broker, two unrelated input
namespaces, actual output observation, broker restart and admitted-work retention.
They also use actual SDK HTTP servers and disconnected GET/POST clients, plus
focused ASGI tests for early responses, bounded capacity, cancellation and late/
duplicate read declarations. They do not run industrial sources or change live
grants. The Python wheel is checked separately; installed Controller integration,
durable input recovery and production deployment remain separate acceptance.
