# Explicit typed null data

Prepared for 3.0.41. Source preparation, npm publication and reader deployment
are separate steps.

An opt-in MQTT publisher can send an explicit clear without inventing a value:

    const output = await runtime.createUnsMqttProxy(host, "inspection", mode, true,
      { allowNullData: true, handoverStateContract: "inspection-clear-v1" });
    await output.publishMqttMessage({
      topic: "example/site/", asset: "line", objectType: "inspection", objectId: "main",
      attributes: [{ attribute: "item-id", valueType: "string",
        data: { time, value: null, valueType: "string" } }]
    });

A second unrelated sensor can explicitly clear a number with valueType number.
The SDK does not infer business meaning: the application owns when it sends a
clear. Missing value remains invalid. Arrays/objects are not scalar clears.
Metadata should retain the declared scalar type, and readers keep the clear's
own timestamp instead of falling back to an older non-null value.

A null may be persisted as a row with NULL value fields and its original type,
unit, timestamp and topic. No measurement zero or application sentinel is created.
The archiver/database and current-value reader must be verified independently.
All participating readers require this packet contract before producer activation.

`allowNullData` is bound into handover compatibility only when enabled. Ordinary
hashes stay identical to 3.0.40. Nullable/non-nullable peers cannot exchange state;
missing declared type, mismatched compatibility or unknown outcomes fail closed.
Use the same operation after a lost response and a fresh reverse move for return.
No disk queue transfer or universal exactly-once behavior is added by this feature.

The [managed forwarder example](examples/mqtt-explicit-null.mjs) shows complete
input/output wiring, scalar validation, bounded RAM output and standalone SDK
stop/drain. Copy it into an initialized Node project using the compatible SDK.
Its three example input frames demonstrate a value, its explicit clear and a new
value. A missing value or a null without its declared type rejects the input;
unknown failed work is retained for review rather than silently retried.

For a numeric clear use `{ value: null, valueType: "number" }` in an application
whose attribute is numeric. The declared type and application state contract must
remain stable. A null is never a counter sample or an instruction to compute a
delta. Storage selection and archive retention are separate configuration choices;
the packet itself grants no archival or deletion authority.
