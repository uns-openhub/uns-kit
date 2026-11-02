// @uns-kit/core >=3.0.41. Copy into a Node project with its own package.json.
// Update packet readers before enabling this producer. Input frames are JSON:
// {"value":"ITEM-001"}, {"value":null,"valueType":"string"}, {"value":"ITEM-002"}.
// The application chooses when a clear is meaningful. The SDK validates the
// shared scalar contract; it never turns a missing value into a clear.
import { UnsPacket, UnsProxyProcess } from '@uns-kit/core';

const runtime = new UnsProxyProcess(process.env.MQTT_INFRA_HOST ?? '127.0.0.1', {
  processName: 'explicit-clear-example',
  handoverProtocol: 'correlated-v2',
  deferHandoverUntilReady: true,
});
const output = await runtime.createUnsMqttProxy(
  process.env.MQTT_OUTPUT_HOST ?? '127.0.0.1', 'observations', 'handover', true, {
    port: Number(process.env.MQTT_OUTPUT_PORT ?? 1883),
    handoverStateContract: 'example-explicit-item-id-v1',
    allowNullData: true,
    defaultPublishOptions: { qos: 1 },
    maxPendingPublishes: 128,
    publishBackpressure: { maxWaiting: 4 },
  },
);
const input = await runtime.createUnsMqttProxy(
  process.env.MQTT_INPUT_HOST ?? '127.0.0.1', 'frames', 'handover', true, {
    port: Number(process.env.MQTT_INPUT_PORT ?? 1883),
    handoverStateContract: 'example-explicit-id-json-input-v1',
    mqttSubToTopics: ['example/item-id-frames'],
    managedInput: true,
  },
);
input.setInputHandler(async event => {
  const frame = JSON.parse(event.message);
  const packet = await UnsPacket.unsPacketFromUnsMessage({ data: {
    time: new Date().toISOString(), value: frame.value,
    ...(frame.valueType === undefined ? {} : { valueType: frame.valueType }),
  } });
  await output.publishMqttMessage({
    topic: 'example/site/', asset: 'line', objectType: 'inspection', objectId: 'main',
    attributes: [{ attribute: 'item-id', data: packet.message.data }],
  });
});
runtime.enableMigrationDrain();
runtime.installSignalHandlers({ retainOnFailure: true });
