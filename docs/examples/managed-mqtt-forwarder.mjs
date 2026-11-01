// @uns-kit/core >=3.0.40. Run inside a Node project with its own package.json.
// RAM queues survive a blocked destination while this process remains alive;
// they do not survive SIGKILL or power loss.
import { UnsProxyProcess } from '@uns-kit/core';

const runtime = new UnsProxyProcess(process.env.MQTT_INFRA_HOST ?? 'localhost', {
  processName: 'managed-forwarder',
  handoverProtocol: 'correlated-v2',
  deferHandoverUntilReady: true,
});

const output = await runtime.createUnsMqttProxy(
  process.env.MQTT_OUTPUT_HOST ?? 'localhost', 'observations', 'handover', true, {
    port: Number(process.env.MQTT_OUTPUT_PORT ?? 1883),
    handoverStateContract: 'example-raw-observations-v1',
    defaultPublishOptions: { qos: 1 },
    maxPendingPublishes: 128,
    publishBackpressure: { maxWaiting: 4 },
  },
);
const input = await runtime.createUnsMqttProxy(
  process.env.MQTT_INPUT_HOST ?? 'localhost', 'frames', 'handover', true, {
    port: Number(process.env.MQTT_INPUT_PORT ?? 1883),
    handoverStateContract: 'example-raw-frames-v1',
    mqttSubToTopics: ['example/frames'],
    managedInput: true,
  },
);

input.setInputHandler(async event => {
  // Await every output. The SDK retains this input until the callback finishes.
  // It never automatically repeats a callback with unknown external effects.
  await output.publishMessage('example/observations', event.message, { qos: 1 });
});
runtime.enableMigrationDrain();
runtime.installSignalHandlers({ retainOnFailure: true });
