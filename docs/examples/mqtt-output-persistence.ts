// Source3.0.30 example: two unrelated standalone output proxies.
// Provision separate persistent/private directories before production use.
import UnsMqttProxy from "@uns-kit/core/uns-mqtt/uns-mqtt-proxy.js";

export async function meterOutput(directory: string) {
  const mqtt = new UnsMqttProxy("127.0.0.1", "meter-publisher", "measurements", {
    publishQueue: { mode: "durable", directory },
  }, true, false);
  await mqtt.publishMessage("example/plant/meter/energy", JSON.stringify({ time: new Date().toISOString(), value: 42, uom: "kWh" }));
  await mqtt.flush(30000);
  await mqtt.stop();
}

export async function inspectionOutput(directory: string) {
  const mqtt = new UnsMqttProxy("127.0.0.1", "vision-publisher", "inspections", {
    publishQueue: { mode: "buffered", directory },
  }, true, false);
  await mqtt.publishMessage("example/plant/camera/result", JSON.stringify({ time: new Date().toISOString(), result: "pass" }));
  await mqtt.flush(30000);
  await mqtt.stop();
}
