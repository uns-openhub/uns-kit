import type { IApiProxyOptions } from "@uns-kit/core/uns/uns-interfaces.js";
import type MqttProxy from "@uns-kit/core/uns-mqtt/mqtt-proxy.js";
import UnsProxyProcess, { type UnsProxyProcessPlugin } from "@uns-kit/core/uns/uns-proxy-process.js";
import UnsApiProxy from "./uns-api-proxy.js";
import { UnsPacket } from "@uns-kit/core/uns/uns-packet.js";
import type { IUnsMessage } from "@uns-kit/core/uns/uns-interfaces.js";

const apiProxyRegistry = new WeakMap<UnsProxyProcess, UnsApiProxy[]>();

const getApiProxies = (instance: UnsProxyProcess): UnsApiProxy[] => {
  let proxies = apiProxyRegistry.get(instance);
  if (!proxies) {
    proxies = [];
    apiProxyRegistry.set(instance, proxies);
  }
  return proxies;
};

const unsApiPlugin: UnsProxyProcessPlugin = ({ define }) => {
  define({
    async createApiProxy(this: UnsProxyProcess, instanceName: string, options: IApiProxyOptions) {
      await this.waitForProcessConnection();

      const internals = this as unknown as {
        processName: string;
        processMqttProxy: MqttProxy;
        unsApiProxies: UnsApiProxy[];
      };

      const unsApiProxy = new UnsApiProxy(internals.processName, instanceName, options);
      let owned = !this.usesStatefulHandover() || this.isActive();
      const deferredMetadata = new Map<string, string>();
      const publishMetadata = (topic: string, payload: string): Promise<unknown> | undefined => {
        if (!owned) { deferredMetadata.set(topic, payload); return; }
        return internals.processMqttProxy.publish(topic, payload);
      };
      if (this.usesStatefulHandover()) unsApiProxy.setOwnershipGate(() => owned);
      this.registerLifecycleParticipant({
        onRelease: () => { owned = false; },
        drain: () => unsApiProxy.drainRequests(),
        onActivate: async () => {
          owned = true;
          for (const [topic, payload] of deferredMetadata) await publishMetadata(topic, payload);
          deferredMetadata.clear();
        },
      });

      unsApiProxy.event.on("unsProxyProducedTopics", (event) => {
        publishMetadata(
          event.statusTopic,
          JSON.stringify(event.producedTopics),
        );
      });

      unsApiProxy.event.on("unsProxyProducedApiEndpoints", (event) => {
        publishMetadata(
          event.statusTopic,
          JSON.stringify(event.producedApiEndpoints),
        );
      });

      (unsApiProxy.event as any).on("unsProxyProducedServiceEndpoints", (event: {
        statusTopic: string;
        producedServiceEndpoints: unknown[];
      }) => {
        publishMetadata(
          event.statusTopic,
          JSON.stringify(event.producedServiceEndpoints),
        );
      });

      (unsApiProxy.event as any).on("unsProxyProducedDataOfferEndpoints", (event: {
        statusTopic: string;
        producedDataOfferEndpoints: unknown[];
      }) => {
        publishMetadata(
          event.statusTopic,
          JSON.stringify(event.producedDataOfferEndpoints),
        );
      });

      unsApiProxy.event.on("unsProxyProducedApiCatchAll", (event) => {
        publishMetadata(
          event.statusTopic,
          JSON.stringify(event.producedCatchall),
        );
      });

      (unsApiProxy.event as any).on("unsProxyProducedDataCatalogOffers", (event: {
        statusTopic: string;
        producedDataCatalogOffers: unknown[];
      }) => {
        publishMetadata(
          event.statusTopic,
          JSON.stringify(event.producedDataCatalogOffers),
        );
      });

      unsApiProxy.event.on("mqttProxyStatus", (event) => {
        const time = UnsPacket.formatToISO8601(new Date());
        const unsMessage: IUnsMessage = { data: { time, value: event.value, uom: event.uom } };
        UnsPacket.unsPacketFromUnsMessage(unsMessage).then((packet) => {
          publishMetadata(
            event.statusTopic,
            JSON.stringify(packet),
          );
        });
      });

      internals.unsApiProxies.push(unsApiProxy);
      getApiProxies(this).push(unsApiProxy);
      return unsApiProxy;
    },
  });
};

UnsProxyProcess.use(unsApiPlugin);

export default unsApiPlugin;

export { UnsApiProxy };

export type UnsProxyProcessWithApi = UnsProxyProcess & {
  createApiProxy(instanceName: string, options: IApiProxyOptions): Promise<UnsApiProxy>;
};
