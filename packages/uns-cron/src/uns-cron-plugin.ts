import UnsProxyProcess, { type UnsProxyProcessPlugin } from "@uns-kit/core/uns/uns-proxy-process.js";
import UnsCronProxy, { type CronProxyOptions, type CronScheduleInput } from "./uns-cron-proxy.js";

const cronProxyRegistry = new WeakMap<UnsProxyProcess, UnsCronProxy[]>();

const getCronProxies = (instance: UnsProxyProcess): UnsCronProxy[] => {
  let proxies = cronProxyRegistry.get(instance);
  if (!proxies) {
    proxies = [];
    cronProxyRegistry.set(instance, proxies);
  }
  return proxies;
};

const unsCronPlugin: UnsProxyProcessPlugin = ({ define }) => {
  define({
    async createCrontabProxy(this: UnsProxyProcess, cronInput: CronScheduleInput, options?: CronProxyOptions) {
      await this.waitForProcessConnection();
      const unsCronProxy = new UnsCronProxy(cronInput, options,
        () => !this.usesStatefulHandover() || this.isActive());
      this.registerLifecycleParticipant({ onRelease: () => { void unsCronProxy.stop(); }, drain: () => unsCronProxy.stop() });
      getCronProxies(this).push(unsCronProxy);
      return unsCronProxy;
    },
  });
};

UnsProxyProcess.use(unsCronPlugin);

export default unsCronPlugin;

export { UnsCronProxy };

export type UnsProxyProcessWithCron = UnsProxyProcess & {
  createCrontabProxy(cronInput: CronScheduleInput, options?: CronProxyOptions): Promise<UnsCronProxy>;
};
