import { ConfigFile, UnsProxyProcess } from "@uns-kit/core";
import { setTimeout as delay } from "node:timers/promises";

import { readHistory } from "./reader.mjs";

const config = await ConfigFile.loadConfig();
const controllerBase = process.env.UNS_CONTROLLER_PUBLIC_BASE;
if (!controllerBase) throw new Error("UNS_CONTROLLER_PUBLIC_BASE must name your Controller.");
const stop = new AbortController();
const runtime = new UnsProxyProcess(config.infra.host, {
  processName: config.uns.processName,
  username: config.mqtt?.username,
  password: config.mqtt?.password,
});
let currentRead = Promise.resolve();
let stopping;
const shutdown = () =>
  (stopping ??= (async () => {
    stop.abort();
    await currentRead.catch(() => {});
    await runtime.shutdown();
  })());
for (const signal of ["SIGINT", "SIGTERM"])
  process.once(signal, () => {
    void shutdown().then(
      () => process.exit(0),
      () => {
        process.exitCode = 1;
      },
    );
  });
try {
  await runtime.prepareHandover(config.uns.instanceMode, config.uns.handover);
  while (!stop.signal.aborted) {
    if (runtime.isActive()) {
      const to = new Date(),
        from = new Date(to.getTime() - 5 * 60000);
      try {
        currentRead = readHistory(controllerBase, from.toISOString(), to.toISOString(), { config, signal: stop.signal });
        const rows = await currentRead;
        // Put application processing here. Never log bearer values or raw records.
        console.info(`Read ${rows.length} samples.`);
      } catch {
        if (!stop.signal.aborted) console.warn("History read failed; check Service access and API availability.");
      }
    }
    try {
      await delay(30000, undefined, { signal: stop.signal });
    } catch (error) {
      if (!stop.signal.aborted) throw error;
    }
  }
} finally {
  await shutdown();
}
