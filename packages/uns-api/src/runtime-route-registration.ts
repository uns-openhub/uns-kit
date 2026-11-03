import { constants } from "node:fs";
import { open } from "node:fs/promises";
import { isAbsolute } from "node:path";
import {
  RUNTIME_ROUTE_REGISTRATION_PATH, RUNTIME_ROUTE_REGISTRATION_LEASE_MS,
  runtimeRouteDeploymentSchema, runtimeRouteOrigin, readRuntimeRouteReceipt,
  type RuntimeRouteDeployment,
} from "@uns-kit/core/runtime/route-registration.js";

export type RuntimeRouteRegistrationEnvironment = RuntimeRouteDeployment & { controllerBase: string; tokenFile: string };

/** Launcher-owned authority only. An unchanged standalone/legacy launch is opt-out. */
export function runtimeRouteRegistrationEnvironment(env: NodeJS.ProcessEnv): RuntimeRouteRegistrationEnvironment | undefined {
  if (!env.UNS_ROUTE_TOKEN_FILE) return undefined;
  try {
    const deployment = runtimeRouteDeploymentSchema.parse({ controllerName: env.UNS_ROUTE_CONTROLLER_NAME,
      rttNode: env.RTT_NODE, version: env.version, instanceId: env.RTT_INSTANCE_ID, launchId: env.UNS_ROUTE_LAUNCH_ID });
    if (!isAbsolute(env.UNS_ROUTE_TOKEN_FILE)) throw Error();
    let base = env.UNS_CONTROLLER_PUBLIC_BASE;
    if (!base) {
      const host = env.UNS_CONTROLLER_HOST, port = env.UNS_CONTROLLER_PORT;
      if (!host || !port || !/^\d+$/.test(port) || +port < 1 || +port > 65535) throw Error();
      base = `http://${host.includes(":") ? `[${host}]` : host}:${port}`;
    }
    return { ...deployment, tokenFile: env.UNS_ROUTE_TOKEN_FILE, controllerBase: runtimeRouteOrigin(base) };
  } catch { throw Error("Incomplete or invalid managed runtime route environment"); }
}

async function readCredential(file: string): Promise<string> {
  const handle = await open(file, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0) | (constants.O_NONBLOCK ?? 0));
  try {
    const stat = await handle.stat();
    if (!stat.isFile() || stat.size > 16384 || (stat.mode & 0o077) !== 0
      || (process.getuid && stat.uid !== process.getuid())) throw Error();
    const buffer = Buffer.alloc(16385);
    const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
    const token = buffer.subarray(0, bytesRead).toString("utf8").trim();
    if (bytesRead > 16384 || !token || /\s/.test(token)) throw Error();
    return token;
  } finally { await handle.close(); }
}

/** One current launch, one origin and at most one pending request. No data grants. */
export class RuntimeRouteRegistration {
  private timer?: ReturnType<typeof setInterval>;
  private pending?: Promise<boolean>;
  private cancel?: () => void;
  private stopped = false;
  private expiresAt = 0;
  private reported?: boolean;
  private readonly apiHost: string;
  constructor(private readonly options: RuntimeRouteRegistrationEnvironment & {
    processName: string; apiHost: string; fetch?: typeof fetch;
    timeoutMs?: number; intervalMs?: number; onState?: (ready: boolean) => void;
  }) {
    runtimeRouteDeploymentSchema.parse({ controllerName: options.controllerName, rttNode: options.rttNode,
      version: options.version, instanceId: options.instanceId, launchId: options.launchId });
    this.apiHost = runtimeRouteOrigin(options.apiHost);
    runtimeRouteOrigin(options.controllerBase);
    if (!isAbsolute(options.tokenFile) || !options.processName || options.processName.length > 256) throw Error("Invalid runtime route launch options");
  }
  get ready(): boolean { return !this.stopped && this.expiresAt > Date.now(); }
  start(): void {
    if (this.stopped || this.timer) return;
    this.timer = setInterval(() => { void this.renew(); }, this.options.intervalMs ?? 10000);
    this.timer.unref();
    void this.renew();
  }
  renew(): Promise<boolean> {
    if (this.stopped) return Promise.resolve(false);
    if (this.pending) return this.pending;
    const startedAt = Date.now();
    const abort = new AbortController();
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    let expired = false, deadline: ReturnType<typeof setTimeout>;
    let finishDeadline: (value: boolean) => void;
    const timeout = new Promise<boolean>(resolve => { finishDeadline = resolve; });
    this.cancel = () => {
      expired = true; abort.abort();
      void reader?.cancel().catch(() => {});
      finishDeadline(false);
    };
    deadline = setTimeout(() => this.cancel?.(), this.options.timeoutMs ?? 3000);
    const work = (async () => {
      try {
        const token = await readCredential(this.options.tokenFile);
        if (expired || this.stopped) return false;
        const response = await (this.options.fetch ?? fetch)(new URL(RUNTIME_ROUTE_REGISTRATION_PATH, this.options.controllerBase), {
          method: "POST", redirect: "error", signal: abort.signal,
          headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body: JSON.stringify({ apiHost: this.apiHost }),
        });
        if (expired || this.stopped || response.status !== 201 || !response.body || Number(response.headers.get("content-length") ?? 0) > 8192) {
          void response.body?.cancel().catch(() => {}); return false;
        }
        reader = response.body.getReader();
        const chunks: Uint8Array[] = []; let bytes = 0;
        while (true) {
          const part = await reader.read();
          if (part.done) break;
          bytes += part.value.byteLength;
          if (bytes > 8192) { void reader.cancel().catch(() => {}); return false; }
          chunks.push(part.value);
        }
        readRuntimeRouteReceipt(JSON.parse(Buffer.concat(chunks).toString("utf8")), { ...this.options, apiHost: this.apiHost });
        if (expired || this.stopped) return false;
        this.expiresAt = startedAt + RUNTIME_ROUTE_REGISTRATION_LEASE_MS;
        return true;
      } catch { return false; }
      finally { try { reader?.releaseLock(); } catch { /* An aborted reader may still be settling. */ } }
    })();
    // Caller/Stop is bounded even when a transport ignores abort. Such a request
    // stays coalesced until it really settles, rather than accumulating retries.
    const pending = Promise.race([work, timeout]).then(ready => { this.report(ready); return ready; });
    this.pending = pending;
    const clear = () => {
      clearTimeout(deadline);
      if (this.pending === pending) { this.pending = undefined; this.cancel = undefined; }
    };
    void work.then(clear, clear);
    return pending;
  }
  private report(ready: boolean): void {
    if (!ready) this.expiresAt = 0;
    if (ready !== this.reported) {
      this.reported = ready;
      try { this.options.onState?.(ready); } catch { /* Status observers cannot grant or break authority. */ }
    }
  }
  stop(): void {
    this.stopped = true; this.expiresAt = 0;
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined; this.cancel?.();
  }
}
