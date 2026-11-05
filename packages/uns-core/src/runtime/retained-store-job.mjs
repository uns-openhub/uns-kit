import { Worker, isMainThread, parentPort, workerData } from "node:worker_threads";
import { inspectRetainedCopy, applyRetainedCopy } from "./retained-store-copy.mjs";
if (!isMainThread) {
  const job = workerData;
  if (job.action === "inspect") parentPort.postMessage({ done: true, result: inspectRetainedCopy(job.path, job.kind, job.limits) });
  else if (job.action === "apply") {
    applyRetainedCopy(job.plan, job.direction, job.limits, id => parentPort.postMessage({ progress: id }));
    parentPort.postMessage({ done: true });
  } else throw Error("Unknown retained storage job");
}
export function retainedStoreJob(job, onProgress) {
  if (!isMainThread) throw Error("Nested retained storage job");
  return new Promise((resolve, reject) => {
    const worker = new Worker(new URL(import.meta.url), { workerData: job, execArgv: [], env: { NODE_NO_WARNINGS: "1" }, resourceLimits: { maxOldGenerationSizeMb: 128 } });
    let done = false;
    const timer = setTimeout(() => finish(Error("Retained storage job timed out; reconcile the same operation")), 300000);
    const finish = (error, result) => {
      if (done) return; done = true; clearTimeout(timer);
      void worker.terminate().finally(() => error ? reject(error) : resolve(result));
    };
    worker.on("message", message => {
      if (message.progress) { try { onProgress?.(message.progress); } catch (error) { finish(error); } }
      if (message.done) finish(undefined, message.result);
    });
    worker.once("error", error => finish(error));
    worker.once("exit", () => { if (!done) finish(Error("Retained storage job exited without confirmation; reconcile the same operation")); });
  });
}
