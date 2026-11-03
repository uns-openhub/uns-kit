import { z } from "zod";

export const RUNTIME_ROUTE_REGISTRATION_PATH = "/api/runtime-services/routes";
export const RUNTIME_ROUTE_REGISTRATION_LEASE_MS = 30_000;
const name = z.string().min(1).max(256);
export const runtimeRouteDeploymentSchema = z.object({
  controllerName: name, rttNode: name, version: name, instanceId: name,
  launchId: z.string().regex(/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/),
}).strict();
export type RuntimeRouteDeployment = z.infer<typeof runtimeRouteDeploymentSchema>;

export function runtimeRouteOrigin(value: string): string {
  if (typeof value !== "string" || value.length > 2048) throw Error("Invalid runtime route origin");
  const url = new URL(value);
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.search || url.hash || url.pathname !== "/")
    throw Error("Invalid runtime route origin");
  return url.origin;
}

export const runtimeRouteBindingSchema = z.object({
  processName: name, version: name, apiHost: z.string().min(1).max(2048), controllerName: name,
  controllerPublicBase: z.string().min(1).max(2048), instanceId: name, deploymentId: z.string().min(1).max(1024),
}).strict();
export type RuntimeRouteBinding = z.infer<typeof runtimeRouteBindingSchema>;
export const runtimeRouteReceiptSchema = z.object({
  protocolVersion: z.literal(1), expiresInMs: z.literal(RUNTIME_ROUTE_REGISTRATION_LEASE_MS), binding: runtimeRouteBindingSchema,
}).strict();

/** A receipt is ownership evidence only for this exact process launch/origin. */
export function readRuntimeRouteReceipt(value: unknown, expected: RuntimeRouteDeployment & { processName: string; apiHost: string }) {
  const receipt = runtimeRouteReceiptSchema.parse(value), binding = receipt.binding;
  if (binding.controllerName !== expected.controllerName || binding.instanceId !== expected.instanceId
    || binding.version !== expected.version || binding.processName !== expected.processName
    || runtimeRouteOrigin(binding.apiHost) !== runtimeRouteOrigin(expected.apiHost)
    || !binding.deploymentId.startsWith(expected.launchId + ":")) throw Error("Runtime route receipt does not match launch");
  runtimeRouteOrigin(binding.controllerPublicBase);
  return receipt;
}
