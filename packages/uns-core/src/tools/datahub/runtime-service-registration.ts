/**
 * Non-secret descriptor sent by a controller-managed RTT service after it has
 * started. The controller derives identity and permissions from the bearer
 * token; this descriptor can therefore never request additional access.
 */
export type RuntimeServiceDescriptor = {
  id: string;
  version?: string;
  capabilities?: string[];
  healthContract?: string;
  processName?: string;
};

export type RuntimeServiceRegistration = {
  registered: true;
  service: {
    rttNode: string;
    instanceId: string;
    serviceVersion?: string;
  };
  controller: {
    restPath: string;
    graphqlPath: string;
  };
};

export type RuntimeServiceRegistrationClient = {
  post(endpoint: string, body?: Record<string, unknown>): Promise<Record<string, unknown>>;
};

export type RegisterServiceOptions = {
  client: RuntimeServiceRegistrationClient;
  service: RuntimeServiceDescriptor;
  environment?: Partial<Pick<NodeJS.ProcessEnv, "RTT_NODE" | "RTT_INSTANCE_ID">>;
};

/**
 * Registers a service only when it was launched by the controller's RTT
 * manager. Direct development deliberately has no RTT instance identifiers
 * and therefore returns null instead of pretending to be a managed service.
 */
export async function registerService(
  options: RegisterServiceOptions,
): Promise<RuntimeServiceRegistration | null> {
  const environment = options.environment ?? process.env;
  const rttNode = normalizeRequired(environment.RTT_NODE);
  const instanceId = normalizeRequired(environment.RTT_INSTANCE_ID);
  if (!rttNode || !instanceId) return null;

  const serviceId = normalizeRequired(options.service.id);
  if (!serviceId) {
    throw new Error("Runtime service registration requires service.id.");
  }
  if (serviceId !== rttNode) {
    throw new Error("Runtime service id does not match the controller-injected RTT_NODE.");
  }

  const payload = await options.client.post("runtime-services/register", {
    rttNode,
    instanceId,
    ...(optionalString(options.service.version) ? { serviceVersion: optionalString(options.service.version) } : {}),
    ...(normalizeStringList(options.service.capabilities).length > 0
      ? { capabilities: normalizeStringList(options.service.capabilities) }
      : {}),
    ...(optionalString(options.service.healthContract)
      ? { healthContract: optionalString(options.service.healthContract) }
      : {}),
    ...(optionalString(options.service.processName)
      ? { processName: optionalString(options.service.processName) }
      : {}),
  });
  return parseRegistration(payload);
}

function parseRegistration(payload: Record<string, unknown>): RuntimeServiceRegistration {
  const service = asObject(payload.service);
  const controller = asObject(payload.controller);
  const rttNode = optionalString(service?.rttNode);
  const instanceId = optionalString(service?.instanceId);
  const restPath = optionalString(controller?.restPath);
  const graphqlPath = optionalString(controller?.graphqlPath);
  if (payload.registered !== true || !rttNode || !instanceId || !restPath || !graphqlPath) {
    throw new Error("Controller returned an invalid runtime service registration response.");
  }
  const serviceVersion = optionalString(service?.serviceVersion);
  return {
    registered: true,
    service: {
      rttNode,
      instanceId,
      ...(serviceVersion ? { serviceVersion } : {}),
    },
    controller: { restPath, graphqlPath },
  };
}

function asObject(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function optionalString(value: unknown): string | undefined {
  const normalized = typeof value === "string" ? value.trim() : "";
  return normalized || undefined;
}

function normalizeRequired(value: unknown): string | undefined {
  return optionalString(value);
}

function normalizeStringList(values: string[] | undefined): string[] {
  return [...new Set((values ?? []).map((value) => optionalString(value)).filter((value): value is string => Boolean(value)))];
}
