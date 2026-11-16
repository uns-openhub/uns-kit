import {
  OPERATOR_DELEGATION_CLAIM,
  OPERATOR_OUTCOME_HEADER,
  OperatorApiError,
  type OperatorDeployment,
  type OperatorOperation,
  type OperatorOperationDefinition,
  assertOperatorDelegation,
  assertOperatorPayload,
  operatorDeploymentSchema,
  operatorOperation,
} from "@uns-kit/core/runtime/operator-api.js";

/** Use only when the application can prove its command had no external effect.
 * A database/transport failure after a possible commit must stay uncertain. */
export function markOperatorNotApplied(response: { setHeader(name: string, value: string): unknown }): void {
  response.setHeader(OPERATOR_OUTCOME_HEADER, "not-applied");
}

export function operatorEndpointDeclarations(
  definitions: OperatorOperationDefinition[] | undefined,
  method: string,
  path: string,
  authenticationConfigured: boolean,
): OperatorOperation[] {
  if (!definitions?.length) return [];
  if (!authenticationConfigured) throw new OperatorApiError("operator_endpoint_requires_authentication", 503);
  if (definitions.length > 16) throw new OperatorApiError("operator_endpoint_declaration_limit");
  const parsed = definitions.map((d) => operatorOperation(d, method, path));
  if (new Set(parsed.map((d) => d.id)).size !== parsed.length) throw new OperatorApiError("operator_endpoint_duplicate_id");
  return parsed;
}

/** Only signed claims are passed here. Any managed-service hint must be
 * checked even when an ordinary endpoint has no operator declaration. */
export function hasServiceAuthorizationHint(claims: Record<string, unknown>): boolean {
  return ["serviceToken", "machineIdentityId", "machineIdentityName", "workloadIdentityType",
    "configuredService", "configuredServiceLaunch", "configuredAccess", "catalogPublication", "catalogLaunch", "runtimeRouteDeployment"]
    .some(key => Object.hasOwn(claims, key)) || claims["identityType"] === "machine";
}

/** Route callbacks pass claims only after the existing JWT/JWKS/path checks.
 * Delegated authority is bound again here, inside the application process. */
export class OperatorApiAuthority {
  private pending = 0;
  constructor(
    private readonly options: {
      deployment: () => OperatorDeployment;
      statusUrl?: string;
      fetch?: typeof fetch;
    },
  ) {}

  async authorize(input: {
    declarations: OperatorOperation[];
    claims: Record<string, unknown>;
    authorization: string;
    payload: unknown;
    query?: Record<string, unknown>;
  }): Promise<OperatorOperation | undefined> {
    const delegated = input.claims[OPERATOR_DELEGATION_CLAIM] !== undefined;
    // Configured Service access grants UNS resources, not ordinary service API
    // operations. Match only verified claims; the separately verified Operator
    // delegation keeps its purpose-bound route and authorization checks below.
    if (!delegated && Object.hasOwn(input.claims, "configuredService"))
      throw new OperatorApiError("service_api_permission_required", 403);
    const service = !delegated && hasServiceAuthorizationHint(input.claims);
    if (!input.declarations.length) {
      if (delegated) throw new OperatorApiError("operator_endpoint_not_declared", 403);
      if (service) await this.currentAuthorization(input.authorization, true);
      return undefined;
    }
    const roles = [input.claims["role"], ...(Array.isArray(input.claims["roles"]) ? input.claims["roles"] : [])];
    if (!roles.some((r) => r === "admin" || r === "operator") || typeof input.claims["sub"] !== "string" || !input.claims["sub"])
      throw new OperatorApiError("operator_role_required", 403);
    if (input.query && Object.keys(input.query).length) throw new OperatorApiError("operator_query_not_supported");
    const matched = input.declarations.filter((d) => {
      try {
        assertOperatorPayload(d, input.payload);
        return true;
      } catch {
        return false;
      }
    });
    if (matched.length !== 1) throw new OperatorApiError(matched.length ? "operator_request_ambiguous" : "operator_request_invalid");
    const operation = matched[0]!;
    if (delegated) {
      let deployment: OperatorDeployment;
      try {
        deployment = operatorDeploymentSchema.parse(this.options.deployment());
      } catch {
        throw new OperatorApiError("operator_deployment_unavailable", 503);
      }
      try {
        assertOperatorDelegation(input.claims, operation, input.payload, deployment);
      } catch {
        throw new OperatorApiError("operator_delegation_rejected", 403);
      }
      if (!this.options.statusUrl) throw new OperatorApiError("operator_authorization_status_required", 503);
    }
    // A standalone, locally verified native operator token can remain offline.
    // Delegated machine authority always requires a fresh parent-status check.
    if (service || this.options.statusUrl) await this.currentAuthorization(input.authorization, service);
    return operation;
  }

  private async currentAuthorization(authorization: string, service = false): Promise<void> {
    if (!this.options.statusUrl)
      throw new OperatorApiError(service ? "service_authorization_status_required" : "operator_authorization_status_required", 503);
    if (this.pending >= 16) throw new OperatorApiError("operator_authorization_capacity", 503);
    this.pending++;
    const controller = new AbortController();
    // Keep the slot occupied until the underlying request settles, even if a
    // custom fetch implementation ignores abort. The caller still gets a
    // bounded response and stuck requests cannot accumulate without limit.
    const work = this.probeAuthorization(authorization, service, controller.signal)
      .finally(() => { this.pending--; });
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([work, new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          controller.abort();
          reject(new OperatorApiError("operator_authorization_unavailable", 503));
        }, 1000);
      })]);
    } catch (error) {
      if (error instanceof OperatorApiError) throw error;
      throw new OperatorApiError("operator_authorization_unavailable", 503);
    } finally {
      controller.abort();
      if (timer) clearTimeout(timer);
    }
  }

  private async probeAuthorization(authorization: string, service: boolean, signal: AbortSignal): Promise<void> {
    const response = await (this.options.fetch ?? fetch)(this.options.statusUrl!, {
      method: "POST",
      headers: { authorization },
      signal,
      redirect: "error",
    });
    if (response.status === 401 || response.status === 403) throw new OperatorApiError("operator_authorization_inactive", 403);
    if (!response.ok || Number(response.headers.get("content-length") ?? 0) > 4096)
      throw new OperatorApiError("operator_authorization_unavailable", 503);
    if (!response.body) throw new OperatorApiError("operator_authorization_unavailable", 503);
    const reader = response.body.getReader();
    let length = 0,
      body = "";
    try {
      const decoder = new TextDecoder();
      while (true) {
        const part = await reader.read();
        if (part.done) break;
        length += part.value.byteLength;
        if (length > 4096) {
          await reader.cancel();
          throw new OperatorApiError("operator_authorization_unavailable", 503);
        }
        body += decoder.decode(part.value, { stream: true });
      }
      body += decoder.decode();
    } finally {
      reader.releaseLock();
    }
    const status = JSON.parse(body);
    if (status.active !== true || status.protocolVersion !== 1) throw new OperatorApiError("operator_authorization_inactive", 403);
    // An older Controller can claim a token is active without checking its
    // configured decision. Never let that response authorize a service token.
    if (service && (status.principalType !== "machine" || status.configuredAccessAuthority !== 2))
      throw new OperatorApiError("service_authorization_unavailable", 503);
  }
}
