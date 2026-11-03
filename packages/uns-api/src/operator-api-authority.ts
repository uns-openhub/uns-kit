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
    if (!input.declarations.length) {
      if (delegated) throw new OperatorApiError("operator_endpoint_not_declared", 403);
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
    if (this.options.statusUrl) await this.currentAuthorization(input.authorization);
    return operation;
  }

  private async currentAuthorization(authorization: string): Promise<void> {
    if (this.pending >= 4) throw new OperatorApiError("operator_authorization_capacity", 503);
    this.pending++;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 1000);
    try {
      const response = await (this.options.fetch ?? fetch)(this.options.statusUrl!, {
        method: "POST",
        headers: { authorization },
        signal: controller.signal,
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
    } catch (error) {
      if (error instanceof OperatorApiError) throw error;
      throw new OperatorApiError("operator_authorization_unavailable", 503);
    } finally {
      controller.abort();
      clearTimeout(timer);
      this.pending--;
    }
  }
}
