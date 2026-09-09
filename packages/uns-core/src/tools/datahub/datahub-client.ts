import { AuthClient } from "../auth/auth-client.js";
import type { AccessTokenProvider } from "../auth/service-token-provider.js";

export type LastValuePayload = {
  topic: string;
  value: unknown;
  values?: Record<string, unknown> | null;
  uom?: string | null;
  timestamp?: string | null;
  dataGroup?: string | null;
  ageMs?: number | null;
  source: string;
};

export type CatchAllTimeField = "auto" | "timestamp" | "interval";
export type CatchAllAggregate = "avg" | "min" | "max" | "last" | "sum" | "count";

export type RangeQueryOptions = {
  table?: string;
  from?: string;
  to?: string;
  timeField?: CatchAllTimeField;
  limit?: number;
  maxPoints?: number;
  bucketMs?: number;
  aggregate?: CatchAllAggregate;
  column?: string;
  summaryOnly?: boolean;
  dedupe?: boolean;
};

export type RangeColumn = {
  name: string;
  type?: string;
};

export type RangeStats = Record<string, unknown> & {
  raw?: {
    columns?: RangeColumn[];
  };
};

export type RangePayload<Row extends unknown[] = unknown[]> = {
  data: Row[];
  stats?: RangeStats | null;
};

export type BatchRangeTopicPayload<Row extends unknown[] = unknown[]> = {
  topic: string;
  error?: string | null;
  data: Row[];
  stats?: RangeStats | null;
};

export type BatchRangeResponsePayload<Row extends unknown[] = unknown[]> = {
  results: BatchRangeTopicPayload<Row>[];
  stats?: Record<string, unknown> | null;
};

export type ProviderAssetIdentity = {
  providerId: string;
  externalSystem: string;
  externalType: string;
  externalId: string;
};

/** Metadata that can be spread directly into an Asset-level UNS publish object. */
export type AssetIdentityPublicationMetadata = {
  assetStableEntityId: string;
  assetIdentityProof: string;
  candidateAssetPath: string;
  expiresAt: string;
};

export type AssetProviderIdentityPublicationMetadata = {
  assetProviderIdentity: ProviderAssetIdentity;
  assetProviderIdentityProof: string;
  candidateAssetPath: string;
  expiresAt: string;
};

export type AssetIdentityPublicationEvidenceMetadata =
  | AssetIdentityPublicationMetadata
  | AssetProviderIdentityPublicationMetadata;

export class RangeResult<Row extends unknown[] = unknown[]> {
  readonly data: Row[];
  readonly stats?: RangeStats | null;

  constructor(payload: RangePayload<Row>) {
    this.data = payload.data;
    this.stats = payload.stats ?? null;
  }

  static fromMapping<Row extends unknown[] = unknown[]>(value: Record<string, unknown>): RangeResult<Row> {
    return new RangeResult<Row>({
      data: Array.isArray(value.data) ? (value.data.filter(Array.isArray) as Row[]) : [],
      stats: value.stats && typeof value.stats === "object" && !Array.isArray(value.stats) ? (value.stats as RangeStats) : null,
    });
  }

  get columns(): RangeColumn[] {
    const columns = this.stats?.raw?.columns;
    return Array.isArray(columns) ? columns : [];
  }

  toRecords(): Array<Record<string, unknown>> {
    const columns = this.columns;
    if (!columns.length) {
      return this.data.map((row) => Object.fromEntries(row.map((value, index) => [String(index), value])));
    }
    return this.data.map((row) =>
      Object.fromEntries(row.map((value, index) => [columns[index]?.name ?? String(index), value])),
    );
  }

  toObject(): RangePayload<Row> {
    return {
      data: this.data,
      stats: this.stats ?? null,
    };
  }
}

export class BatchRangeTopicResult<Row extends unknown[] = unknown[]> extends RangeResult<Row> {
  readonly topic: string;
  readonly error?: string | null;

  constructor(payload: BatchRangeTopicPayload<Row>) {
    super({
      data: payload.data ?? [],
      stats: payload.stats ?? null,
    });
    this.topic = payload.topic;
    this.error = payload.error ?? null;
  }

  static fromMapping<Row extends unknown[] = unknown[]>(value: Record<string, unknown>): BatchRangeTopicResult<Row> {
    return new BatchRangeTopicResult<Row>({
      topic: typeof value.topic === "string" ? value.topic : "",
      error: typeof value.error === "string" ? value.error : null,
      data: Array.isArray(value.data) ? (value.data.filter(Array.isArray) as Row[]) : [],
      stats: value.stats && typeof value.stats === "object" && !Array.isArray(value.stats) ? (value.stats as RangeStats) : null,
    });
  }

  override toObject(): BatchRangeTopicPayload<Row> {
    return {
      topic: this.topic,
      error: this.error ?? null,
      data: this.data,
      stats: this.stats ?? null,
    };
  }
}

export class BatchRangeResponse<Row extends unknown[] = unknown[]> {
  readonly results: BatchRangeTopicResult<Row>[];
  readonly stats?: Record<string, unknown> | null;

  constructor(payload: BatchRangeResponsePayload<Row>) {
    this.results = payload.results.map((item) => new BatchRangeTopicResult<Row>(item));
    this.stats = payload.stats ?? null;
  }

  static fromMapping<Row extends unknown[] = unknown[]>(value: Record<string, unknown>): BatchRangeResponse<Row> {
    const rawResults = Array.isArray(value.results)
      ? value.results.filter((item): item is Record<string, unknown> => !!item && typeof item === "object" && !Array.isArray(item))
      : [];
    return new BatchRangeResponse<Row>({
      results: rawResults.map((item) => BatchRangeTopicResult.fromMapping<Row>(item).toObject()),
      stats: value.stats && typeof value.stats === "object" && !Array.isArray(value.stats) ? (value.stats as Record<string, unknown>) : null,
    });
  }

  get byTopic(): Record<string, BatchRangeTopicPayload<Row>> {
    return Object.fromEntries(this.results.map((result) => [result.topic, result.toObject()]));
  }

  toObject(): BatchRangeResponsePayload<Row> {
    return {
      results: this.results.map((result) => result.toObject()),
      stats: this.stats ?? null,
    };
  }
}

export class LastValueResult {
  readonly topic: string;
  readonly value: unknown;
  readonly values?: Record<string, unknown> | null;
  readonly uom?: string | null;
  readonly timestamp?: string | null;
  readonly dataGroup?: string | null;
  readonly ageMs?: number | null;
  readonly source: string;

  constructor(payload: LastValuePayload) {
    this.topic = payload.topic;
    this.value = payload.value;
    this.values = payload.values ?? null;
    this.uom = payload.uom ?? null;
    this.timestamp = payload.timestamp ?? null;
    this.dataGroup = payload.dataGroup ?? null;
    this.ageMs = payload.ageMs ?? null;
    this.source = payload.source;
  }

  static fromMapping(value: Record<string, unknown>): LastValueResult {
    const rawValues = value.values;
    return new LastValueResult({
      topic: typeof value.topic === "string" ? value.topic : "",
      value: value.value,
      values: rawValues && typeof rawValues === "object" && !Array.isArray(rawValues) ? (rawValues as Record<string, unknown>) : null,
      uom: typeof value.uom === "string" ? value.uom : null,
      timestamp: typeof value.timestamp === "string" ? value.timestamp : null,
      dataGroup: typeof value.dataGroup === "string" ? value.dataGroup : null,
      ageMs: typeof value.ageMs === "number" ? value.ageMs : null,
      source: typeof value.source === "string" ? value.source : "miss",
    });
  }

  get hit(): boolean {
    return this.source === "cache";
  }

  toObject(): LastValuePayload {
    return {
      topic: this.topic,
      value: this.value,
      values: this.values ?? null,
      uom: this.uom ?? null,
      timestamp: this.timestamp ?? null,
      dataGroup: this.dataGroup ?? null,
      ageMs: this.ageMs ?? null,
      source: this.source,
    };
  }
}

export class ClientError extends Error {
  readonly statusCode?: number;

  constructor(message: string, statusCode?: number) {
    super(message);
    this.statusCode = statusCode;
  }
}

export type UnsClientOptions = {
  apiBasePath?: string;
  /** Full controller GraphQL URL when it is not `${baseUrl}/graphql`. */
  graphqlUrl?: string;
  token?: string;
  timeoutMs?: number;
  /** Preferred non-interactive service credential source. */
  tokenProvider?: AccessTokenProvider;
  authClient?: AuthClient;
};

export type RequestOptions = {
  baseUrl?: string;
  authorize?: boolean;
};

export class UnsClient {
  private readonly apiBasePath: string;
  private readonly baseUrl: string;
  private readonly apiUrl: string;
  private readonly graphqlUrl: string;
  private readonly timeoutMs: number;
  private readonly tokenProvider?: AccessTokenProvider;
  private readonly authClient?: AuthClient;
  private manualAccessToken?: string;

  constructor(baseUrl: string, options: UnsClientOptions = {}) {
    const apiBasePath = UnsClient.normalizeBasePath(options.apiBasePath ?? "/api");
    const strippedBase = baseUrl.replace(/\/$/, "");
    if (apiBasePath && strippedBase.endsWith(apiBasePath)) {
      this.apiUrl = strippedBase;
      this.baseUrl = strippedBase.slice(0, -apiBasePath.length).replace(/\/$/, "");
    } else {
      this.baseUrl = strippedBase;
      this.apiUrl = `${strippedBase}${apiBasePath}`;
    }
    this.apiBasePath = apiBasePath;
    this.graphqlUrl = options.graphqlUrl?.replace(/\/$/, "") || `${this.baseUrl}/graphql`;
    this.timeoutMs = options.timeoutMs ?? 10_000;
    this.tokenProvider = options.tokenProvider;
    this.authClient = options.authClient;
    this.manualAccessToken = options.token;
  }

  setToken(token?: string): void {
    this.manualAccessToken = token;
  }

  async ensureToken(): Promise<string | undefined> {
    if (this.manualAccessToken) return this.manualAccessToken;
    const serviceToken = await this.tokenProvider?.getAccessToken();
    if (serviceToken) return serviceToken;
    if (!this.authClient) return undefined;
    return this.authClient.getAccessToken();
  }

  async get(endpoint: string, params?: Record<string, string | number | boolean>, options: RequestOptions = {}): Promise<Response> {
    const authorize = options.authorize ?? true;
    const token = authorize ? await this.ensureToken() : undefined;
    const search = params ? `?${new URLSearchParams(this.stringifyQueryParams(params))}` : "";
    return this.request("GET", `${this.buildUrl(endpoint, options.baseUrl)}${search}`, undefined, token);
  }

  async getData(
    endpoint: string,
    params?: Record<string, string | number | boolean>,
    options: RequestOptions = {},
  ): Promise<Response> {
    return this.get(endpoint, params, options);
  }

  async post(endpoint: string, body?: Record<string, unknown>, options: RequestOptions = {}): Promise<Record<string, unknown>> {
    const authorize = options.authorize ?? true;
    const token = authorize ? await this.ensureToken() : undefined;
    return this.requestJson("POST", this.buildUrl(endpoint, options.baseUrl), body ?? {}, token);
  }

  async lastValue(topics: string | string[], options: { token?: string } = {}): Promise<Record<string, LastValuePayload> | null> {
    const topicList = Array.isArray(topics) ? topics : [topics];
    if (!topicList.length) {
      throw new Error("topics must contain at least one topic.");
    }
    if (topicList.length > 500) {
      throw new Error("Maximum 500 topics per request.");
    }
    const token = options.token ?? (await this.ensureToken());
    try {
      const payload = await this.requestJson("POST", this.buildUrl("catchall/batch/last"), { topics: topicList }, token);
      const rawResults = payload.results;
      if (!Array.isArray(rawResults)) {
        throw new ClientError("Last-value response did not include a results array.");
      }
      const results = rawResults
        .filter((item): item is Record<string, unknown> => !!item && typeof item === "object" && !Array.isArray(item))
        .map((item) => LastValueResult.fromMapping(item));
      return Object.fromEntries(results.map((result) => [result.topic, result.toObject()]));
    } catch (error) {
      if (error instanceof ClientError && error.statusCode === 404) {
        return null;
      }
      throw error;
    }
  }

  async getAttributeData<Row extends unknown[] = unknown[]>(
    topicPath: string,
    options: RangeQueryOptions & { token?: string } = {},
  ): Promise<RangeResult<Row> | null> {
    const token = options.token ?? (await this.ensureToken());
    const { token: _token, ...query } = options;
    try {
      const payload = await this.requestJson(
        "GET",
        this.buildUrl(`catchall/${encodeURIComponent(topicPath)}`) + this.buildQuerySuffix(query),
        undefined,
        token,
      );
      return RangeResult.fromMapping<Row>(payload);
    } catch (error) {
      if (error instanceof ClientError && error.statusCode === 404) {
        return null;
      }
      throw error;
    }
  }

  async history<Row extends unknown[] = unknown[]>(
    topics: string | string[],
    options: RangeQueryOptions & { token?: string } = {},
  ): Promise<BatchRangeResponse<Row> | null> {
    const topicList = Array.isArray(topics) ? topics : [topics];
    if (!topicList.length) {
      throw new Error("topics must contain at least one topic.");
    }
    if (topicList.length > 500) {
      throw new Error("Maximum 500 topics per request.");
    }
    const token = options.token ?? (await this.ensureToken());
    const { token: _token, ...body } = options;
    try {
      const payload = await this.requestJson(
        "POST",
        this.buildUrl("catchall/batch/range"),
        { topics: topicList, ...body },
        token,
      );
      const rawResults = payload.results;
      if (!Array.isArray(rawResults)) {
        throw new ClientError("Batch-range response did not include a results array.");
      }
      return BatchRangeResponse.fromMapping<Row>(payload);
    } catch (error) {
      if (error instanceof ClientError && error.statusCode === 404) {
        return null;
      }
      throw error;
    }
  }

  async issueAssetIdentityPublicationProof(
    stableEntityId: string,
    candidateAssetPath: string,
    options: { token?: string } = {},
  ): Promise<AssetIdentityPublicationMetadata> {
    return this.requestAssetIdentityPublicationProof(
      `mutation IssueAssetIdentityPublicationProof($stableEntityId: ID!, $candidateAssetPath: String!) {
        IssueAssetIdentityPublicationProof(
          stableEntityId: $stableEntityId
          candidateAssetPath: $candidateAssetPath
        ) {
          proof
          stableEntityId
          candidateAssetPath
          expiresAt
        }
      }`,
      { stableEntityId, candidateAssetPath },
      "IssueAssetIdentityPublicationProof",
      options.token,
    );
  }

  async issueAssetIdentityPublicationProofByExternalIdentity(
    identity: ProviderAssetIdentity,
    candidateAssetPath: string,
    options: { token?: string } = {},
  ): Promise<AssetIdentityPublicationMetadata> {
    return this.requestAssetIdentityPublicationProof(
      `mutation IssueAssetIdentityPublicationProofByExternalIdentity(
        $input: ProviderAssetIdentityPublicationProofInput!
      ) {
        IssueAssetIdentityPublicationProofByExternalIdentity(input: $input) {
          proof
          stableEntityId
          candidateAssetPath
          expiresAt
        }
      }`,
      { input: { ...identity, candidateAssetPath } },
      "IssueAssetIdentityPublicationProofByExternalIdentity",
      options.token,
    );
  }

  async issueAssetIdentityPublicationEvidenceByExternalIdentity(
    identity: ProviderAssetIdentity,
    candidateAssetPath: string,
    options: { token?: string } = {},
  ): Promise<AssetIdentityPublicationEvidenceMetadata> {
    const token = options.token ?? (await this.ensureToken());
    const query = `mutation IssueAssetIdentityPublicationEvidenceByExternalIdentity(
      $input: ProviderAssetIdentityPublicationProofInput!
    ) {
      IssueAssetIdentityPublicationEvidenceByExternalIdentity(input: $input) {
        mode
        proof
        stableEntityId
        providerId
        externalSystem
        externalType
        externalId
        candidateAssetPath
        expiresAt
      }
    }`;
    const payload = await this.requestJson(
      "POST",
      this.graphqlUrl,
      { query, variables: { input: { ...identity, candidateAssetPath } } },
      token,
    );
    const errors = Array.isArray(payload.errors) ? payload.errors : [];
    if (errors.length > 0) {
      const message = errors.map((error) => this.graphqlErrorMessage(error)).filter(Boolean).join("; ");
      throw new ClientError(`Asset identity evidence request failed: ${message || "GraphQL returned an error."}`);
    }
    const data = this.objectValue(payload.data);
    const evidence = this.objectValue(data?.IssueAssetIdentityPublicationEvidenceByExternalIdentity);
    const mode = this.requiredResponseString(evidence?.mode, "mode");
    const proof = this.requiredResponseString(evidence?.proof, "proof");
    const path = this.requiredResponseString(evidence?.candidateAssetPath, "candidateAssetPath");
    const expiresAt = this.requiredResponseString(evidence?.expiresAt, "expiresAt");
    if (mode === "stable") {
      return {
        assetStableEntityId: this.requiredResponseString(evidence?.stableEntityId, "stableEntityId").toLowerCase(),
        assetIdentityProof: proof,
        candidateAssetPath: path,
        expiresAt,
      };
    }
    if (mode === "provider-candidate") {
      return {
        assetProviderIdentity: {
          providerId: this.requiredResponseString(evidence?.providerId, "providerId").toLowerCase(),
          externalSystem: this.requiredResponseString(evidence?.externalSystem, "externalSystem").toLowerCase(),
          externalType: this.requiredResponseString(evidence?.externalType, "externalType").toLowerCase(),
          externalId: this.requiredResponseString(evidence?.externalId, "externalId"),
        },
        assetProviderIdentityProof: proof,
        candidateAssetPath: path,
        expiresAt,
      };
    }
    throw new ClientError(`Asset identity evidence response used unsupported mode: ${mode}.`);
  }

  private async requestAssetIdentityPublicationProof(
    query: string,
    variables: Record<string, unknown>,
    fieldName: string,
    explicitToken?: string,
  ): Promise<AssetIdentityPublicationMetadata> {
    const token = explicitToken ?? (await this.ensureToken());
    const payload = await this.requestJson("POST", this.graphqlUrl, { query, variables }, token);
    const errors = Array.isArray(payload.errors) ? payload.errors : [];
    if (errors.length > 0) {
      const message = errors
        .map((error) => this.graphqlErrorMessage(error))
        .filter(Boolean)
        .join("; ");
      throw new ClientError(`Asset identity proof request failed: ${message || "GraphQL returned an error."}`);
    }
    const data = this.objectValue(payload.data);
    const proof = this.objectValue(data?.[fieldName]);
    const assetStableEntityId = this.requiredResponseString(proof?.stableEntityId, "stableEntityId").toLowerCase();
    return {
      assetStableEntityId,
      assetIdentityProof: this.requiredResponseString(proof?.proof, "proof"),
      candidateAssetPath: this.requiredResponseString(proof?.candidateAssetPath, "candidateAssetPath"),
      expiresAt: this.requiredResponseString(proof?.expiresAt, "expiresAt"),
    };
  }

  private graphqlErrorMessage(value: unknown): string {
    const error = this.objectValue(value);
    return typeof error?.message === "string" ? error.message.trim() : "";
  }

  private objectValue(value: unknown): Record<string, unknown> | null {
    return value && typeof value === "object" && !Array.isArray(value)
      ? value as Record<string, unknown>
      : null;
  }

  private requiredResponseString(value: unknown, field: string): string {
    const normalized = typeof value === "string" ? value.trim() : "";
    if (!normalized) {
      throw new ClientError(`Asset identity proof response did not include ${field}.`);
    }
    return normalized;
  }

  private async requestJson(
    method: "GET" | "POST",
    url: string,
    body?: Record<string, unknown>,
    token?: string,
  ): Promise<Record<string, unknown>> {
    try {
      const resp = await this.request(method, url, body, token);
      const text = await resp.text();
      if (!text) return {};
      const parsed = JSON.parse(text);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        throw new ClientError("UNS response must be a JSON object.");
      }
      return parsed as Record<string, unknown>;
    } catch (error: any) {
      if (error instanceof ClientError) {
        throw error;
      }
      if (error?.name === "AbortError") {
        throw new ClientError("UNS request timed out.");
      }
      throw new ClientError(`UNS request failed: ${error?.message ?? String(error)}`);
    }
  }

  private async request(
    method: "GET" | "POST",
    url: string,
    body?: Record<string, unknown>,
    token?: string,
  ): Promise<Response> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const resp = await fetch(url, {
        method,
        headers: {
          "Accept": "*/*",
          ...(body ? { "Content-Type": "application/json" } : {}),
          ...(token ? { "Authorization": `Bearer ${token}` } : {}),
        },
        ...(body ? { body: JSON.stringify(body) } : {}),
        signal: controller.signal,
      });
      if (!resp.ok) {
        const text = await resp.text();
        throw new ClientError(
          `UNS request failed with HTTP ${resp.status}: ${text || resp.statusText}`,
          resp.status,
        );
      }
      return resp;
    } finally {
      clearTimeout(timeout);
    }
  }

  private buildUrl(endpoint: string, baseUrl?: string): string {
    if (endpoint.startsWith("http://") || endpoint.startsWith("https://")) {
      return endpoint;
    }
    const root = (baseUrl ?? this.apiUrl).replace(/\/$/, "");
    let path = endpoint.startsWith("/") ? endpoint : `/${endpoint}`;
    if (!baseUrl && this.apiBasePath && (path === this.apiBasePath || path.startsWith(`${this.apiBasePath}/`))) {
      path = path.slice(this.apiBasePath.length) || "/";
    }
    return `${root}${path}`;
  }

  private stringifyQueryParams(params: Record<string, string | number | boolean>): Record<string, string> {
    return Object.fromEntries(Object.entries(params).map(([key, value]) => [key, String(value)]));
  }

  private buildQuerySuffix(params: Record<string, unknown>): string {
    const filteredEntries = Object.entries(params).filter(([, value]) => value !== undefined && value !== null);
    if (!filteredEntries.length) {
      return "";
    }
    const search = new URLSearchParams(
      Object.fromEntries(filteredEntries.map(([key, value]) => [key, String(value)])),
    );
    return `?${search.toString()}`;
  }

  private static normalizeBasePath(value: string): string {
    const stripped = value.trim();
    if (!stripped) return "";
    return stripped.startsWith("/") ? stripped : `/${stripped}`;
  }
}
