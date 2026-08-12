import { readFile as readFileFromFs } from "node:fs/promises";

/**
 * A source of bearer tokens for controller and DataHub requests.
 *
 * The interface is intentionally small so services can use a controller-issued
 * machine token without depending on the legacy user-login client.
 */
export interface AccessTokenProvider {
  getAccessToken(): Promise<string | undefined>;
}

type ServiceTokenEnvironment = Partial<Pick<NodeJS.ProcessEnv, "UNS_SERVICE_TOKEN_FILE" | "UNS_SERVICE_TOKEN">>;

export type ServiceTokenProviderOptions = {
  /**
   * Path to a controller-managed token file. When set, the file is
   * authoritative and failures are reported instead of falling back to another
   * credential source.
   */
  tokenFile?: string;
  /** Environment used to resolve UNS_SERVICE_TOKEN_FILE and UNS_SERVICE_TOKEN. */
  environment?: ServiceTokenEnvironment;
  /** Resolved config.uns.token value. */
  configToken?: string;
  /** Legacy development fallback, normally an AuthClient using uns.email/password. */
  fallback?: AccessTokenProvider;
  /** Test seam; production uses node:fs/promises.readFile. */
  readFile?: (path: string, encoding: BufferEncoding) => Promise<string>;
};

/**
 * Resolves service credentials in the order used by controller-managed RTT
 * services: mounted token file, environment, configuration, then the legacy
 * login provider. The mounted file is deliberately read for each request so
 * atomically-rotated controller credentials are picked up without restart.
 */
export class ServiceTokenProvider implements AccessTokenProvider {
  private readonly tokenFile?: string;
  private readonly environment: ServiceTokenEnvironment;
  private readonly configToken?: string;
  private readonly fallback?: AccessTokenProvider;
  private readonly readFile: (path: string, encoding: BufferEncoding) => Promise<string>;

  constructor(options: ServiceTokenProviderOptions = {}) {
    this.tokenFile = normalizeToken(options.tokenFile);
    this.environment = options.environment ?? process.env;
    this.configToken = normalizeToken(options.configToken);
    this.fallback = options.fallback;
    this.readFile = options.readFile ?? readFileFromFs;
  }

  async getAccessToken(): Promise<string | undefined> {
    const tokenFile = this.tokenFile ?? normalizeToken(this.environment.UNS_SERVICE_TOKEN_FILE);
    if (tokenFile) {
      return this.readControllerManagedToken(tokenFile);
    }

    const environmentToken = normalizeToken(this.environment.UNS_SERVICE_TOKEN);
    if (environmentToken) {
      return environmentToken;
    }

    if (this.configToken) {
      return this.configToken;
    }

    return this.fallback?.getAccessToken();
  }

  private async readControllerManagedToken(path: string): Promise<string> {
    try {
      const token = normalizeToken(await this.readFile(path, "utf8"));
      if (!token) {
        throw new Error("empty token file");
      }
      return token;
    } catch (error) {
      throw new Error("Controller-managed service token file is unavailable.", {
        cause: error instanceof Error ? error : undefined,
      });
    }
  }
}

function normalizeToken(value: string | undefined): string | undefined {
  const token = value?.trim();
  return token ? token : undefined;
}
