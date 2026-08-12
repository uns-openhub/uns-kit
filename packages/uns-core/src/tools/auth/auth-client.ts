import { ConfigFile } from "../../config-file.js";
import { SecureStoreFactory, ISecureStore } from "./secure-store.js";
import jwt from "jsonwebtoken";

type LoginResponse = {
  accessToken: string;
};

/**
 * AuthClient handles acquiring and refreshing JWT access tokens
 * using the configured REST base URL.
 */
export class AuthClient {
  private readonly restBase: string;
  private readonly namespace: string;
  private readonly configEmail?: string;
  private readonly configPassword?: string;
  private store!: ISecureStore;

  private constructor(restBase: string, configEmail?: string, configPassword?: string) {
    this.restBase = restBase.replace(/\/$/, "");
    // namespace store by rest base to allow multiple environments
    this.namespace = `uns-auth:${this.restBase}`;
    this.configEmail = configEmail;
    this.configPassword = configPassword;
  }

  static async create(): Promise<AuthClient> {
    const cfg = await ConfigFile.loadConfig();
    const restBase: string = cfg?.uns?.rest;
    if (!restBase) throw new Error("config.uns.rest is not set");
    const client = new AuthClient(
      restBase,
      typeof cfg?.uns?.email === "string" ? cfg.uns.email : undefined,
      typeof cfg?.uns?.password === "string" ? cfg.uns.password : undefined,
    );
    client.store = await SecureStoreFactory.create(client.namespace);
    return client;
  }

  async getAccessToken(): Promise<string> {
    const accessToken = await this.store.get("accessToken");
    const refreshToken = await this.store.get("refreshToken");

    if (accessToken && !AuthClient.isExpired(accessToken)) {
      return accessToken;
    }

    // Try refresh if we have refresh token
    if (refreshToken) {
      try {
        const refreshed = await this.refresh(refreshToken);
        await this.persistTokens(refreshed.accessToken, refreshed.refreshToken);
        return refreshed.accessToken;
      } catch {
        // ignore, fallback to login
      }
    }

    // First try to get email and password from config
    if (this.configEmail && this.configPassword) {
      try {
        const loggedIn = await this.login(this.configEmail, this.configPassword);
        await this.persistTokens(loggedIn.accessToken, loggedIn.refreshToken);
        return loggedIn.accessToken;
      } catch (error) {
        throw new Error(
          `Authentication failed using config.json credentials from uns.email/uns.password: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    }

    throw new Error(
      "No access token available. AuthClient reads config.json and expected valid uns.email and uns.password.",
    );
  }

  private static isExpired(token: string, skewSeconds = 30): boolean {
    try {
      const decoded: any = jwt.decode(token);
      if (!decoded || typeof decoded !== "object" || !decoded.exp) return true;
      const now = Math.floor(Date.now() / 1000);
      return decoded.exp <= now + skewSeconds;
    } catch {
      return true;
    }
  }

  private static endpoint(base: string, tail: string): string {
    // base ends without trailing slash; tail must not start with slash
    const b = base.replace(/\/$/, "");
    const t = tail.replace(/^\//, "");
    return `${b}/${t}`;
  }

  private static async fetchWithTimeout(url: string, init: RequestInit = {}, timeoutMs = 10_000): Promise<Response> {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const resp = await fetch(url, { ...init, signal: controller.signal });
      return resp;
    } finally {
      clearTimeout(id);
    }
  }

  private static extractRefreshCookie(resp: Response): string | null {
    const anyHeaders: any = resp.headers as any;
    const getSetCookie = typeof anyHeaders.getSetCookie === "function" ? anyHeaders.getSetCookie.bind(anyHeaders) : null;
    const candidates: string[] = getSetCookie ? getSetCookie() : (resp.headers.get("set-cookie") ? [resp.headers.get("set-cookie") as string] : []);
    for (const header of candidates) {
      const match = header.match(/(?:^|;\s*)(?:RefreshToken|rt)=([^;]+)/i);
      if (match) return match[1];
    }
    return null;
  }

  private async login(email: string, password: string): Promise<{ accessToken: string; refreshToken: string; }> {
    const url = AuthClient.endpoint(this.restBase, "auth/login");
    const resp = await AuthClient.fetchWithTimeout(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password }),
    }, 12_000);

    if (!resp.ok) {
      throw new Error(`Login failed: ${resp.status} ${resp.statusText}`);
    }
    const data = (await resp.json()) as LoginResponse;
    const refreshToken = AuthClient.extractRefreshCookie(resp);
    if (!data?.accessToken || !refreshToken) {
      throw new Error("Login response missing tokens");
    }
    return { accessToken: data.accessToken, refreshToken };
  }

  private async refresh(refreshToken: string): Promise<{ accessToken: string; refreshToken: string; }> {
    const url = AuthClient.endpoint(this.restBase, "auth/refresh");
    let lastError: Error | undefined;
    for (const cookieName of ["RefreshToken", "rt"]) {
      const resp = await AuthClient.fetchWithTimeout(url, {
        method: "POST",
        headers: {
          "Cookie": `${cookieName}=${refreshToken}`,
        },
      }, 8_000);

      if (!resp.ok) {
        lastError = new Error(`Refresh failed: ${resp.status} ${resp.statusText}`);
        continue;
      }
      const data = (await resp.json()) as LoginResponse;
      const newRefreshToken = AuthClient.extractRefreshCookie(resp) || refreshToken;
      if (!data?.accessToken) throw new Error("Refresh response missing accessToken");
      return { accessToken: data.accessToken, refreshToken: newRefreshToken };
    }

    throw lastError ?? new Error("Refresh failed.");
  }

  private async persistTokens(accessToken: string, refreshToken: string): Promise<void> {
    await this.store.set("accessToken", accessToken);
    await this.store.set("refreshToken", refreshToken);
  }
}
