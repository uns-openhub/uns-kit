import logger from "@uns-kit/core/logger.js";

import { type PostgresDatabaseConfig, requireResolvedDatabaseString } from "../schema.js";
import type { DatabaseAdapter, DatabaseQueryResult } from "../types.js";
import type { CompiledSqlStatement } from "../types.js";

type PgPool = {
  query<T = Record<string, unknown>>(
    text: string,
    values?: unknown[],
  ): Promise<{
    rows: T[];
    rowCount?: number | null;
  }>;
  end(): Promise<void>;
  on(event: "error", listener: (error: Error) => void): void;
};

type PgClient = {
  connect(): Promise<void>;
  query<T = Record<string, unknown>>(
    text: string,
    values?: unknown[],
  ): Promise<{
    rows: T[];
    rowCount?: number | null;
  }>;
  end(): Promise<void>;
  on(event: "error" | "end", listener: (error?: Error) => void): void;
};

function buildPgSsl(config: PostgresDatabaseConfig): unknown {
  if (config.ssl === undefined) {
    return undefined;
  }

  if (typeof config.ssl === "boolean") {
    return config.ssl;
  }

  return {
    rejectUnauthorized: config.ssl.rejectUnauthorized,
    ca: config.ssl.ca === undefined ? undefined : requireResolvedDatabaseString(config.ssl.ca, "ssl.ca"),
    cert: config.ssl.cert === undefined ? undefined : requireResolvedDatabaseString(config.ssl.cert, "ssl.cert"),
    key: config.ssl.key === undefined ? undefined : requireResolvedDatabaseString(config.ssl.key, "ssl.key"),
    servername: config.ssl.servername === undefined ? undefined : requireResolvedDatabaseString(config.ssl.servername, "ssl.servername"),
  };
}

export async function createPgAdapter(config: PostgresDatabaseConfig): Promise<DatabaseAdapter> {
  let pgModule: {
    Pool: new (options: Record<string, unknown>) => PgPool;
    Client: new (options: Record<string, unknown>) => PgClient;
  };

  try {
    const importedModule = await import("pg");
    pgModule = ((importedModule as { default?: typeof importedModule }).default ?? importedModule) as typeof pgModule;
  } catch (error) {
    throw new Error("The 'pg' package is required for dialect 'pg'. Install it with `pnpm add pg`.", { cause: error as Error });
  }

  const connectionOptions = {
    host: requireResolvedDatabaseString(config.host, "host"),
    port: config.port,
    database: requireResolvedDatabaseString(config.database, "database"),
    user: requireResolvedDatabaseString(config.user, "user"),
    password: config.password === undefined ? undefined : requireResolvedDatabaseString(config.password, "password"),
    ssl: buildPgSsl(config),
    application_name: config.applicationName,
    statement_timeout: config.statementTimeoutMs,
    connectionTimeoutMillis: config.connectionTimeoutMs,
    idleTimeoutMillis: config.idleTimeoutMs,
    max: config.maxPoolSize,
    min: config.minPoolSize,
  };

  if (config.usePool !== false) {
    const pool = new pgModule.Pool(connectionOptions);
    pool.on("error", (error) => {
      logger.error("uns-database - PostgreSQL pool idle client error", error);
    });

    return {
      dialect: "pg",
      async query<T = Record<string, unknown>>(statement: CompiledSqlStatement): Promise<DatabaseQueryResult<T>> {
        const result = await pool.query<T>(statement.text, statement.values as unknown[]);
        return {
          rows: result.rows,
          rowCount: result.rowCount ?? result.rows.length,
        };
      },
      async execute(statement: CompiledSqlStatement) {
        const result = await pool.query(statement.text, statement.values as unknown[]);
        return {
          rowCount: result.rowCount ?? 0,
        };
      },
      close() {
        return pool.end();
      },
    };
  }

  let client: PgClient | null = null;
  let clientPromise: Promise<PgClient> | null = null;

  const detachCachedClient = (activeClient: PgClient) => {
    if (client !== activeClient) {
      return false;
    }

    client = null;
    return true;
  };

  const handleClientConnectionError = (activeClient: PgClient, error: unknown) => {
    if (!detachCachedClient(activeClient)) {
      return;
    }

    logger.error("uns-database - PostgreSQL client connection error", error);
    void activeClient.end().catch((): undefined => undefined);
  };

  const handleClientConnectionEnd = (activeClient: PgClient) => {
    if (!detachCachedClient(activeClient)) {
      return;
    }

    logger.warn("uns-database - PostgreSQL client connection ended");
  };

  const attachClientListeners = (activeClient: PgClient) => {
    activeClient.on("error", (error) => {
      handleClientConnectionError(activeClient, error);
    });
    activeClient.on("end", () => {
      handleClientConnectionEnd(activeClient);
    });
  };

  const getClient = async () => {
    if (client) {
      return client;
    }

    if (!clientPromise) {
      clientPromise = (async () => {
        const nextClient = new pgModule.Client(connectionOptions);
        attachClientListeners(nextClient);
        await nextClient.connect();
        client = nextClient;
        return nextClient;
      })().finally(() => {
        clientPromise = null;
      });
    }

    return clientPromise;
  };

  return {
    dialect: "pg",
    async query<T = Record<string, unknown>>(statement: CompiledSqlStatement): Promise<DatabaseQueryResult<T>> {
      const activeClient = await getClient();
      const result = await activeClient.query<T>(statement.text, statement.values as unknown[]);
      return {
        rows: result.rows,
        rowCount: result.rowCount ?? result.rows.length,
      };
    },
    async execute(statement: CompiledSqlStatement) {
      const activeClient = await getClient();
      const result = await activeClient.query(statement.text, statement.values as unknown[]);
      return {
        rowCount: result.rowCount ?? 0,
      };
    },
    async close() {
      const activeClient = client;
      const pendingClientPromise = clientPromise;
      client = null;
      clientPromise = null;

      if (activeClient) {
        await activeClient.end();
        return;
      }

      if (!pendingClientPromise) {
        return;
      }

      const pendingClient = await pendingClientPromise.catch((): null => null);
      if (pendingClient) {
        await pendingClient.end();
      }
    },
  };
}
