import { type OracleDatabaseConfig, requireResolvedDatabaseString } from "../schema.js";
import type { DatabaseAdapter, DatabaseQueryResult } from "../types.js";
import type { CompiledSqlStatement } from "../types.js";

type OraclePool = {
  getConnection(): Promise<OracleConnection>;
  close(closeMode?: number): Promise<void>;
};

type OracleConnection = {
  execute<T = Record<string, unknown>>(
    sqlText: string,
    bindParams?: Record<string, unknown>,
    options?: Record<string, unknown>,
  ): Promise<{
    rows?: T[];
    rowsAffected?: number;
  }>;
  close(): Promise<void>;
};

type OracleModule = {
  OUT_FORMAT_OBJECT: number;
  createPool(options: Record<string, unknown>): Promise<OraclePool>;
  getConnection(options: Record<string, unknown>): Promise<OracleConnection>;
};

function buildOracleConnectString(config: OracleDatabaseConfig): string {
  if (config.connectString) {
    return requireResolvedDatabaseString(config.connectString, "connectString");
  }

  const host = requireResolvedDatabaseString(config.host, "host");
  return config.serviceName
    ? `${host}:${config.port}/${requireResolvedDatabaseString(config.serviceName, "serviceName")}`
    : `${host}:${config.port}/${requireResolvedDatabaseString(config.sid, "sid")}`;
}

export async function createOracleAdapter(config: OracleDatabaseConfig): Promise<DatabaseAdapter> {
  let oracleModule: OracleModule;

  try {
    const importedModule = await import("oracledb");
    oracleModule = ((importedModule as { default?: OracleModule }).default ?? importedModule) as OracleModule;
  } catch (error) {
    throw new Error("The 'oracledb' package is required for dialect 'oracle'. Install it with `pnpm add oracledb`.", { cause: error as Error });
  }

  const connectionOptions = {
    user: requireResolvedDatabaseString(config.user, "user"),
    password: config.password === undefined ? undefined : requireResolvedDatabaseString(config.password, "password"),
    connectString: buildOracleConnectString(config),
    poolMin: config.poolMin,
    poolMax: config.poolMax,
    poolIncrement: config.poolIncrement,
    stmtCacheSize: config.stmtCacheSize,
  };

  if (config.usePool !== false) {
    const pool = await oracleModule.createPool(connectionOptions);

    return {
      dialect: "oracle",
      async query<T = Record<string, unknown>>(statement: CompiledSqlStatement): Promise<DatabaseQueryResult<T>> {
        const connection = await pool.getConnection();
        try {
          const result = await connection.execute<T>(statement.text, statement.values as Record<string, unknown>, {
            outFormat: oracleModule.OUT_FORMAT_OBJECT,
          });
          const rows = result.rows ?? [];
          return {
            rows,
            rowCount: rows.length,
          };
        } finally {
          await connection.close();
        }
      },
      async execute(statement: CompiledSqlStatement) {
        const connection = await pool.getConnection();
        try {
          const result = await connection.execute(statement.text, statement.values as Record<string, unknown>, { autoCommit: true });
          return {
            rowCount: result.rowsAffected ?? 0,
          };
        } finally {
          await connection.close();
        }
      },
      close() {
        return pool.close(0);
      },
    };
  }

  return {
    dialect: "oracle",
    async query<T = Record<string, unknown>>(statement: CompiledSqlStatement): Promise<DatabaseQueryResult<T>> {
      const connection = await oracleModule.getConnection(connectionOptions);
      try {
        const result = await connection.execute<T>(statement.text, statement.values as Record<string, unknown>, {
          outFormat: oracleModule.OUT_FORMAT_OBJECT,
        });
        const rows = result.rows ?? [];
        return {
          rows,
          rowCount: rows.length,
        };
      } finally {
        await connection.close();
      }
    },
    async execute(statement: CompiledSqlStatement) {
      const connection = await oracleModule.getConnection(connectionOptions);
      try {
        const result = await connection.execute(statement.text, statement.values as Record<string, unknown>, { autoCommit: true });
        return {
          rowCount: result.rowsAffected ?? 0,
        };
      } finally {
        await connection.close();
      }
    },
    async close() {
      // Non-pooled Oracle mode opens/closes per operation, so there is nothing to tear down.
    },
  };
}
