import { z } from "zod";

import { hostPlaceholderSchema, secretValueSchema } from "./placeholders.js";

// Preserve the Zod 3 contract, where .int() accepted finite integers outside
// Number's safe-integer range as well.
const integerSchema = () => z.number().multipleOf(1);
const positiveInt = integerSchema().positive();
const sqlDirSchema = z.string().min(1).optional();
const nonEmptySecretValueSchema = secretValueSchema.refine(
  (value) => typeof value !== "string" || value.trim().length > 0,
  "Value must not be empty",
);
// Database endpoints are often stored alongside credentials in Infisical. Keep
// host-specific placeholders (inline/external/system) while allowing the same
// env/Infisical references accepted for other resolved string values.
const databaseHostSchema = z.union([hostPlaceholderSchema, secretValueSchema]);

const sslModeSchema = z.union([
  z.boolean(),
  z
    .object({
      rejectUnauthorized: z.boolean().default(true),
      ca: secretValueSchema.optional(),
      cert: secretValueSchema.optional(),
      key: secretValueSchema.optional(),
      servername: secretValueSchema.optional(),
    })
    .strict(),
]);

export const postgresDatabaseSchema = z
  .object({
    dialect: z.literal("pg"),
    host: databaseHostSchema,
    port: positiveInt.default(5432),
    database: nonEmptySecretValueSchema,
    user: nonEmptySecretValueSchema,
    password: secretValueSchema.optional(),
    usePool: z.boolean().default(true),
    ssl: sslModeSchema.optional(),
    sqlDir: sqlDirSchema,
    applicationName: z.string().min(1).optional(),
    statementTimeoutMs: integerSchema().nonnegative().optional(),
    connectionTimeoutMs: integerSchema().nonnegative().optional(),
    idleTimeoutMs: integerSchema().nonnegative().optional(),
    maxPoolSize: positiveInt.optional(),
    minPoolSize: integerSchema().nonnegative().optional(),
  })
  .strict();

export const sqliteDatabaseSchema = z
  .object({
    dialect: z.literal("sqlite"),
    filename: z.string().min(1),
    sqlDir: sqlDirSchema,
    readonly: z.boolean().optional(),
    fileMustExist: z.boolean().optional(),
    timeoutMs: integerSchema().nonnegative().optional(),
  })
  .strict();

const oracleDatabaseBaseSchema = z
  .object({
    dialect: z.literal("oracle"),
    user: nonEmptySecretValueSchema,
    password: secretValueSchema.optional(),
    usePool: z.boolean().default(true),
    connectString: nonEmptySecretValueSchema.optional(),
    host: databaseHostSchema.optional(),
    port: positiveInt.default(1521),
    serviceName: nonEmptySecretValueSchema.optional(),
    sid: nonEmptySecretValueSchema.optional(),
    sqlDir: sqlDirSchema,
    poolMin: integerSchema().nonnegative().optional(),
    poolMax: positiveInt.optional(),
    poolIncrement: positiveInt.optional(),
    stmtCacheSize: positiveInt.optional(),
  })
  .strict();

export const oracleDatabaseSchema = oracleDatabaseBaseSchema.superRefine((value: z.infer<typeof oracleDatabaseBaseSchema>, ctx) => {
  if (!value.connectString && !value.host) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: "oracle connection requires either connectString or host",
      path: ["connectString"],
    });
  }

  if (!value.connectString && !value.serviceName && !value.sid) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: "oracle connection requires serviceName or sid when connectString is omitted",
      path: ["serviceName"],
    });
  }
});

export const databaseConnectionSchema = z.union([postgresDatabaseSchema, sqliteDatabaseSchema, oracleDatabaseSchema]);

export const databasesConfigSchema = z.record(z.string().min(1), databaseConnectionSchema);

export const databaseProjectExtrasSchema = z.object({
  databases: databasesConfigSchema,
});

/**
 * Database adapters run after ConfigFile.loadConfig() resolves env/Infisical and
 * host placeholders. Rejecting an unresolved placeholder here avoids sending a
 * configuration object to a database driver when a caller bypasses that flow.
 */
export function requireResolvedDatabaseString(value: unknown, field: string): string {
  if (typeof value === "string" && value.trim().length > 0) return value;
  throw new Error(`Database configuration '${field}' must be resolved to a non-empty string before connecting.`);
}

export type PostgresDatabaseConfig = z.infer<typeof postgresDatabaseSchema>;
export type SqliteDatabaseConfig = z.infer<typeof sqliteDatabaseSchema>;
export type OracleDatabaseConfig = z.infer<typeof oracleDatabaseSchema>;
export type DatabaseConnectionConfig = z.infer<typeof databaseConnectionSchema>;
export type DatabasesConfig = z.infer<typeof databasesConfigSchema>;
export type DatabaseProjectExtras = z.infer<typeof databaseProjectExtrasSchema>;
