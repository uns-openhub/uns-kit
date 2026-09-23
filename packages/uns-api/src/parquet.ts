import logger from "@uns-kit/core/logger.js";
import { randomUUID } from "crypto";
import fs from "fs/promises";
import { type BasicType, type ColumnSource, fileWriter, parquetWriteRows } from "hyparquet-writer";
import os from "os";
import path from "path";

import type { DataCatalogSchemaRegistration } from "./api-interfaces.js";

export type CatalogParquetColumn = Omit<ColumnSource, "data">;
export type CatalogParquetRows = Iterable<Record<string, unknown>> | AsyncIterable<Record<string, unknown>>;

export type CatalogParquetWriteInput = {
  rows: CatalogParquetRows;
  schema: DataCatalogSchemaRegistration;
  outputDir?: string;
  fileName?: string;
  /** Maximum source rows buffered for one Parquet row group. */
  rowGroupSize?: number;
  signal?: AbortSignal;
};

const PARQUET_TYPE_MAP: Record<string, BasicType> = {
  string: "STRING",
  number: "DOUBLE",
  integer: "INT64",
  boolean: "BOOLEAN",
  date: "TIMESTAMP",
  "date-time": "TIMESTAMP",
};

/**
 * Write catalog rows a group at a time. The input can be a database cursor or
 * another async source; it is never collected into one array. A failed write
 * leaves no incomplete file at the requested path.
 */
export async function writeSchemaRowsToParquetStream(input: CatalogParquetWriteInput): Promise<string> {
  const outputDir = input.outputDir ?? path.join(os.tmpdir(), "uns-data-offers");
  const fileName = input.fileName ?? `${randomUUID()}.parquet`;
  if (fileName !== path.basename(fileName) || fileName === "." || fileName === "..") {
    throw new Error("Parquet fileName must be a file name without a directory.");
  }
  const rowGroupSize = input.rowGroupSize ?? 2_000;
  if (!Number.isSafeInteger(rowGroupSize) || rowGroupSize < 1 || rowGroupSize > 10_000) {
    throw new Error("Parquet rowGroupSize must be between 1 and 10,000.");
  }
  const columns = buildParquetSchemaFromCatalogSchema(input.schema);
  if (!columns.length) {
    throw new Error(`Parquet schema '${input.schema.id}' has no supported fields.`);
  }

  const filePath = path.join(outputDir, fileName);
  const incompletePath = `${filePath}.${randomUUID()}.partial`;
  await fs.mkdir(outputDir, { recursive: true });
  try {
    await parquetWriteRows({
      writer: fileWriter(incompletePath),
      rows: normalizeParquetRows(input.rows, columns, input.signal),
      columns,
      rowGroupSize,
    });
    input.signal?.throwIfAborted();
    await fs.rename(incompletePath, filePath);
    return filePath;
  } catch (error) {
    await fs.rm(incompletePath, { force: true });
    throw error;
  }
}

/** Legacy null-on-error interface retained for existing callers. */
export async function writeSchemaRowsToParquet(input: CatalogParquetWriteInput): Promise<string | null> {
  try {
    return await writeSchemaRowsToParquetStream(input);
  } catch (error) {
    logger.error("Failed to write schema-driven parquet:", error);
    return null;
  }
}

async function* normalizeParquetRows(
  rows: CatalogParquetRows,
  columns: CatalogParquetColumn[],
  signal?: AbortSignal,
): AsyncGenerator<Record<string, unknown>> {
  for await (const row of rows) {
    signal?.throwIfAborted();
    yield normalizeParquetRow(row, columns);
  }
  signal?.throwIfAborted();
}

export function buildParquetSchemaFromCatalogSchema(schema: DataCatalogSchemaRegistration): CatalogParquetColumn[] {
  const columns: CatalogParquetColumn[] = [];
  for (const field of schema.fields ?? []) {
    const parquetType = toParquetType(field.type ?? "string", field.format ?? null);
    if (!parquetType) {
      continue;
    }
    columns.push({
      name: field.name,
      type: parquetType,
      nullable: field.required !== true,
    });
  }
  return columns;
}

function toParquetType(type: string, format: string | null): BasicType | null {
  if (format && PARQUET_TYPE_MAP[format]) {
    return PARQUET_TYPE_MAP[format];
  }
  return PARQUET_TYPE_MAP[type] ?? null;
}

function normalizeParquetRow(row: Record<string, unknown>, columns: CatalogParquetColumn[]): Record<string, unknown> {
  const normalized: Record<string, unknown> = {};
  for (const column of columns) {
    const value = row[column.name];
    normalized[column.name] =
      column.type === "INT64" && typeof value === "number"
        ? BigInt(value)
        : column.type === "TIMESTAMP" && typeof value === "string"
          ? new Date(value)
          : value;
  }
  return normalized;
}
