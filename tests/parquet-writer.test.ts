import fs from "fs/promises";
import { asyncBufferFromFile, parquetReadObjects } from "hyparquet";
import os from "os";
import path from "path";
import { afterEach, describe, expect, it } from "vitest";

import type { DataCatalogSchemaRegistration } from "../packages/uns-api/src/api-interfaces.js";
import { buildParquetSchemaFromCatalogSchema, writeSchemaRowsToParquet, writeSchemaRowsToParquetStream } from "../packages/uns-api/src/parquet.js";

const temporaryDirectories: string[] = [];

const schema: DataCatalogSchemaRegistration = {
  id: "machine-events",
  title: "Machine events",
  fields: [
    { name: "name", type: "string", required: true },
    { name: "count", type: "integer", required: true },
    { name: "temperature", type: "number" },
    { name: "running", type: "boolean" },
    { name: "recordedAt", type: "string", format: "date-time", required: true },
    { name: "metadata", type: "object" },
  ],
};

afterEach(async () => {
  await Promise.all(temporaryDirectories.splice(0).map((directory) => fs.rm(directory, { recursive: true, force: true })));
});

describe("schema-driven Parquet writer", () => {
  it("maps supported catalog fields to typed nullable columns", () => {
    expect(buildParquetSchemaFromCatalogSchema(schema)).toEqual([
      { name: "name", type: "STRING", nullable: false },
      { name: "count", type: "INT64", nullable: false },
      { name: "temperature", type: "DOUBLE", nullable: true },
      { name: "running", type: "BOOLEAN", nullable: true },
      { name: "recordedAt", type: "TIMESTAMP", nullable: false },
    ]);
  });

  it("writes a readable Parquet file without the vulnerable Thrift dependency", async () => {
    const outputDir = await fs.mkdtemp(path.join(os.tmpdir(), "uns-kit-parquet-"));
    temporaryDirectories.push(outputDir);
    const recordedAt = new Date("2026-08-05T10:15:00.000Z");

    const filePath = await writeSchemaRowsToParquet({
      outputDir,
      fileName: "machine-events.parquet",
      schema,
      rows: [
        {
          name: "press-01",
          count: 12,
          temperature: 42.5,
          running: true,
          recordedAt,
          metadata: { ignored: true },
        },
      ],
    });

    expect(filePath).toBe(path.join(outputDir, "machine-events.parquet"));
    const rows = await parquetReadObjects({
      file: await asyncBufferFromFile(filePath!),
    });
    expect(rows).toEqual([
      {
        name: "press-01",
        count: 12n,
        temperature: 42.5,
        running: true,
        recordedAt,
      },
    ]);
  });

  it("streams many rows without requiring a source array", async () => {
    const outputDir = await fs.mkdtemp(path.join(os.tmpdir(), "uns-kit-parquet-"));
    temporaryDirectories.push(outputDir);
    let produced = 0;
    async function* rows() {
      for (let index = 0; index < 10_001; index++) {
        produced++;
        yield { name: `press-${index}`, count: index, recordedAt: "2026-08-05T10:15:00.000Z" };
      }
    }

    const filePath = await writeSchemaRowsToParquetStream({
      outputDir,
      fileName: "large.parquet",
      schema,
      rows: rows(),
      rowGroupSize: 128,
    });
    expect(produced).toBe(10_001);
    const result = await parquetReadObjects({ file: await asyncBufferFromFile(filePath) });
    expect(result).toHaveLength(10_001);
    expect(result[0]).toMatchObject({ name: "press-0", count: 0n });
    expect(result.at(-1)).toMatchObject({ name: "press-10000", count: 10_000n });
  });

  it("removes an incomplete file when the source fails", async () => {
    const outputDir = await fs.mkdtemp(path.join(os.tmpdir(), "uns-kit-parquet-"));
    temporaryDirectories.push(outputDir);
    async function* rows() {
      for (let index = 0; index < 130; index++) {
        yield { name: `press-${index}`, count: index, recordedAt: new Date("2026-08-05T10:15:00.000Z") };
      }
      throw new Error("source disconnected");
    }
    await expect(writeSchemaRowsToParquetStream({ outputDir, fileName: "failed.parquet", schema, rows: rows(), rowGroupSize: 64 })).rejects.toThrow(
      "source disconnected",
    );
    expect(await fs.readdir(outputDir)).toEqual([]);
  });

  it("stops on cancellation and rejects unsafe options", async () => {
    const outputDir = await fs.mkdtemp(path.join(os.tmpdir(), "uns-kit-parquet-"));
    temporaryDirectories.push(outputDir);
    const controller = new AbortController();
    async function* rows() {
      yield { name: "press-01", count: 1, recordedAt: new Date("2026-08-05T10:15:00.000Z") };
      controller.abort();
      yield { name: "press-02", count: 2, recordedAt: new Date("2026-08-05T10:15:01.000Z") };
    }
    await expect(writeSchemaRowsToParquetStream({ outputDir, schema, rows: rows(), signal: controller.signal, rowGroupSize: 1 })).rejects.toThrow();
    expect(await fs.readdir(outputDir)).toEqual([]);
    await expect(writeSchemaRowsToParquetStream({ outputDir, fileName: "../escape.parquet", schema, rows: [] })).rejects.toThrow("fileName");
    await expect(writeSchemaRowsToParquetStream({ outputDir, schema, rows: [], rowGroupSize: 0 })).rejects.toThrow("rowGroupSize");
  });
});
