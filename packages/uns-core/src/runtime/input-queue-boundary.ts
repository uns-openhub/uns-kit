import { createHash } from "node:crypto";
import { z } from "zod";

export const inputPreviewSchema = z.array(z.string().regex(/^[a-f0-9]{64}$/)).max(100);
export const inputCursorSchema = z.strictObject({
  batchSize: z.number().int().min(0).max(100),
  referenceHash: z.string().regex(/^[a-f0-9]{64}$/),
  processedTail: inputPreviewSchema.optional(),
});
export type InputQueueCursor = z.infer<typeof inputCursorSchema>;
export function inputFingerprint(item: { topic: string; message: string }): string {
  return createHash("sha256").update(JSON.stringify({ topic: item.topic, message: item.message })).digest("hex");
}

/** Find an ordered common suffix. Neither a timestamp nor equal values alone authorize skipping. */
export function uniqueInputBoundary(processedTail: readonly string[], buffered: readonly string[]): number {
  if (processedTail.length === 0) return 0;
  for (let size = Math.min(processedTail.length, buffered.length); size > 0; size--) {
    const tail = processedTail.slice(-size);
    const matches = (items: readonly string[]) => {
      const ends: number[] = [];
      for (let start = 0; start <= items.length - size; start++)
        if (tail.every((value, offset) => items[start + offset] === value)) ends.push(start + size);
      return ends;
    };
    if (matches(processedTail).length !== 1) continue;
    const ends = matches(buffered);
    if (ends.length === 1) return ends[0];
  }
  throw Error("MQTT input overlap is missing or ambiguous; successor stays passive");
}
