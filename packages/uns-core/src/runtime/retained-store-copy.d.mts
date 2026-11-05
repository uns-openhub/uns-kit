export type RetainedCopyFingerprint = { digest: string; bytes: number; entries: number };
export function inspectRetainedCopy(root: string, kind: "file" | "directory", limits: unknown): RetainedCopyFingerprint;
export function applyRetainedCopy(plan: unknown, direction: "adopt" | "return", limits: unknown, afterMove?: (id: string) => void, onFile?: (file: string) => void): void;
