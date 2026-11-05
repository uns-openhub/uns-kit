export function rootLease(root: string, exclusive?: boolean, restoring?: boolean): () => void;
export function maintenanceLease(directory: string): () => void;
export function runtimeLease(binding: unknown): { close: () => void };
export function captureRetained(
  root: string,
  destination: string,
  mode: string,
  limits: unknown,
): { state: string; stores: number; files: number; payloadBytes: number };
export function restoreRetained(
  source: string,
  root: string,
  backupDigest: string,
  limits: unknown,
  afterPublish?: (id: string) => void,
): { state: string; stores: number };
export function available(root: string): void;
export function owned(path: string, directory?: boolean): import("node:fs").Stats;
export function sha(path: string): string;
export function sync(path: string): void;
export function relative(path: string): boolean;
export function checkLimit(limits: unknown): void;
export function verifyRestoredRetained(source:string,root:string,backupDigest:string,limits:unknown):{state:string;stores:number;backupDigest:string;manifestDigest:string;root:string};
export function verifyRetainedRecoveryRoot(root:string,backupDigest:string,manifestDigest:string,limits:unknown):{state:string;stores:number;backupDigest:string;manifestDigest:string;root:string};
