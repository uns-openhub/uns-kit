export function openManagedOutputRoot(
  directory: string,
  root: string | undefined,
): { rootId: string; relative: string; schema: "2"; close(): void } | undefined;
export function validateOutputRow(row: unknown): {
  seq: number;
  id: string;
  topic: string;
  message: string;
  options: { qos: 1 | 2; retain: boolean };
  bytes: number;
};
export function outputStateLimits(
  value: unknown,
): Readonly<{
  schemaVersion: 1;
  maximumQueues: 32;
  maximumRecordsPerQueue: 100000;
  maximumManifestBytes: 262144;
  maximumSnapshotBytes: 536870912;
  maximumCapsuleBytes: 1073741824;
}>;
export function runOutputStateProgram(args: string[]): Promise<unknown>;
export function validateOutputSnapshot(directory: string, limits: unknown): { queues: unknown[]; rootId: string };

export function outputStateRelative(root: string, directory: string): string;

export function outputStateMain(args?: string[]): Promise<void>;

export function inspectManagedOutputQueue(root: string, directory: string, identity: string, limits: unknown, requireStopped?: boolean): {
  rootId: string; relative: string; identity: string; records: number; bytes: number; exists: boolean; ownerActive:boolean;
};
