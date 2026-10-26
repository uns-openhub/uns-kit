import { describe, expect, it } from "vitest";
import { processGroupBackupLimits, processGroupStateBackupSchema } from "../../packages/uns-core/src/runtime/process-group-backup-contract.js";
const state = {schemaVersion:1,state:"included",rootSha256:"a".repeat(64),journals:1,bytes:1024,sha256:"b".repeat(64)};
describe("shared bounded journal backup coverage",()=>{
 it("supports existing archives and active plus retained generations within the SDK budget",()=>{
  expect(processGroupBackupLimits.maximumJournals).toBe(32);
  for(const journals of [1,9,10,32]) expect(processGroupStateBackupSchema.parse({...state,journals}).journals).toBe(journals);
 });
 it.each([-1,0,33,1.5,Infinity])("rejects invalid included count %s",journals=>{
  expect(processGroupStateBackupSchema.safeParse({...state,journals}).success).toBe(false);
 });
 it("retains byte bounds, distinct empty coverage and rejects extra unencrypted identity fields",()=>{
  expect(processGroupStateBackupSchema.safeParse({...state,bytes:processGroupBackupLimits.maximumCapsuleBytes+1}).success).toBe(false);
  expect(processGroupStateBackupSchema.parse({...state,state:"empty",journals:0}).state).toBe("empty");
  expect(processGroupStateBackupSchema.safeParse({...state,state:"empty"}).success).toBe(false);
  expect(processGroupStateBackupSchema.safeParse({...state,profileNames:["private-name"]}).success).toBe(false);
  expect(processGroupBackupLimits.maximumSnapshotBytes).toBe(512*1024*1024);
 });
});
