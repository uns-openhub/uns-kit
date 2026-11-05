import fs from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { randomUUID } from "node:crypto";
import { afterEach, expect, it } from "vitest";
import { prepareRetainedStoreMove, applyRetainedStoreMove, prepareRetainedStoreReturn, retainedStoreManifestSchema, writeRetainedStoreJson } from "../packages/uns-core/src/runtime/retained-store.js";
import {prepareRetainedStoreMoveAsync,applyRetainedStoreMoveAsync,prepareRetainedStoreReturnAsync}from"../packages/uns-core/src/runtime/retained-store.js";
import { captureRetained,restoreRetained,verifyRestoredRetained,runtimeLease } from "../packages/uns-core/src/runtime/retained-store-storage.mjs";
const roots:string[]=[];
afterEach(()=>roots.splice(0).forEach(p=>fs.rmSync(p,{recursive:true,force:true})));
function fixture(){
 const base=fs.realpathSync(fs.mkdtempSync(path.join(tmpdir(),"retained-copy-")));roots.push(base);
 const source=path.join(base,"source"),root=path.join(base,"state");
 fs.mkdirSync(source,{mode:0o700});fs.mkdirSync(root,{mode:0o700});fs.mkdirSync(path.join(source,"queue"),{mode:0o700});
 for(const id of [1,2,3])fs.writeFileSync(path.join(source,"queue",id+".json"),JSON.stringify({id,value:40+id,uom:"kWh"}));
 const manifest=retainedStoreManifestSchema.parse({protocol:1,storageProtocol:1,format:"independent-journal-v1",resources:[{id:"work",path:"queue",kind:"directory",required:true},{id:"control",path:"pause.json",kind:"file",required:false,backup:"metadata"}]});
 const plan=prepareRetainedStoreMove(source,root,randomUUID(),manifest,"copy-verified");
 return{base,source,root,manifest,plan};
}
it("resumes a partial verified copy and returns current work while retaining the original",()=>{
 const f=fixture(),original=fs.statSync(path.join(f.source,"queue"));
 expect(()=>applyRetainedStoreMove(f.plan,"adopt",undefined,()=>{throw Error("copy interrupted")})).toThrow("interrupted");
 expect(fs.existsSync(path.join(f.plan.directory,"work"))).toBe(false);
 expect(fs.statSync(path.join(f.source,"queue")).ino).toBe(original.ino);
 applyRetainedStoreMove(f.plan,"adopt");applyRetainedStoreMove(f.plan,"adopt");
 fs.writeFileSync(path.join(f.plan.directory,"work","4.json"),JSON.stringify({id:4,value:44,uom:"kWh"}));
 fs.writeFileSync(path.join(f.plan.directory,"control"),JSON.stringify({paused:true,revision:7}));
 const back=prepareRetainedStoreReturn(f.plan);
 expect(()=>applyRetainedStoreMove(back,"return",undefined,()=>{throw Error("return interrupted")})).toThrow("interrupted");
 const saved=path.join(f.source,".retained-original-"+f.plan.storeId+"-work");
 expect(fs.readdirSync(saved)).toHaveLength(3);
 applyRetainedStoreMove(back,"return");applyRetainedStoreMove(back,"return");
 expect(fs.readdirSync(path.join(f.source,"queue"))).toHaveLength(4);
 expect(JSON.parse(fs.readFileSync(path.join(f.source,"pause.json"),"utf8"))).toEqual({paused:true,revision:7});
 expect(fs.readdirSync(path.join(f.plan.directory,"work"))).toHaveLength(4);
 expect(fs.readdirSync(saved)).toHaveLength(3);
});
it("can abort an unpublished partial copy without losing or replacing the source",()=>{
 const f=fixture();
 expect(()=>applyRetainedStoreMove(f.plan,"adopt",undefined,()=>{throw Error("interrupted")})).toThrow();
 applyRetainedStoreMove(prepareRetainedStoreReturn(f.plan),"return");
 expect(fs.readdirSync(path.join(f.source,"queue"))).toHaveLength(3);
 expect(fs.readdirSync(f.plan.directory)).toHaveLength(0);
});
it("keeps the host event loop responsive while worker jobs copy and return data",async()=>{
 const f=fixture();let ticks=0;const timer=setInterval(()=>ticks++,1);
 try{
  const plan=await prepareRetainedStoreMoveAsync(f.source,f.root,randomUUID(),f.manifest,"copy-verified");
  await applyRetainedStoreMoveAsync(plan,"adopt");
  fs.writeFileSync(path.join(plan.directory,"work","4.json"),"advanced");
  const back=await prepareRetainedStoreReturnAsync(plan);
  await applyRetainedStoreMoveAsync(back,"return");
  expect(ticks).toBeGreaterThan(0);expect(fs.readFileSync(path.join(f.source,"queue","4.json"),"utf8")).toBe("advanced");
 }finally{clearInterval(timer);}
});
it("refuses changed source and unknown staging bytes instead of publishing",()=>{
 const f=fixture();fs.writeFileSync(path.join(f.source,"queue","1.json"),"changed");
 expect(()=>applyRetainedStoreMove(f.plan,"adopt")).toThrow("changed after review");
 expect(fs.existsSync(path.join(f.plan.directory,"work"))).toBe(false);
 const g=fixture();
 expect(()=>applyRetainedStoreMove(g.plan,"adopt",undefined,()=>{throw Error("interrupted")})).toThrow();
 fs.writeFileSync(path.join(g.plan.directory,".retained-copy-work","unknown"),"keep");
 expect(()=>applyRetainedStoreMove(g.plan,"adopt")).toThrow("Unexpected");
 expect(()=>applyRetainedStoreMove(prepareRetainedStoreReturn(g.plan),"return")).toThrow("Unexpected");
 expect(fs.readFileSync(path.join(g.plan.directory,".retained-copy-work","unknown"),"utf8")).toBe("keep");
});
it("backs up retired copies as inert data and verifies recovered work before activation",()=>{
 const f=fixture();applyRetainedStoreMove(f.plan,"adopt");
 const b={protocol:1,storeId:f.plan.storeId,generation:f.plan.generation,operationId:randomUUID(),directory:f.plan.directory,format:f.manifest.format,configurationDigest:"a".repeat(64),owner:{rttNode:"unrelated-worker",version:"1.0.0",instanceId:"one"},maintenance:true};
 writeRetainedStoreJson(path.join(f.plan.directory,"store.json"),b);writeRetainedStoreJson(path.join(f.plan.directory,"store-resources.json"),f.manifest);
 applyRetainedStoreMove(prepareRetainedStoreReturn(f.plan),"return");
 writeRetainedStoreJson(path.join(f.plan.directory,"store-returned.json"),{operationId:b.operationId,generation:b.generation});
 const snapshot=path.join(f.base,"snapshot");fs.mkdirSync(snapshot,{mode:0o700});
 const limits=JSON.parse(fs.readFileSync(new URL("../packages/uns-core/src/runtime/retained-store-storage-contract.json",import.meta.url),"utf8"));
 expect(captureRetained(f.root,snapshot,"include",limits).state).toBe("included");
 fs.renameSync(f.root,f.root+".original");fs.mkdirSync(f.root,{mode:0o700});
 restoreRetained(snapshot,f.root,"b".repeat(64),limits);
 expect(verifyRestoredRetained(snapshot,f.root,"b".repeat(64),limits).state).toBe("verified");
 expect(()=>runtimeLease(b)).toThrow("retired");
 fs.writeFileSync(path.join(f.plan.directory,"work","1.json"),"advanced");
 expect(()=>verifyRestoredRetained(snapshot,f.root,"b".repeat(64),limits)).toThrow("payload changed");
 restoreRetained(snapshot,f.root,"b".repeat(64),limits);
 expect(fs.readFileSync(path.join(f.plan.directory,"work","1.json"),"utf8")).toBe("advanced");
});
