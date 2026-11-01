import { describe, it, expect } from 'vitest';
import { inputFingerprint, uniqueInputBoundary } from '../packages/uns-core/src/runtime/input-queue-boundary.js';
import { ThrottledSubscriber } from '../packages/uns-core/src/uns-mqtt/throttled-queue.js';
const key=(message:string,topic='input')=>inputFingerprint({topic,message});
const tick=()=>new Promise(resolve=>setTimeout(resolve,5));
describe('bounded shared input frontier',()=>{
  it('recognizes a short common suffix without needing pre-subscription source history',()=>{
    expect(uniqueInputBoundary(['old1','old2','new'],['new','next'])).toBe(1);
    expect(uniqueInputBoundary(['old1','a','b'],['a','b','next'])).toBe(2);
  });
  it('never skips from equal values on another topic',()=>{
    expect(()=>uniqueInputBoundary([key('same','a')],[key('same','b')])).toThrow(/missing/);
  });
  it.each([
    [['a','b'],[]], [['a','b'],['next']], [['same','same'],['same']], [['a','same'],['same','same']],
  ])('blocks a missing or ambiguous frontier %j %j',(source,target)=>{
    expect(()=>uniqueInputBoundary(source,target)).toThrow(/missing or ambiguous/);
  });
  it('keeps a silent source active until a real matching input becomes available',async()=>{
    const seen:string[]=[];
    const source=new ThrottledSubscriber(0,async(_t,m)=>{seen.push(m)},false,undefined,'unrelated-feed',true);
    const target=new ThrottledSubscriber(0,async()=>{},false,undefined,'unrelated-feed',false);
    source.enqueue('input','old1');source.enqueue('input','old2');await tick();
    expect(source.canResumeInput(target.getInputPreview())).toBe(false);expect(source.getState()).toBe(true);
    source.enqueue('input','fresh');target.enqueue('input','fresh');await tick();
    expect(source.canResumeInput(target.getInputPreview())).toBe(true);expect(seen).toEqual(['old1','old2','fresh']);
  });
  it('moves accepted progress exactly once and preserves the new owner queue on recheck',async()=>{
    const received:string[]=[];
    const source=new ThrottledSubscriber(0,async()=>{},false,undefined,'batched-reader',true);
    for(const m of ['old1','old2','shared'])source.enqueue('feed',m);await tick();
    const target=new ThrottledSubscriber(0,async(_t,m)=>{received.push(m)},false,undefined,'batched-reader',false);
    for(const m of ['shared','next','next2'])target.enqueue('feed',m);
    expect(source.canResumeInput(target.getInputPreview())).toBe(true);
    const cursor=await source.becomePassive(3,true);expect(cursor.batchSize).toBe(3);
    target.prepareResume(cursor);target.prepareResume(cursor);target.activatePreparedResume();await tick();
    expect(received).toEqual(['next','next2']);expect(source.getState()).toBe(false);
  });
  it('invalidates a prior prepared frontier when the next review fails',()=>{
    const target=new ThrottledSubscriber(0,async()=>{},false,undefined,'reader',false);target.enqueue('input','one');
    target.prepareResume({batchSize:1,referenceHash:'a'.repeat(64),processedTail:[key('one')]});
    expect(()=>target.prepareResume({batchSize:1,referenceHash:'a'.repeat(64),processedTail:[key('missing')]})).toThrow();
    expect(()=>target.activatePreparedResume()).toThrow(/not prepared/);expect(target.getState()).toBe(false);
  });
  it('bounds non-consuming previews while actual activation reviews the whole buffer',()=>{
    const target=new ThrottledSubscriber(0,async()=>{},false,undefined,'reader',false);
    target.enqueue('input','anchor');for(let i=0;i<150;i++)target.enqueue('input',String(i));target.enqueue('input','anchor');
    expect(target.getInputPreview()).toHaveLength(100);
    expect(()=>target.prepareResume({batchSize:1,referenceHash:'a'.repeat(64),processedTail:[key('anchor')]})).toThrow(/ambiguous/);
  });
});
