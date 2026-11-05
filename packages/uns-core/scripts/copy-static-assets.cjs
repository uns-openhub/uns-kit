const fs = require("fs");
const path = require("path");

const protoSource = path.resolve("src/uns-grpc/uns-gateway.proto");
const protoDest = path.resolve("dist/uns-grpc/uns-gateway.proto");

if (!fs.existsSync(protoSource)) {
  console.error(`Cannot find ${protoSource}`);
  process.exit(1);
}

fs.mkdirSync(path.dirname(protoDest), { recursive: true });
fs.copyFileSync(protoSource, protoDest);

console.log(`Copied uns-gateway.proto -> ${protoDest}`);

const runtimeSnapshotSource=path.resolve("src/runtime/process-group-snapshot.mjs");
const runtimeSnapshotDest=path.resolve("dist/runtime/process-group-snapshot.mjs");
fs.mkdirSync(path.dirname(runtimeSnapshotDest),{recursive:true});
fs.copyFileSync(runtimeSnapshotSource,runtimeSnapshotDest);

fs.copyFileSync(path.resolve("src/runtime/process-group-backup-contract.json"),path.resolve("dist/runtime/process-group-backup-contract.json"));
for (const asset of ['mqtt-output-state-program.mjs','mqtt-output-state-program.d.mts','mqtt-output-state-contract.json']) {
  fs.copyFileSync(path.resolve('src/runtime',asset),path.resolve('dist/runtime',asset));
}
for (const asset of ["retained-store-job.mjs", "retained-store-job.d.mts", "retained-store-copy.mjs", "retained-store-copy.d.mts", "retained-store-storage.mjs", "retained-store-storage.d.mts", "retained-store-storage-contract.json"]) {
  fs.copyFileSync(path.resolve("src/runtime", asset), path.resolve("dist/runtime", asset));
}
fs.copyFileSync(path.resolve("../../docs/retained-store-adoption.md"), path.resolve("dist/runtime/retained-store-guide.md"));
const retainedExamples = path.resolve("dist/runtime/retained-store-examples");
fs.mkdirSync(retainedExamples, { recursive: true });
for (const name of ["measurement-worker.manifest.json", "index-worker.manifest.json"]) {
  fs.copyFileSync(path.resolve("../../examples/retained-store", name), path.join(retainedExamples, name));
}
