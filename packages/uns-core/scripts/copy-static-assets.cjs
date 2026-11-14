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
fs.copyFileSync(path.resolve("../../docs/catalog-publication-access.md"), path.resolve("dist/runtime/service-access-guide.md"));
const serviceExamples = path.resolve("dist/runtime/service-access-examples");
fs.rmSync(serviceExamples, { recursive: true, force: true });
fs.mkdirSync(serviceExamples, { recursive: true });
fs.copyFileSync(path.resolve("../../examples/service-access/README.md"), path.join(serviceExamples, "README.md"));
// Explicit assets only: never bundle a developer's ignored config, .env or node_modules.
for (const [name, module] of [["background-reader", "reader.mjs"], ["catalog-summary", "offer.mjs"]]) {
  for (const asset of ["package.json", "service-access.json", "config-example.json", "README.md", "build.mjs", ".gitignore", "src/index.mjs", `src/${module}`]) {
    const target = path.join(serviceExamples, name, asset);
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.copyFileSync(path.resolve("../../examples/service-access", name, asset), target);
  }
}
const retainedExamples = path.resolve("dist/runtime/retained-store-examples");
fs.mkdirSync(retainedExamples, { recursive: true });
for (const name of ["measurement-worker.manifest.json", "index-worker.manifest.json"]) {
  fs.copyFileSync(path.resolve("../../examples/retained-store", name), path.join(retainedExamples, name));
}
