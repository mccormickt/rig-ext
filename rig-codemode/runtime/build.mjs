import { build } from "esbuild";
import { readFile, writeFile, copyFile } from "node:fs/promises";
import { createHash } from "node:crypto";

await build({ entryPoints: ["entry.js"], bundle: true, format: "esm", platform: "browser",
  external: ["./quickjs.wasm"], outfile: "worker.js", legalComments: "inline" });
await copyFile("build/runtime_bg.wasm", "quickjs.wasm");
const files = {};
for (const name of ["worker.js", "quickjs.wasm"]) {
  const data = await readFile(name);
  files[name] = { bytes: data.length, sha256: createHash("sha256").update(data).digest("hex") };
}
await writeFile("fingerprint.json", JSON.stringify({ protocol: 1, engine: "quickjs-ng-0.16.2",
  rquickjs: "0.14.0", adapter: "rust-v1", files }, null, 2) + "\n");
await writeFile("identity.txt", `worker-replay-v1:quickjs-ng-0.16.2:${files["worker.js"].sha256}:${files["quickjs.wasm"].sha256}\n`);
console.log(JSON.stringify(files));
