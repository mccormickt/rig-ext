import { build } from "esbuild";
import { readFile, writeFile, copyFile } from "node:fs/promises";
import { createHash } from "node:crypto";

await build({ entryPoints: ["guest.js"], bundle: true, format: "esm", platform: "browser",
  conditions: ["workerd", "browser"], external: ["./quickjs.wasm"], outfile: "worker.js",
  plugins: [{ name: "guest-source", setup(build) {
    build.onLoad({ filter: /\/prelude\.js$/ }, async ({ path }) => ({ contents: await readFile(path, "utf8"), loader: "text" }));
    build.onLoad({ filter: /quickjs-emscripten-core\/dist\/chunk-.*\.mjs$/ }, async ({ path }) => {
      const source = await readFile(path, "utf8");
      if (!source.includes("newTypedArray(kind,length){")) return;
      const pattern = /newTypedArray\(kind,length\)\{.*?return typedArray\.set\(zeros\),new Lifetime\(\{typedArray,ptr\},void 0,value=>this.module\._free\(value\.ptr\)\)\}/g;
      if ([...source.matchAll(pattern)].length !== 1) throw new Error("QuickJS wrapper patch no longer matches exactly once");
      const replacement = "newTypedArray(kind,length){let module=this.module,numBytes=length*kind.BYTES_PER_ELEMENT,ptr=module._malloc(numBytes);new kind(module.HEAPU8.buffer,ptr,length).fill(0);return new Lifetime({get typedArray(){return new kind(module.HEAPU8.buffer,ptr,length)},ptr},void 0,value=>module._free(value.ptr))}";
      return { contents: source.replace(pattern, replacement), loader: "js" };
    });
  } }], legalComments: "inline" });
await copyFile("node_modules/@jitl/quickjs-ng-wasmfile-release-sync/dist/emscripten-module.wasm", "quickjs.wasm");
const files = {};
for (const name of ["worker.js", "quickjs.wasm"]) {
  const data = await readFile(name);
  files[name] = { bytes: data.length, sha256: createHash("sha256").update(data).digest("hex") };
}
await writeFile("fingerprint.json", JSON.stringify({ protocol: 1, harness: 1, engine: "quickjs-ng-0.12.1",
  package: "0.32.0", wrapper_patch: "refresh-typed-array-after-memory-growth-v1", files }, null, 2) + "\n");
const identity = `worker-replay-v1:quickjs-ng-0.12.1:${files["worker.js"].sha256}:${files["quickjs.wasm"].sha256}`;
await writeFile("identity.txt", identity + "\n");
console.log(JSON.stringify(files));
