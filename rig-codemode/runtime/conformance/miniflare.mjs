import { build } from "esbuild";
import { Miniflare } from "miniflare";
import { copyFile, mkdir } from "node:fs/promises";
await mkdir("build/miniflare", { recursive: true });
await build({ entryPoints: ["entry.mjs"], bundle: true, format: "esm", platform: "neutral",
  conditions: ["workerd", "browser"], external: ["cloudflare:*", "*.wasm"], outfile: "build/miniflare/entry.mjs" });
await copyFile("build/index_bg.wasm", "build/miniflare/index_bg.wasm");
const mf = new Miniflare({ port: Number(process.argv[2] || 9890), compatibilityDate: "2026-07-01",
  modules: true, scriptPath: "build/miniflare/entry.mjs",
  modulesRules: [{ type: "CompiledWasm", include: ["**/*.wasm"] }], workerLoaders: { LOADER: {} } });
console.log(`Rust replay fixture: ${await mf.ready}`);
for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal, async () => { await mf.dispose(); process.exit(0); });
