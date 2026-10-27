import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { Miniflare } from "miniflare";

const port = Number(process.argv[2]);
if (!Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error("Usage: node miniflare.mjs <port>");
}
const source = await readFile(new URL("worker.js", import.meta.url), "utf8");
const wasm = await readFile(new URL("quickjs.wasm", import.meta.url));
const host = `import wasm from './engine.bytes';
export default { async fetch(request, env) {
  if (new URL(request.url).pathname === '/health') return new Response('ready');
  const worker = env.LOADER.load({ compatibilityDate: '2026-07-01', mainModule: 'worker.js',
    modules: { 'worker.js': ${JSON.stringify(source)}, 'quickjs.wasm': { wasm } },
    globalOutbound: null, env: {}, limits: { cpuMs: 100, subRequests: 0 } });
  try { return await worker.getEntrypoint().fetch(request); }
  finally { if (typeof worker.dispose === 'function') worker.dispose(); }
} };`;
const mf = new Miniflare({ host: "127.0.0.1", port, compatibilityDate: "2026-07-01",
  modulesRoot: fileURLToPath(new URL(".", import.meta.url)),
  modules: [
    { type: "ESModule", path: fileURLToPath(new URL("probe-host.js", import.meta.url)), contents: host },
    { type: "Data", path: fileURLToPath(new URL("engine.bytes", import.meta.url)), contents: wasm },
  ], workerLoaders: { LOADER: {} } });
let stopping = false;
for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal, async () => {
  if (stopping) return;
  stopping = true;
  await mf.dispose();
  process.exit(0);
});
console.log(`Worker replay adapter: ${await mf.ready}`);
