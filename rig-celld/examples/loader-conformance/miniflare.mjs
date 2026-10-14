// Pass the audited tooling directory containing node_modules as argument 1.
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { copyFile, mkdir } from 'node:fs/promises';

const tooling = resolve(process.argv[2]);
const { build } = await import(pathToFileURL(resolve(tooling, 'node_modules/esbuild/lib/main.js')));
const { Miniflare } = await import(pathToFileURL(resolve(tooling, 'node_modules/miniflare/dist/src/index.js')));
await mkdir('build/miniflare', { recursive: true });
await build({
  entryPoints: ['entry.mjs'], bundle: true, format: 'esm', platform: 'neutral',
  conditions: ['workerd', 'browser'], external: ['cloudflare:*', '*.wasm'],
  outfile: 'build/miniflare/entry.mjs'
});
await copyFile('build/index_bg.wasm', 'build/miniflare/index_bg.wasm');
const mf = new Miniflare({
  host: '127.0.0.1', port: Number(process.argv[3] ?? 9877),
  name: 'loader-conformance', compatibilityDate: '2026-07-01',
  modules: true, scriptPath: 'build/miniflare/entry.mjs',
  modulesRules: [{ type: 'CompiledWasm', include: ['**/*.wasm'] }],
  workerLoaders: { LOADER: {} },
  serviceBindings: { GATEWAY: { name: 'loader-conformance', entrypoint: 'Gateway' } }
});
console.log(`Loader conformance: ${await mf.ready}`);
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, async () => { await mf.dispose(); process.exit(0); });
}
