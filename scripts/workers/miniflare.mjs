// Run the packaged Worker with SQLite storage retained across process restarts.
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { Miniflare } from "miniflare";

const rootPath = resolve(process.argv[2] ?? "rig-durable/examples/durable-object");
const config = JSON.parse(await readFile(resolve(rootPath, "wrangler.jsonc"), "utf8"));
const sqliteClasses = new Set(config.migrations?.flatMap(m => m.new_sqlite_classes ?? []));
const mf = new Miniflare({
  host: "127.0.0.1",
  port: Number(process.argv[3] ?? 9877),
  rootPath,
  scriptPath: config.main,
  modules: true,
  compatibilityDate: config.compatibility_date,
  compatibilityFlags: config.compatibility_flags ?? [],
  bindings: config.vars ?? {},
  modulesRules: [
    { type: "ESModule", include: ["**/*.js", "**/*.mjs"] },
    { type: "CompiledWasm", include: ["**/*.wasm"] },
    ...(config.rules ?? []).map(rule => ({
      type: rule.type, include: rule.globs, fallthrough: rule.fallthrough ?? false,
    })),
  ],
  workerLoaders: Object.fromEntries((config.worker_loaders ?? []).map(b => [b.binding, {}])),
  durableObjects: Object.fromEntries((config.durable_objects?.bindings ?? []).map(b => [
    b.name, { className: b.class_name, useSQLite: sqliteClasses.has(b.class_name) },
  ])),
  durableObjectsPersist: resolve(rootPath, ".workerd/state"),
});

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.once(signal, async () => {
    await mf.dispose();
    process.exit(0);
  });
}
console.log(`workerd ready: ${await mf.ready}`);
