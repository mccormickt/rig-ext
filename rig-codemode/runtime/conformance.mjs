import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { Miniflare } from "miniflare";

const source = await readFile("worker.js", "utf8");
const wasm = await readFile("quickjs.wasm");
const host = `import wasm from './engine.bytes';
export default { async fetch(request, env) {
  const worker = env.LOADER.load({ compatibilityDate: '2026-07-01', mainModule: 'worker.js',
    modules: { 'worker.js': ${JSON.stringify(source)}, 'quickjs.wasm': { wasm } },
    globalOutbound: null, env: {}, limits: { cpuMs: 100, subRequests: 0 } });
  try { return await worker.getEntrypoint().fetch(request); }
  finally { if (typeof worker.dispose === 'function') worker.dispose(); }
} };`;
const mf = new Miniflare({ compatibilityDate: "2026-07-01", modulesRoot: process.cwd(),
  modules: [{ type: "ESModule", path: "probe-host.js", contents: host }, { type: "Data", path: "engine.bytes", contents: wasm }], workerLoaders: { LOADER: {} } });
const limits = { source_bytes: 65536, memory_bytes: 8 * 1024 * 1024, wall_time: { secs: 5, nanos: 0 },
  max_calls: 64, max_in_flight: 8, message_bytes: 65536, output_bytes: 65536 };
const entry = (name, description = "echo value") => ({ name, description, input_schema: { type: "object" }, presentation: "inline" });
const request = (code, extra = {}) => ({ version: 1, code, catalog: [entry("echo"), entry("other")], limits, prefix: [],
  budget: { bytes: 16 * 1024 * 1024, work: 1000000, elapsed: { secs: 5, nanos: 0 } }, ...extra });
const send = async (input, expected = 200) => {
  const response = await mf.dispatchFetch("http://probe/exchange", { method: "POST", body: JSON.stringify(input) });
  const text = await response.text();
  assert.equal(response.status, expected, text);
  return expected === 200 ? JSON.parse(text) : text;
};
const suffix = (response) => response.batches.at(-1);
let passed = 0;
async function test(name, fn) { await fn(); passed++; console.log(`PASS ${name}`); }
try {
  await test("deep promise drain and ordered parallel calls", async () => {
    const code = `let p = Promise.resolve(0); for(let i=0;i<300;i++) p=p.then(n=>n+1); text(await p); return await Promise.all([tools.echo({n:1}),tools.other({n:2})]);`;
    const input = request(code), first = suffix(await send(input));
    assert.deepEqual(first.events.map((e) => e.Call.name), ["echo", "other"], JSON.stringify(first));
    assert.equal(first.boundary.Idle.output.text, "300\n");
    input.prefix.push({ batch: first, reply: { ordinal: 1, payload: { Resolve: '"second"' } } });
    const second = suffix(await send(input));
    assert.deepEqual(second.events, []);
    input.prefix.push({ batch: second, reply: { ordinal: 0, payload: { Resolve: '"first"' } } });
    const terminal = suffix(await send(input)).boundary.Done;
    assert.equal(terminal.output.text, "300\n");
    assert.deepEqual(JSON.parse(terminal.returned), ["first", "second"]);
  });
  await test("one reply before full continuation drain", async () => {
    const input = request(`const winner=await Promise.race([tools.echo({n:1}),tools.other({n:2})]); await Promise.resolve().then(()=>Promise.resolve()); return await tools.echo({winner});`);
    const first = suffix(await send(input));
    input.prefix.push({ batch: first, reply: { ordinal: 1, payload: { Resolve: '"other-wins"' } } });
    const next = suffix(await send(input));
    assert.deepEqual(next.events, [{ Call: { ordinal: 2, name: "echo", arguments: '{"winner":"other-wins"}', raw: false } }]);
  });
  await test("tampered prefix arguments, output, boundary and unknown targets reject", async () => {
    const input = request(`text('once'); return await tools.echo({n:7});`);
    const batch = suffix(await send(input));
    for (const mutate of [
      (x) => x.batch.events[0].Call.arguments = '{"n":8}',
      (x) => x.batch.boundary.Idle.output.text = "twice\n",
      (x) => x.batch.boundary = { Done: {} },
      (x) => x.reply.ordinal = 99,
    ]) {
      const prefix = { batch: structuredClone(batch), reply: { ordinal: 0, payload: { Resolve: "7" } } };
      mutate(prefix); await send({ ...input, prefix: [prefix] }, 422);
    }
    await send({ ...input, code: "return 1", prefix: [{ batch, reply: { ordinal: 0, payload: { Resolve: "7" } } }] }, 422);
  });
  await test("fresh state and prototype mutations do not corrupt trusted events", async () => {
    const input = request(`globalThis.secret=99; JSON.stringify=()=> '"forged"'; Object.prototype.toJSON=()=> 'forged'; return await tools.echo({n:3});`);
    const first = suffix(await send(input));
    assert.equal(first.events[0].Call.arguments, '"forged"');
    assert.equal(first.events[0].Call.name, "echo");
    assert.deepEqual(first.boundary.Idle.output, { text: "", truncated: false });
    const terminal = suffix(await send(request(`return typeof secret`))).boundary.Done;
    assert.equal(JSON.parse(terminal.returned), "undefined");
    const clean = suffix(await send(request(`return await tools.echo({n:3})`)));
    assert.equal(clean.events[0].Call.arguments, '{"n":3}');
  });
  await test("output unicode boundary and non-string bounds", async () => {
    const small = { ...limits, output_bytes: 2 };
    const terminal = suffix(await send(request(`text('héllo'); text('more');`, { limits: small }))).boundary.Done;
    assert.deepEqual(terminal.output, { text: "h", truncated: true });
    assert.equal(suffix(await send(request(`text({n:9})`, { limits: small }))).boundary.Done.status, "script_error");
    assert.equal(suffix(await send(request(`return 'x'.repeat(100)`, { limits: { ...limits, message_bytes: 32 } }))).boundary.Done.status, "script_error");
  });
  await test("embedded NUL text, exact discovery names and rejection messages", async () => {
    const done = suffix(await send(request(`text('a\\0b');return [describeTool('echo\\0junk'),searchTools('echo\\0')];`))).boundary.Done;
    assert.equal(done.output.text, "a\0b\n");
    assert.equal(JSON.parse(done.returned)[0], null);
    assert.equal(JSON.parse(done.returned)[1][0].name, "echo");
    const query = suffix(await send(request(`try{searchTools('echo\\0'+'x'.repeat(4096))}catch(e){return e.message}`))).boundary.Done;
    assert.match(JSON.parse(query.returned), /4096/);
    const input = request(`try{await tools.echo({})}catch(e){return [e.message,e.tool]}`);
    const batch = suffix(await send(input));
    input.prefix.push({ batch, reply: { ordinal: 0, payload: { Reject: { message: "a\0b", kind: "other", status: "error", tool: "echo\0suffix" } } } });
    assert.deepEqual(JSON.parse(suffix(await send(input)).boundary.Done.returned), ["a\0b", "echo\0suffix"]);
  });
  await test("aggregate events, escaping and repeated snapshots exhaust transport fatally", async () => {
    const budget = (bytes) => ({ bytes, work: 1000000, elapsed: { secs: 5, nanos: 0 } });
    await send(request(`try{await Promise.all(Array.from({length:16},()=>tools.echo({s:'x'.repeat(1024)})))}catch(e){};return 1`, { budget: budget(5000) }), 429);
    await send(request(`try{await tools.echo({s:'\\0'.repeat(128)})}catch(e){};return 1`, { budget: budget(1000) }), 429);
    const input = request(`text('x'.repeat(512));await tools.echo({});await tools.echo({});`, { budget: budget(1200) });
    const batch = suffix(await send(input));
    input.prefix.push({ batch, reply: { ordinal: 0, payload: { Resolve: "null" } } });
    await send(input, 429);
    assert.equal(JSON.parse(suffix(await send(request("return 'fresh'"))).boundary.Done.returned), "fresh");
  });
  await test("capabilities absent, constructors and eval stay in QuickJS", async () => {
    const terminal = suffix(await send(request(`return [typeof fetch,typeof WebSocket,typeof process,typeof require,typeof caches,typeof crypto,typeof Worker,typeof __host,typeof Date,typeof WeakRef,typeof FinalizationRegistry,Function('return typeof fetch')(),eval('typeof process')];`))).boundary.Done;
    assert.deepEqual(JSON.parse(terminal.returned), Array(13).fill("undefined"));
    assert.equal(suffix(await send(request(`Math.random()`))).boundary.Done.status, "script_error");
    const frozen = suffix(await send(request(`return [Object.getPrototypeOf(tools)===null,Object.isFrozen(tools),Object.isFrozen(tools.echo),Object.isFrozen(tools.echo.raw)]`))).boundary.Done;
    assert.deepEqual(JSON.parse(frozen.returned), [true, true, true, true]);
  });
  await test("CPU loop, promise job storm, heap exhaustion and stack overflow stop", async () => {
    for (const code of ["while(true){}", "await new Promise(()=>{const storm=()=>Promise.resolve().then(storm);storm()})", "const a=[];while(true)a.push('x'.repeat(65536));", "function recurse(){return recurse()} recurse();"]) {
      console.log(`CHECK bounded guest: ${code}`);
      const result = await send(request(code));
      assert.ok(["timed_out", "script_error"].includes(suffix(result).boundary.Done.status));
      assert.ok(result.usage.work > 0);
    }
  });
  await test("serialization rejection, raw call and call budget", async () => {
    const result = suffix(await send(request(`const cycle={};cycle.self=cycle; try{await tools.echo(cycle)}catch(e){text(e.kind)} return await tools.other.raw({n:8});`)));
    assert.equal(result.events[0].Rejected.kind, "invalid_args");
    assert.equal(result.events[1].Call.raw, true);
    assert.equal(result.boundary.Idle.output.text, "invalid_args\n");
    const input = request(`await tools.echo({}); try{await tools.echo({})}catch(e){return e.kind}`, { limits: { ...limits, max_calls: 1, max_in_flight: 1 } });
    const first = suffix(await send(input));
    input.prefix.push({ batch: first, reply: { ordinal: 0, payload: { Resolve: "1" } } });
    const terminal = suffix(await send(input)).boundary.Done;
    assert.equal(JSON.parse(terminal.returned), "other"); assert.equal(terminal.over_budget_calls, 1);
  });
  await test("unsafe integers and transport exhaustion reject", async () => {
    await send(request("return 1", { budget: { bytes: 1000000, work: 2 ** 53, elapsed: { secs: 1, nanos: 0 } } }), 422);
    await send(request("return 1", { budget: { bytes: 1, work: 1000, elapsed: { secs: 1, nanos: 0 } } }), 429);
  });
  console.log(`${passed} real Dynamic Worker conformance groups passed`);
} finally { await mf.dispose(); }
