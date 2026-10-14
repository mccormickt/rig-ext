import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";

const url = process.argv[2] || "http://127.0.0.1:9890";
const limits = { source_bytes: 65536, memory_bytes: 8 * 1024 * 1024, wall_time: { secs: 10, nanos: 0 },
  max_calls: 64, max_in_flight: 8, message_bytes: 65536, output_bytes: 65536 };
const catalog = [{ name: "agent", namespace: "delegate", description: "Delegate a prompt", presentation: "inline",
  input_schema: { type: "object", properties: { prompt: { type: "string" } }, required: ["prompt"] } }];
const input = (code, extra = {}) => ({ code, catalog, limits, admitted: [], history: [], ...extra });
const call = (ordinal, prompt, raw = false) => ({ ordinal, name: "agent", arguments: { prompt }, raw });
const settled = (call, answer) => ({ call, answer });
const normalize = (value) => {
  const result = structuredClone(value);
  // Engine-specific stacks and wording are not a cross-engine history contract.
  if (result.step?.Done?.diagnostic) result.step.Done.diagnostic = { present: true };
  return result;
};
const worker = async (body, endpoint = "/") => {
  const response = await fetch(new URL(endpoint, url), { method: "POST", body: JSON.stringify(body) });
  assert.equal(response.status, 200, await response.clone().text());
  return response.json();
};
const compare = async (body) => {
  const native = spawnSync("target/debug/native-replay", [], { input: JSON.stringify(body) + "\n", encoding: "utf8" });
  assert.equal(native.status, 0, native.stderr);
  const expected = JSON.parse(native.stdout);
  const actual = await worker(body);
  assert.deepEqual(normalize(actual), normalize(expected), JSON.stringify({ body, actual, expected }));
  return actual;
};
let groups = 0;
async function test(name, fn) { await fn(); groups++; console.log(`PASS ${name}`); }

await test("deep promises and reconstructed output", async () => {
  const body = input(`text('once'); let p=Promise.resolve(0);for(let i=0;i<300;i++)p=p.then(n=>n+1);text(await p);return await tools.agent({prompt:'p'});`);
  const first = await compare(body);
  assert.deepEqual(first.step.Calls, [call(0, "p")]);
  body.admitted = first.step.Calls; body.history = [settled(call(0, "p"), "answer")];
  const terminal = (await compare(body)).step.Done;
  assert.equal(terminal.output.text, "once\n300\n"); assert.equal(terminal.returned, "answer");
});
await test("asymmetric invalid/valid race admits valid before local raw failure", async () => {
  const body = input(`return await Promise.race([tools.agent.raw({prompt:9}),tools.agent({prompt:'valid'})]);`);
  const initial = await compare(body);
  assert.deepEqual(initial.step.Calls, [call(1, "valid")]);
  body.admitted = initial.step.Calls;
  const local = (await compare(body)).step.Done;
  assert.equal(local.returned.status, "error");
  body.history = [settled(call(1, "valid"), "valid-winner-if-order-is-wrong")];
  const drained = (await compare(body)).step.Done;
  assert.equal(drained.returned.status, "error");
  assert.equal(drained.calls[1].delivery, "discarded");
});
await test("sparse history and continuation admits new call, drains loser", async () => {
  const body = input(`const winner=await Promise.race([tools.agent({prompt:'slow'}),tools.agent({prompt:'fast'})]);await Promise.resolve().then(()=>Promise.resolve());return await tools.agent({prompt:winner});`, {
    admitted: [call(0, "slow"), call(1, "fast")], history: [settled(call(1, "fast"), "fast-result")],
  });
  const pending = await compare(body);
  assert.deepEqual(pending.step.Calls, [call(0, "slow"), call(2, "fast-result")]);
  body.admitted.push(call(2, "fast-result")); body.history.push(settled(call(2, "fast-result"), "final"));
  assert.equal((await compare(body)).step.Done.returned, "final");
  body.history.push(settled(call(0, "slow"), "late"));
  const terminal = (await compare(body)).step.Done;
  assert.equal(terminal.calls[0].delivery, "discarded");
  assert.equal(terminal.returned, "final");
});
await test("toJSON reentrant arrival order and rejected outer serialization", async () => {
  const body = input(`return await tools.agent({toJSON(){void tools.agent({prompt:'inner'});return {prompt:'outer'}}});`);
  assert.deepEqual((await compare(body)).step.Calls, [call(0, "outer"), call(1, "inner")]);
  body.admitted = [call(0, "outer"), call(1, "inner")];
  body.history = [settled(call(0, "outer"), "outer-answer"), settled(call(1, "inner"), "inner-answer")];
  assert.equal((await compare(body)).step.Done.calls[1].delivery, "discarded");
  await compare(input(`try{await tools.agent({toJSON(){void tools.agent({prompt:'inner'});throw Error('bad')}})}catch(e){text(e.kind)};return 1;`));
});
await test("new unawaited calls and divergent history are rejected", async () => {
  assert.match((await compare(input(`void tools.agent({prompt:'p'});return 1`))).error, /await/);
  assert.match((await compare(input(`return await tools.agent({prompt:'different'})`, { admitted: [call(0, "original")] }))).error, /diverged/);
});
await test("guest helpers, typed failure and deterministic restrictions", async () => {
  const body = input(`return [searchTools('delegate prompt'),describeTool('agent'),describeTool('absent'),Object.getPrototypeOf(tools)===null,typeof Date,typeof WeakRef,typeof FinalizationRegistry];`);
  const result = await compare(body);
  assert.equal(result.step.Done.returned[0][0].name, "agent");
  await compare(input(`try{Math.random()}catch(e){return e.message}`));
  await compare(input(`try{await tools.agent({prompt:9})}catch(e){return [e.kind,e.status,e.tool]}`));
});
await test("embedded NUL text, lookup, query bounds and tool replies", async () => {
  const result = await compare(input(`text('a\\0b');return [describeTool('agent\\0suffix'),searchTools('prompt\\0')];`));
  assert.equal(result.step.Done.output.text, "a\0b\n");
  assert.equal(result.step.Done.returned[0], null);
  await compare(input(`try{searchTools('prompt\\0'+'x'.repeat(4096))}catch(e){return e.message}`));
  const body = input(`return await tools.agent({prompt:'p'})`, { admitted: [call(0, "p")], history: [settled(call(0, "p"), "a\0b")] });
  assert.equal((await compare(body)).step.Done.returned, "a\0b");
});
await test("UTF-8 output, returned depth and message bounds", async () => {
  const result = await compare(input(`text('héllo');text('ignored')`, { limits: { ...limits, output_bytes: 2 } }));
  assert.deepEqual(result.step.Done.output, { text: "h", truncated: true });
  await compare(input(`text({x:1})`, { limits: { ...limits, output_bytes: 2 } }));
  await compare(input(`return 'x'.repeat(100)`, { limits: { ...limits, message_bytes: 32 } }));
  for (const depth of [65, 124, 127]) {
    const result = await compare(input(`let value=0;for(let i=0;i<${depth};i++)value=[value];return value`));
    assert.equal(result.step.Done.status, "completed");
  }
});
await test("invalid guest JSON returns are Done script errors, not protocol errors", async () => {
  for (const code of [String.raw`return "\uD800";`,
    `let value=0;for(let i=0;i<128;i++)value=[value];return value;`]) {
    const result = await compare(input(`text('before');${code}`));
    assert.equal(result.step.Done.status, "script_error");
    assert.equal(result.step.Done.returned, undefined);
    assert.equal(result.step.Done.output.text, "before\n");
    assert.deepEqual(result.step.Done.diagnostic, {
      message: "the return value is not valid JSON",
    });
  }
});
await test("fresh interpreter and no Workers capabilities via constructor escape", async () => {
  await compare(input(`globalThis.marker=123;return typeof marker`));
  const result = await compare(input(`return [typeof marker,typeof fetch,typeof crypto,typeof caches,typeof process,typeof require,typeof Worker,typeof __host,Function('return typeof fetch')()];`));
  assert.deepEqual(result.step.Done.returned, Array(9).fill("undefined"));
});
await test("aggregate byte/work/time and unsafe integer budgets fail closed", async () => {
  const budget = 12000;
  const measured = await worker(input(`/*${"x".repeat(10000)}*/return 1;`, { budget_bytes: budget }), "/measure");
  assert.equal(measured.step?.Done?.returned, 1, JSON.stringify(measured));
  const { request: q, response: s } = measured.exchanges[0];
  assert.ok(q + s < budget && budget < 2 * q, JSON.stringify({ q, s, budget }));
  console.log(`BYTE REGRESSION Q=${q} S=${s} budget=${budget}; Q+S < budget < 2Q`);
  for (const extra of [{ budget_bytes: 1 }, { budget_work: 1 }, { budget_ms: 0 }, { budget_work: 2 ** 53 }])
    assert.match((await worker(input("return 1", extra))).error, /budget/);
  for (const endpoint of ["/fault?kind=hang", "/fault?kind=body"]) {
    const started = performance.now();
    assert.match((await worker(input("return 1", { budget_ms: 20 }), endpoint)).error, /budget/);
    assert.ok(performance.now() - started < 2000);
  }
  assert.match((await worker(input("return 1"), "/fault?kind=malformed")).error, /protocol/);
});
await test("frozen CPU clock includes full exchange through the post-decode checkpoint", async () => {
  const result = await worker(input("return 1", { budget_ms: 200 }), "/clock");
  assert.equal(result.step?.Done?.returned, 1, JSON.stringify(result));
  assert.ok(result.clock_read_delays.at(-1) >= 10, JSON.stringify(result));
  assert.match((await worker(input("return 1", { budget_ms: 5 }), "/clock")).error, /budget/);
});
await test("CPU/job storms stop; repeated fresh controls still pass", async () => {
  for (const code of ["while(true){}", "await new Promise(()=>{const f=()=>Promise.resolve().then(f);f()})", "function f(){return f()} f()", "const a=[];while(true)a.push('x'.repeat(65536))"]) {
    const result = await worker(input(code));
    assert.ok(result.error || ["timed_out", "script_error"].includes(result.step?.Done?.status), JSON.stringify(result));
    assert.equal((await worker(input("return 'fresh-control'"))).step.Done.returned, "fresh-control");
  }
});
console.log(`${groups} real Rust native/Worker conformance groups passed: ${url}`);
