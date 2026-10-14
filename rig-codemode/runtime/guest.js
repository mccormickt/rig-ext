import { newVariant, newQuickJSWASMModuleFromVariant } from "quickjs-emscripten-core";
import variant from "@jitl/quickjs-ng-wasmfile-release-sync";
import wasmModule from "./quickjs.wasm";
import prelude from "./prelude.js";
import { discover, describe } from "./discovery.js";

const MAX_TRANSPORT = 16 * 1024 * 1024;
const MAX_INTERRUPTS = 10000;
const MAX_JOBS = 100000;
const plain = (message) => ({ message, line: null, column: null, stack: null });
const charSize = (char) => { const point = char.codePointAt(0); return point < 128 ? 1 : point < 2048 ? 2 : point < 65536 ? 3 : 4; };
const size = (text) => { let bytes = 0; for (const char of text) bytes += charSize(char); return bytes; };
const stringSize = (text) => {
  let bytes = 2;
  for (const char of text) {
    const point = char.codePointAt(0);
    bytes += char === '"' || char === "\\" || "\b\t\n\f\r".includes(char) ? 2
      : point < 32 || (point >= 0xd800 && point <= 0xdfff) ? 6 : charSize(char);
  }
  return bytes;
};
const wireSize = (value) => {
  if (typeof value === "string") return stringSize(value);
  if (value === null || typeof value !== "object") return JSON.stringify(value).length;
  if (Array.isArray(value)) return 2 + Math.max(0, value.length - 1) + value.reduce((n, v) => n + wireSize(v), 0);
  const fields = Object.entries(value);
  return 2 + Math.max(0, fields.length - 1) + fields.reduce((n, [key, v]) => n + stringSize(key) + 1 + wireSize(v), 0);
};
const cut = (text, limit) => {
  let result = "", length = 0;
  for (const char of text) { const n = size(char); if (length + n > limit) break; result += char; length += n; }
  return result;
};
const equal = (a, b) => {
  if (a === b) return true;
  if (!a || !b || typeof a !== "object" || typeof b !== "object") return false;
  const ak = Object.keys(a).sort(), bk = Object.keys(b).sort();
  return ak.length === bk.length && ak.every((k, i) => k === bk[i] && equal(a[k], b[k]));
};

async function exchange(request) {
  if (request.version !== 1) throw new Error("unsupported replay protocol");
  for (const value of [request.budget.work, request.budget.bytes, ...Object.values(request.limits).filter((v) => typeof v === "number")])
    if (!Number.isSafeInteger(value) || value <= 0) throw new Error("unsafe numeric limit");
  let work = 1, interrupts = 0, jobs = 0, interrupted = false, transportExceeded = false;
  const responseLimit = Math.min(MAX_TRANSPORT, request.budget.bytes);
  let retainedBytes = wireSize({ version: 1, batches: [], usage: { work: 1, response_bytes: 0, elapsed: { secs: 0, nanos: 0 } } });
  const checkTransport = (extra) => {
    if (transportExceeded || extra > responseLimit - retainedBytes) {
      transportExceeded = true;
      throw { budget: true, message: "response exceeds transport limit" };
    }
  };
  const charge = (extra) => { checkTransport(extra); retainedBytes += extra; };
  checkTransport(0);
  const qjs = await newQuickJSWASMModuleFromVariant(newVariant(variant, { wasmModule }));
  const runtime = qjs.newRuntime();
  runtime.setMemoryLimit(request.limits.memory_bytes);
  runtime.setMaxStackSize(64 * 1024);
  runtime.setInterruptHandler(() => {
    work++;
    return interrupted ||= transportExceeded || ++interrupts > MAX_INTERRUPTS || work > request.budget.work;
  });
  const context = runtime.newContext();
  const handles = [], pending = new Map(), batches = [];
  const output = { text: "", truncated: false };
  let ordinal = 0, overBudget = 0, events = [];
  const keep = (handle) => { handles.push(handle); return handle; };
  const diagnostic = (error) => {
    const budget = Math.min(request.limits.message_bytes, request.limits.output_bytes, 8192);
    let message = "error", stack = "";
    const read = (key, limit) => {
      const handle = context.getProp(error, key);
      try { return cut(guestString(handle), limit); } finally { handle.dispose(); }
    };
    try {
      if (context.typeof(error) === "object") {
        const name = read("name", Math.floor(budget / 4));
        message = name && name !== "Error" ? name + ": " : "";
        message = cut(message, budget) + read("message", Math.max(0, budget - size(message)));
        stack = read("stack", Math.max(0, budget - size(message)));
      } else message = cut(guestString(error), budget);
    } catch (_) {}
    const match = /script:(\d+)(?::(\d+))?/.exec(stack);
    return { message, line: match ? Math.max(1, Number(match[1]) - 1) : null,
      column: match && match[2] ? Number(match[2]) : null, stack: stack || null };
  };
  const unwrap = (result) => {
    if (result.error) {
      const error = result.error;
      try { throw interrupted ? { ...plain("the script exceeded its work limit"), budget: true } : diagnostic(error); }
      finally { error.dispose(); }
    }
    return result.value;
  };
  const json = keep(context.getProp(context.global, "JSON"));
  const stringify = keep(context.getProp(json, "stringify"));
  const parse = keep(context.getProp(json, "parse"));
  const string = keep(context.getProp(context.global, "String"));
  const serialize = (value) => {
    const handle = unwrap(context.callFunction(stringify, context.undefined, value));
    try {
      if (context.typeof(handle) === "undefined") return undefined;
      const length = context.getProp(handle, "length");
      try { checkTransport(context.getNumber(length)); } finally { length.dispose(); }
      return context.getString(handle);
    }
    finally { handle.dispose(); }
  };
  const guestString = (value) => {
    const converted = unwrap(context.callFunction(string, context.undefined, value));
    try { return JSON.parse(serialize(converted)); } finally { converted.dispose(); }
  };
  const fromJson = (text) => {
    const handle = context.newString(text);
    try { return unwrap(context.callFunction(parse, context.undefined, handle)); }
    finally { handle.dispose(); }
  };
  const toGuest = (value) => {
    const text = JSON.stringify(value);
    if (size(text) > request.limits.message_bytes) throw new RangeError("discovery result exceeds the byte or nesting limit");
    let depth = 0, quoted = false, escaped = false;
    for (const char of text) {
      if (quoted) { if (escaped) escaped = false; else if (char === "\\") escaped = true; else if (char === '"') quoted = false; }
      else if (char === '"') quoted = true;
      else if (char === "[" || char === "{") { if (++depth > 64) throw new RangeError("discovery result exceeds the byte or nesting limit"); }
      else if (char === "]" || char === "}") depth--;
    }
    return fromJson(text);
  };
  const guestError = (properties) => {
    const handle = context.newError("");
    for (const [key, value] of Object.entries(properties)) {
      const property = fromJson(JSON.stringify(value));
      context.setProp(handle, key, property);
      property.dispose();
    }
    return handle;
  };
  const toolError = (error) => guestError({ name: "CodeModeToolError", message: error.message, kind: error.kind, status: error.status, ...(error.tool ? { tool: error.tool } : {}) });
  let primary;
  try {
    const host = keep(context.newObject());
    const callback = (name, fn) => {
      const handle = context.newFunction(name, (...args) => {
        try { checkTransport(0); return fn(...args); }
        catch (error) { return { error: guestError({ name: error instanceof RangeError ? "RangeError" : "Error", message: error.message || "guest bridge failed" }) }; }
      });
      context.setProp(host, name, handle); handle.dispose();
    };
    callback("names", () => fromJson(JSON.stringify(request.catalog.map((entry) => entry.name))));
    callback("invoke", (nameHandle, args, rawHandle) => {
      const name = guestString(nameHandle), raw = context.getNumber(rawHandle) !== 0;
      const promise = context.newPromise();
      const event = (value) => {
        try { charge(wireSize(value) + (events.length ? 1 : 0)); }
        catch (error) { promise.dispose(); throw error; }
        events.push(value);
      };
      const reject = (kind, message, status = "rejected", id) => {
        if (id !== undefined) event({ Rejected: { ordinal: id, name, kind } });
        const error = toolError({ kind, message, status, tool: name });
        const result = promise.handle.dup();
        promise.reject(error); error.dispose(); promise.dispose();
        return result;
      };
      if (ordinal >= request.limits.max_calls) {
        overBudget = Math.min(0xffffffff, overBudget + 1);
        return reject("other", `call budget of ${request.limits.max_calls} tool calls is exhausted`);
      }
      const id = ordinal++;
      let argumentsJson;
      try { argumentsJson = serialize(args); } catch (_) {}
      if (argumentsJson === undefined || size(argumentsJson) > request.limits.message_bytes) {
        if (argumentsJson === undefined) return reject("invalid_args", "tool arguments must be a JSON-serializable value", "rejected", id);
        return reject("invalid_args", `tool arguments are ${size(argumentsJson)} bytes; the limit is ${request.limits.message_bytes} bytes`, "size", id);
      }
      event({ Call: { ordinal: id, name, arguments: argumentsJson, raw } });
      pending.set(id, promise);
      return promise.handle.dup();
    });
    callback("text", (value) => {
      const isString = context.typeof(value) === "string";
      const text = isString ? guestString(value) : serialize(value) ?? "undefined";
      const remaining = Math.max(0, request.limits.output_bytes - size(output.text));
      if (!isString && size(text) >= remaining)
        throw new RangeError(`text() value is ${size(text)} bytes but only ${Math.max(0, remaining - 1)} bytes of the ${request.limits.output_bytes} byte output budget remain`);
      if (!output.truncated) {
        const appended = size(text) < remaining ? text + "\n" : cut(text, remaining);
        checkTransport(stringSize(output.text) + stringSize(appended) - 2);
        output.text += appended;
        if (size(text) >= remaining) output.truncated = true;
      }
    });
    callback("describeTool", (nameHandle) => {
      const name = guestString(nameHandle);
      if (size(name) > 128) throw new RangeError("tool name exceeds 128 bytes");
      const entry = request.catalog.find((entry) => entry.name === name);
      return entry ? toGuest(describe(entry)) : context.null;
    });
    callback("searchTools", (query, limit) => toGuest(discover(request.catalog, guestString(query), !limit || context.typeof(limit) === "undefined" ? undefined : context.getNumber(limit))));
    context.setProp(context.global, "__host", host);
    unwrap(context.evalCode(prelude, "prelude")).dispose();
    unwrap(context.evalCode(`delete globalThis.Date; delete globalThis.WeakRef; delete globalThis.FinalizationRegistry;
      Object.defineProperty(Math, "random", { value: () => { throw new Error("Math.random is unavailable in code mode"); }, writable: false, configurable: false });`, "restrictions")).dispose();
    let root, failure;
    try { root = keep(unwrap(context.evalCode(`(async () => {\n${request.code}\n})()`, "script"))); }
    catch (error) { failure = error; }
    for (let index = 0; ; index++) {
      try {
        while (runtime.hasPendingJob()) {
          if (++jobs > MAX_JOBS || ++work > request.budget.work) { interrupted = true; throw plain("the script exceeded its job or work limit"); }
          const result = runtime.executePendingJobs(1);
          if (result.error) unwrap(result);
        }
      } catch (error) { failure = error; }
      let boundary;
      const done = (status, returned, diagnostic) => ({ Done: { status, output: { ...output }, returned, diagnostic, over_budget_calls: overBudget } });
      if (failure || interrupted) boundary = done(interrupted ? "timed_out" : "script_error", null, failure || plain("the script exceeded its work limit"));
      else {
        const promise = context.getPromiseState(root);
        if (promise.type === "pending") boundary = pending.size ? { Idle: { output: { ...output } } } : done("stalled", null, plain("the script awaited a promise that no tool call or pending job can settle"));
        else {
          let returned = null, error = null;
          try {
            if (promise.type === "rejected") error = diagnostic(promise.error);
            else {
              const text = serialize(promise.value);
              if (text !== undefined) {
                if (size(text) > request.limits.message_bytes) throw plain(`the return value is ${size(text)} bytes; the limit is ${request.limits.message_bytes} bytes`);
                returned = text;
              }
            }
          } catch (failure) { error = failure; }
          finally { (promise.error || promise.value)?.dispose(); }
          boundary = done(interrupted ? "timed_out" : error ? "script_error" : "completed", returned, error);
        }
      }
      if (transportExceeded) throw { budget: true, message: "response exceeds transport limit" };
      charge(wireSize({ events: [], boundary }) + (batches.length ? 1 : 0));
      const batch = { events, boundary }; events = [];
      batches.push(batch);
      if (index === request.prefix.length) break;
      const expected = request.prefix[index];
      if (!equal(batch, expected.batch) || !boundary.Idle) throw new Error("replay prefix mismatch");
      const deferred = pending.get(expected.reply.ordinal);
      if (!deferred) throw new Error("unknown or already settled reply target");
      pending.delete(expected.reply.ordinal);
      try {
        const payload = expected.reply.payload;
        const value = payload.Resolve !== undefined ? fromJson(payload.Resolve) : toolError(payload.Reject);
        try { if (payload.Resolve !== undefined) deferred.resolve(value); else deferred.reject(value); }
        finally { value.dispose(); }
      } finally { deferred.dispose(); }
    }
    charge(String(work).length - 1);
    return { version: 1, batches, usage: { work, response_bytes: 0, elapsed: { secs: 0, nanos: 0 } } };
  } catch (error) { primary = error; throw error;
  } finally {
    let cleanup;
    const dispose = (value) => { try { value.dispose(); } catch (error) { cleanup ||= error; } };
    for (const deferred of pending.values()) dispose(deferred);
    for (const handle of handles.reverse()) if (handle.alive) dispose(handle);
    dispose(context); dispose(runtime);
    if (!primary && cleanup) throw cleanup;
  }
}

async function readBounded(body, limit) {
  if (!body) throw new Error("missing request body");
  const reader = body.getReader(), chunks = [];
  let length = 0;
  try {
    while (true) {
      const { done, value } = await reader.read(); if (done) break;
      if (value.byteLength > limit - length) throw new Error("request exceeds transport limit");
      length += value.byteLength; chunks.push(value);
    }
  } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
}

export default {
  async fetch(request) {
    try {
      const input = JSON.parse(await readBounded(request.body, MAX_TRANSPORT));
      const response = JSON.stringify(await exchange(input));
      if (size(response) > Math.min(MAX_TRANSPORT, input.budget.bytes)) throw new Error("response exceeds transport limit");
      return new Response(response, { headers: { "Content-Type": "application/json" } });
    } catch (error) { return new Response("replay exchange failed", { status: error.budget ? 429 : 422 }); }
  },
};
