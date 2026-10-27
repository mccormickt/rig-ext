import { exchange } from "./build/runtime_bg.js";
import * as bindings from "./build/runtime_bg.js";
import wasmModule from "./quickjs.wasm";

const instance = new WebAssembly.Instance(wasmModule, { "./runtime_bg.js": bindings });
bindings.__wbg_set_wasm(instance.exports);

export default {
  async fetch(request) {
    const reader = request.body?.getReader();
    if (!reader) return new Response("missing request body", { status: 422 });
    const chunks = [];
    let length = 0;
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        if (value.byteLength > 16 * 1024 * 1024 - length) throw 429;
        length += value.byteLength;
        chunks.push(value);
      }
      const bytes = new Uint8Array(length);
      let offset = 0;
      for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
      const input = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
      return new Response(exchange(input), { headers: { "Content-Type": "application/json" } });
    } catch (error) {
      return new Response("replay exchange failed", { status: error === 429 ? 429 : 422 });
    } finally {
      await reader.cancel().catch(() => {});
      reader.releaseLock();
    }
  },
};
