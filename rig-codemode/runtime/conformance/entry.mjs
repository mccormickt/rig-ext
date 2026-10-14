import rust from './build/worker/shim.mjs';
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === '/clock') {
      const now = Date.now, timeout = globalThis.setTimeout;
      const started = now(), samples = [];
      let frozen = started, cpuCost = 0;
      Date.now = () => { samples.push(frozen - started); return frozen; };
      globalThis.setTimeout = (callback, delay, ...args) => timeout(() => { frozen = Math.max(now(), started + cpuCost); callback(...args); }, delay);
      const loader = { load() { return { getEntrypoint() { return { fetch() {
        cpuCost += 10;
        return new Response(JSON.stringify({ version: 1, batches: [{ events: [], boundary: { Done: {
          status: 'completed', output: { text: '', truncated: false }, returned: '1', diagnostic: null, over_budget_calls: 0,
        } } }], usage: { work: 1, response_bytes: 0, elapsed: { secs: 0, nanos: 0 } } }));
      } }; } }; } };
      try {
        const response = await new rust(ctx, { LOADER: loader }).fetch(request);
        return Response.json({ ...await response.json(), clock_read_delays: samples });
      }
      finally { Date.now = now; globalThis.setTimeout = timeout; }
    }
    if (url.pathname === '/measure') {
      const exchanges = [];
      const loader = { load(config) {
        const owner = env.LOADER.load(config);
        return { getEntrypoint() {
          const entrypoint = owner.getEntrypoint();
          return { async fetch(request) {
            const q = (await request.clone().arrayBuffer()).byteLength;
            const response = await entrypoint.fetch(request);
            const s = (await response.clone().arrayBuffer()).byteLength;
            exchanges.push({ request: q, response: s });
            return response;
          } };
        }, dispose() { if (typeof owner.dispose === 'function') owner.dispose(); } };
      } };
      const response = await new rust(ctx, { LOADER: loader }).fetch(request);
      return Response.json({ ...await response.json(), exchanges });
    }
    if (url.pathname === '/fault') {
      const kind = url.searchParams.get('kind');
      const loader = { load() { return { getEntrypoint() { return { fetch() {
        if (kind === 'hang') return new Promise(() => {});
        if (kind === 'body') return new Response(new ReadableStream({ start() {} }));
        return new Response('invalid JSON');
      } }; } }; } };
      return new rust(ctx, { LOADER: loader }).fetch(request);
    }
    return new rust(ctx, env).fetch(request);
  }
};
