import rust from './build/worker/shim.mjs';
import { WorkerEntrypoint } from 'cloudflare:workers';

// Deterministic outbound control, without external network or credentials.
export class Gateway extends WorkerEntrypoint {
  fetch(request) {
    return new Response('gateway:' + new URL(request.url).pathname);
  }
}

export default {
  async fetch(request, env, ctx) {
    if (new URL(request.url).pathname !== '/cleanup') {
      return new rust(ctx, { ...env, PARENT_MARKER: 'not-in-guest' }).fetch(request);
    }
    // Fault injection checks Rust cleanup against real local stream objects.
    // It does not stand in for the runtime Loader tests at /run.
    const params = new URL(request.url).searchParams;
    const disposal = params.get('dispose') ?? 'throw';
    const fault = params.get('fault') ?? 'overflow';
    const stats = { dispose: 0, entrypointDispose: 0, read: 0, cancel: 0, release: 0 };
    const entrypoint = {
      dispose() { ++stats.entrypointDispose; throw new Error('RPC dispose'); },
      fetch() {
        if (fault === 'invoke') throw new Error('invoke-primary');
        const stream = new ReadableStream({
          start(c) {
            if (fault === 'body') c.error(new Error('body-primary'));
            else if (fault !== 'abandon') c.enqueue(new Uint8Array(1024 * 1024));
          },
          cancel() { throw new Error('cancel-secondary'); }
        });
        const getReader = stream.getReader.bind(stream);
        stream.getReader = () => {
          const reader = getReader();
          const read = reader.read.bind(reader);
          const cancel = reader.cancel.bind(reader);
          const release = reader.releaseLock.bind(reader);
          reader.read = () => { ++stats.read; return read(); };
          reader.cancel = () => { ++stats.cancel; return cancel(); };
          reader.releaseLock = () => { ++stats.release; release(); throw new Error('release-secondary'); };
          return reader;
        };
        return new Response(stream);
      }
    };
    const stub = { getEntrypoint() { return entrypoint; } };
    if (disposal === 'getter') Object.defineProperty(stub, 'dispose', {
      get() { ++stats.dispose; throw new Error('dispose-getter-secondary'); }
    });
    if (disposal === 'throw') stub.dispose = () => { ++stats.dispose; throw new Error('dispose-call-secondary'); };
    const loader = { load(config) {
      if (Object.keys(config.env).length || config.globalOutbound !== null
          || config.limits.cpuMs !== 100 || config.limits.subRequests !== 0) {
        throw new Error('unexpected code/policy');
      }
      return stub;
    }};
    const response = await new rust(ctx, { LOADER: loader }).fetch(request);
    const result = await response.json();
    return Response.json({ ...result, stats });
  }
};
