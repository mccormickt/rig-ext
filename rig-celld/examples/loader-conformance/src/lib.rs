use rig_celld::{OutboundPolicy, WorkerCode, WorkerLimits, WorkerLoader, WorkerModule};
use serde_json::json;
use worker::wasm_bindgen::JsCast;
use worker::*;

const STATEFUL: &str = r#"
import { WorkerEntrypoint } from 'cloudflare:workers';
let count = 0;
let disposed = 0;
export default { async fetch(request, env) {
  const path = new URL(request.url).pathname;
  if (path === '/error') throw new Error('invoke-primary');
  if (path === '/empty') return new Response(null, { status: 204 });
  if (path === '/stream') {
    const chunks = ['ab', 'cde', 'f'];
    return new Response(new ReadableStream({ async pull(controller) {
      await new Promise(resolve => setTimeout(resolve, 5));
      if (chunks.length) controller.enqueue(new TextEncoder().encode(chunks.shift()));
      else controller.close();
    }}), {status: 207, headers: {'x-fixture': 'stream'}});
  }
  if (path === '/body-error') return new Response(new ReadableStream({
    start(c) { c.error(new Error('body-primary')); }
  }));
  if (path === '/huge') return new Response(new Uint8Array(1024 * 1024));
  if (path === '/nonempty') return new Response('x');
  if (path === '/env') return new Response(JSON.stringify(Object.keys(env)));
  return new Response(`${++count}:${disposed}:${await request.text()}`);
}};
export class Named extends WorkerEntrypoint {
  fetch() { return new Response(`named:${disposed}`); }
  dispose() { ++disposed; throw new Error('entrypoint dispose is RPC'); }
}
"#;

fn code(source: &str, outbound: OutboundPolicy, subrequests: u32) -> WorkerCode {
    WorkerCode {
        main_module: "guest.js".into(),
        modules: [("guest.js".into(), WorkerModule::JavaScript(source.into()))].into(),
        compatibility_date: "2026-07-01".into(),
        compatibility_flags: Vec::new(),
        outbound,
        limits: WorkerLimits {
            cpu_ms: 100,
            subrequests,
        },
    }
}

fn request(path: &str) -> Result<Request> {
    Request::new(&format!("https://guest.invalid{path}"), Method::Get)
}

fn ensure(ok: bool, label: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(Error::RustError(format!("FAIL: {label}")))
    }
}

#[event(fetch)]
async fn fetch(incoming: Request, env: Env, _ctx: Context) -> Result<Response> {
    if incoming.path() == "/cpu" || incoming.path() == "/cpu-control" {
        return cpu(&env, if incoming.path() == "/cpu" { 1 } else { 1000 }).await;
    }
    if incoming.path() == "/cleanup" {
        let loader = WorkerLoader::from_env(&env, "LOADER")?;
        let mut loaded = loader.load(code(STATEFUL, OutboundPolicy::Deny, 0))?;
        let abandon = incoming
            .url()?
            .query_pairs()
            .any(|(key, value)| key == "fault" && value == "abandon");
        let result = if abandon {
            use std::{
                future::{Future, poll_fn},
                task::Poll,
                time::Duration,
            };
            let mut read = Box::pin(loaded.fetch_bounded(None, request("/fault")?, 3));
            let mut deadline = Box::pin(Delay::from(Duration::from_millis(20)));
            let result = poll_fn(|cx| {
                if let Poll::Ready(result) = read.as_mut().poll(cx) {
                    return Poll::Ready(result);
                }
                if deadline.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Err(Error::RustError("read-abandoned".into())));
                }
                Poll::Pending
            })
            .await;
            drop(read);
            result
        } else {
            loaded.fetch_bounded(None, request("/fault")?, 3).await
        };
        loaded.dispose();
        loaded.dispose();
        ensure(loaded.entrypoint(None).is_err(), "disposed owner")?;
        drop(loaded);
        let error = result
            .err()
            .ok_or_else(|| Error::RustError("expected fault".into()))?;
        return Response::from_json(&json!({"primary": error.to_string()}));
    }
    if incoming.path() != "/run" {
        return Response::error("not found", 404);
    }
    let loader = WorkerLoader::from_env(&env, "LOADER")?;
    ensure(
        WorkerLoader::from_env(&env, "MISSING").is_err(),
        "missing binding",
    )?;
    let mut passed = Vec::new();
    let mut loaded = loader.load(code(STATEFUL, OutboundPolicy::Deny, 0))?;
    let req = Request::new_with_init(
        "https://guest.invalid/",
        RequestInit::new()
            .with_method(Method::Post)
            .with_body(Some("asymmetric".into())),
    )?;
    let response = loaded.fetch_bounded(None, req, 64).await?;
    ensure(response.body == b"1:0:asymmetric", "post body")?;
    ensure(
        loaded.fetch_bounded(None, request("/")?, 64).await?.body == b"2:0:",
        "repeat fetch",
    )?;
    passed.push("post/repeat");
    ensure(
        loaded.fetch_bounded(None, request("/env")?, 2).await?.body == b"[]",
        "empty env",
    )?;
    passed.push("empty-env");
    ensure(
        loaded
            .fetch_bounded(Some("Named"), request("/")?, 32)
            .await?
            .body
            == b"named:0",
        "named entrypoint",
    )?;
    let mut direct = loaded
        .entrypoint(Some("Named"))?
        .fetch_request(request("/")?)
        .await?;
    ensure(direct.text().await? == "named:0", "Fetcher access")?;
    passed.push("entrypoints/no-rpc-dispose");
    let streamed = loaded.fetch_bounded(None, request("/stream")?, 6).await?;
    ensure(
        streamed.body == b"abcdef"
            && streamed.status == 207
            && streamed.headers.get("x-fixture")?.as_deref() == Some("stream"),
        "stream metadata/exact bound",
    )?;
    passed.push("delayed-stream/exact-bound/status/headers");
    for (path, limit, expected) in [
        ("/stream", 5, "exceeds 5 bytes"),
        ("/huge", 32, "exceeds 32 bytes"),
        ("/nonempty", 0, "exceeds 0 bytes"),
        ("/body-error", 64, "body-primary"),
        ("/error", 64, "invoke-primary"),
    ] {
        let result = loaded.fetch_bounded(None, request(path)?, limit).await;
        ensure(result.is_err(), path)?;
        let error = result
            .err()
            .ok_or_else(|| Error::RustError("expected error".into()))?;
        ensure(error.to_string().contains(expected), &error.to_string())?;
    }
    passed.push("overflow/single-huge-chunk/zero-bound/body-error/invoke-error");
    ensure(
        loaded
            .fetch_bounded(None, request("/empty")?, 0)
            .await?
            .body
            .is_empty(),
        "empty body zero bound",
    )?;
    passed.push("empty-zero-bound");
    loaded.dispose();
    loaded.dispose();
    ensure(loaded.entrypoint(None).is_err(), "use after dispose")?;
    passed.push("idempotent-dispose/use-after-dispose");
    drop(loaded);
    for _ in 0..300 {
        let fresh = loader.load(code(STATEFUL, OutboundPolicy::Deny, 0))?;
        ensure(
            fresh.fetch_bounded(None, request("/")?, 16).await?.body == b"1:0:",
            "fresh isolation",
        )?;
    }
    passed.push("300-fresh-loads/drop-cleanup");
    for source in [
        "export default !!!",
        "import './missing.js'; export default {}",
    ] {
        let result = match loader.load(code(source, OutboundPolicy::Deny, 0)) {
            Ok(worker) => worker
                .fetch_bounded(None, request("/")?, 32)
                .await
                .map(|_| ()),
            Err(error) => Err(error),
        };
        ensure(result.is_err(), "invalid JS/missing import")?;
    }
    let mut invalid = code("export default {}", OutboundPolicy::Deny, 0);
    invalid
        .modules
        .insert("bad.wasm".into(), WorkerModule::Wasm(vec![0, 1, 2]));
    invalid.modules.insert(
        "guest.js".into(),
        WorkerModule::JavaScript("import './bad.wasm'; export default {}".into()),
    );
    let invalid_result = match loader.load(invalid) {
        Ok(worker) => worker
            .fetch_bounded(None, request("/")?, 32)
            .await
            .map(|_| ()),
        Err(error) => Err(error),
    };
    ensure(invalid_result.is_err(), "invalid wasm")?;
    passed.push("invalid-js/missing-module/invalid-wasm");
    let mut wasm = code(
        r#"import module from './answer.wasm';
      const instance = new WebAssembly.Instance(module);
      export default {fetch() {return new Response(String(instance.exports.answer()));}};"#,
        OutboundPolicy::Deny,
        0,
    );
    // (module (func (export "answer") (result i32) i32.const 42))
    wasm.modules.insert(
        "answer.wasm".into(),
        WorkerModule::Wasm(vec![
            0, 97, 115, 109, 1, 0, 0, 0, 1, 5, 1, 96, 0, 1, 127, 3, 2, 1, 0, 7, 10, 1, 6, 97, 110,
            115, 119, 101, 114, 0, 0, 10, 6, 1, 4, 0, 65, 42, 11,
        ]),
    );
    ensure(
        loader
            .load(wasm)?
            .fetch_bounded(None, request("/")?, 2)
            .await?
            .body
            == b"42",
        "compiled wasm import",
    )?;
    passed.push("js+compiled-wasm");
    let outbound = r#"export default {async fetch() {
      return await fetch('https://outbound.invalid/one');
    }};"#;
    // celld's service binding is an Object rather than a Fetcher constructor.
    let gateway: Fetcher = js_sys::Reflect::get(env.as_ref(), &"GATEWAY".into())?.unchecked_into();
    let outbound_result = loader
        .load(code(outbound, OutboundPolicy::Deny, 5))?
        .fetch_bounded(None, request("/")?, 64)
        .await;
    let outbound_error = outbound_result
        .err()
        .ok_or_else(|| Error::RustError("outbound Deny allowed a fetch".into()))?
        .to_string();
    ensure(
        outbound_error.contains("This worker is not permitted to access the internet"),
        &format!("explicit outbound denial: {outbound_error}"),
    )?;
    let allowed = loader.load(code(outbound, OutboundPolicy::Service(gateway.clone()), 1))?;
    ensure(
        allowed.fetch_bounded(None, request("/")?, 64).await?.body == b"gateway:/one",
        "service outbound allowed",
    )?;
    let denied = loader.load(code(outbound, OutboundPolicy::Service(gateway.clone()), 0))?;
    let result = denied.fetch_bounded(None, request("/")?, 64).await;
    let zero_denied = result.is_err();
    passed.push("deny-outbound/service-outbound");
    let twice = r#"export default {async fetch() {
      await (await fetch('https://outbound.invalid/one')).text();
      return await fetch('https://outbound.invalid/two');
    }};"#;
    let second_denied = loader
        .load(code(twice, OutboundPolicy::Service(gateway.clone()), 1))?
        .fetch_bounded(None, request("/")?, 64)
        .await
        .is_err();
    ensure(
        loader
            .load(code(twice, OutboundPolicy::Service(gateway), 2))?
            .fetch_bounded(None, request("/")?, 64)
            .await?
            .body
            == b"gateway:/two",
        "two subrequests allowed",
    )?;
    passed.push("two-subrequests-allowed");
    Response::from_json(&json!({"passed": passed, "count": passed.len(),
        "outbound_error": outbound_error,
        "zero_subrequests_denied": zero_denied, "second_subrequest_denied": second_denied}))
}

async fn cpu(env: &Env, cpu_ms: u32) -> Result<Response> {
    let loader = WorkerLoader::from_env(env, "LOADER")?;
    // Finite work so local workerd's lack of enforcement cannot hang the probe.
    let source = r#"export default {fetch() {
      let x = 0; for (let i = 0; i < 100000000; ++i) x = (x + i) | 0;
      return new Response(String(x));
    }};"#;
    let mut config = code(source, OutboundPolicy::Deny, 0);
    config.limits.cpu_ms = cpu_ms;
    let result = loader
        .load(config)?
        .fetch_bounded(None, request("/")?, 64)
        .await;
    match result {
        Ok(response) => Response::from_json(&json!({"cpu_enforced": false,
            "body": String::from_utf8_lossy(&response.body)})),
        Err(error) => {
            ensure(error.to_string().contains("CPU limit"), &error.to_string())?;
            Response::from_json(&json!({"cpu_enforced": true, "error": error.to_string()}))
        }
    }
}
