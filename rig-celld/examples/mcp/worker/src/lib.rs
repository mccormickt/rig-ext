#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#![allow(clippy::arc_with_non_send_sync)]

use futures_util::future::{Either, select};
use rig_celld::mcp::{Client, Error, HttpTransport, Limits, model::Implementation};
use rig_codemode::{DynamicToolDispatcher, HostDispatcher, Invocation};
use rig_core::tool::ToolContext;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use worker::*;

mod demo;

fn client(base: &str, path: &str) -> std::result::Result<Client<HttpTransport>, Error> {
    Ok(Client::new(
        HttpTransport::new(&format!("{base}/{path}"))?.bearer_auth("conformance-only")?,
        Implementation::new("worker-conformance", "1"),
    ))
}

async fn checks(
    base: &str,
) -> std::result::Result<BTreeMap<&'static str, bool>, Box<dyn std::error::Error>> {
    let mut checks = BTreeMap::new();
    let tools = client(base, "ok")?.list_tools().await?;
    checks.insert("pagination", tools.len() == 2);
    let definition = tools.first().ok_or("no tools")?.definition().clone();
    let dispatcher: Arc<dyn HostDispatcher> =
        Arc::new(DynamicToolDispatcher::new([client(base, "ok")?
            .tool(definition.clone())?
            .into_dynamic_tool()])?);
    let result = dispatcher
        .dispatch(Invocation::new(
            "parent",
            0,
            "lookup",
            json!({"region": "west"}),
            ToolContext::new(),
        ))
        .await;
    checks.insert(
        "code_mode_dispatch",
        result.result.is_success()
            && result.result.output().as_json() == Some(&json!({"answer": 37})),
    );
    checks.insert(
        "metadata_private",
        !serde_json::to_string(&result.result)?.contains("private-marker"),
    );

    for path in [
        "is-error",
        "bad-schema",
        "rpc",
        "mismatch",
        "sse",
        "oversize",
        "timeout",
        "disconnect",
        "redirect",
        "http-empty",
        "http-html",
        "http-json",
        "http-missing-id",
        "http-wrong-id",
        "http-malformed",
        "http-oversize",
        "http-timeout",
    ] {
        let limits = Limits {
            response_bytes: 4096,
            timeout: Duration::from_millis(500),
            ..Limits::default()
        };
        let tool = client(base, path)?
            .with_limits(limits)?
            .tool(definition.clone())?;
        let result = tool
            .call(json!({"region": "west"}), &Default::default())
            .await;
        let passed = match path {
            "is-error" => result.is_ok_and(|result| result.is_error == Some(true)),
            "bad-schema" => matches!(result, Err(Error::OutputSchema)),
            "rpc" => matches!(result, Err(Error::Rpc(-32022))),
            "mismatch" => matches!(result, Err(Error::Protocol(_))),
            "sse" => matches!(result, Err(Error::ContentType)),
            "oversize" => matches!(result, Err(Error::Limit(_))),
            "timeout" => matches!(result, Err(Error::Timeout)),
            "disconnect" => matches!(result, Err(Error::Transport)),
            "redirect" => matches!(result, Err(Error::Http(307))),
            "http-empty" => matches!(result, Err(Error::Http(401))),
            "http-html" => matches!(result, Err(Error::Http(503))),
            "http-json" => matches!(result, Err(Error::Http(429))),
            "http-missing-id" | "http-wrong-id" => matches!(result, Err(Error::Http(403))),
            "http-malformed" => matches!(result, Err(Error::Http(502))),
            "http-oversize" => matches!(result, Err(Error::Limit(_))),
            "http-timeout" => matches!(result, Err(Error::Timeout)),
            _ => false,
        };
        checks.insert(path, passed);
    }
    for path in ["cancel-headers", "cancel-body"] {
        let tool = client(base, path)?.tool(definition.clone())?;
        let options = Default::default();
        let call = Box::pin(tool.call(json!({"region": "west"}), &options));
        match select(call, Box::pin(wait_started(base, path))).await {
            Either::Right((observed, pending)) => {
                drop(pending);
                checks.insert(path, observed?);
            }
            Either::Left(_) => {
                checks.insert(path, false);
            }
        }
    }
    Ok(checks)
}

async fn wait_started(base: &str, path: &str) -> worker::Result<bool> {
    for _ in 0..100 {
        let mut response = Fetch::Url(format!("{base}/stats").parse()?).send().await?;
        let stats: Value = response.json().await?;
        if stats["started"]
            .as_array()
            .is_some_and(|paths| paths.iter().any(|value| value.as_str() == Some(path)))
        {
            return Ok(true);
        }
        Delay::from(Duration::from_millis(10)).await;
    }
    Ok(false)
}

#[event(fetch)]
async fn main(
    request: worker::Request,
    env: Env,
    _context: Context,
) -> worker::Result<worker::Response> {
    if request.path() == "/health" {
        return worker::Response::ok("ready");
    }
    if matches!(request.path().as_str(), "/demo" | "/demo/bad-schema") {
        let path = if request.path() == "/demo" {
            "ok"
        } else {
            "bad-schema"
        };
        return match demo::run(&env, path).await {
            Ok(result) => worker::Response::from_json(&result),
            Err(error) => worker::Response::error(error.to_string(), 500),
        };
    }
    match checks(&env.var("MCP_ENDPOINT")?.to_string()).await {
        Ok(checks) => {
            let passed = checks.values().all(|passed| *passed);
            Ok(
                worker::Response::from_json(&json!({"passed": passed, "checks": checks}))?
                    .with_status(if passed { 200 } else { 500 }),
            )
        }
        Err(error) => worker::Response::error(error.to_string(), 500),
    }
}
