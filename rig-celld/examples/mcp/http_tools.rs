//! Run against the loopback fixture started by scripts/workers/mcp.sh.
//! cargo run -p rig-celld --example http_tools --features mcp-http

use std::sync::Arc;

use rig_celld::mcp::{Client, Error, HttpTransport, model::Implementation};
use rig_codemode::{Catalog, CatalogEntry, CodeMode, DynamicToolDispatcher, ExecutionRequest};
use serde_json::{Value, json};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:9899/ok".into());
    // This credential is a public fixture constant, not a production secret.
    let client = Client::new(
        HttpTransport::new(&endpoint)?.bearer_auth("conformance-only")?,
        Implementation::new("native-example", "1"),
    );
    let tools = client.list_tools().await?;
    let names: Vec<_> = tools
        .iter()
        .map(|tool| tool.definition().name.to_string())
        .collect();
    let lookup = tools
        .iter()
        .find(|tool| tool.definition().name == "lookup")
        .ok_or("lookup is missing")?;
    let invalid_input_blocked = matches!(
        lookup.call(json!({"region": 9}), &Default::default()).await,
        Err(Error::Arguments)
    );
    let catalog = Catalog::new(tools.iter().map(|tool| {
        let wire = tool.definition();
        let mut entry = CatalogEntry::new(
            wire.name.to_string(),
            wire.description.as_deref().unwrap_or(""),
            Value::Object(wire.input_schema.as_ref().clone()),
        );
        if let Some(output) = &wire.output_schema {
            entry = entry.with_output_schema(Value::Object(output.as_ref().clone()));
        }
        entry
    }))?;
    let dispatcher =
        DynamicToolDispatcher::new(tools.into_iter().map(|tool| tool.into_dynamic_tool()))?;
    let code = CodeMode::builder(catalog, Arc::new(dispatcher)).build()?;
    let report = code
        .execute(ExecutionRequest::new(include_str!("stock.js")))
        .await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "runtime": "native-quickjs", "tools": names, "invalid_input_blocked": invalid_input_blocked,
            "script": include_str!("stock.js"), "transcript": report.render_text(), "report": report,
        }))?
    );
    Ok(())
}
