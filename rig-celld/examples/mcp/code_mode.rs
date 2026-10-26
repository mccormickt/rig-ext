//! Execute a local script with an explicit allowlist of remote MCP tools.

use std::sync::Arc;

use rig_celld::mcp::{Client, HttpTransport, model::Implementation};
use rig_codemode::{
    Catalog, CatalogEntry, CodeMode, DynamicToolDispatcher, ExecutionRequest, ExecutionStatus,
};
use serde_json::Value;

const USAGE: &str = "Usage: mcp_code_mode <endpoint> <script.js> <allowed-tool>...\nOptional environment: MCP_BEARER_TOKEN";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if matches!(args.as_slice(), [arg] if arg == "--help") {
        println!("{USAGE}");
        return Ok(());
    }
    let (endpoint, script, allowed) = match args.as_slice() {
        [endpoint, script, allowed @ ..] if !allowed.is_empty() => (endpoint, script, allowed),
        _ => return Err(USAGE.into()),
    };
    let script = std::fs::read_to_string(script)?;
    let transport = HttpTransport::new(endpoint)?;
    let transport = match std::env::var("MCP_BEARER_TOKEN") {
        Ok(token) => transport.bearer_auth(token)?,
        Err(std::env::VarError::NotPresent) => transport,
        Err(_) => return Err("MCP_BEARER_TOKEN must contain Unicode text".into()),
    };
    let client = Client::new(transport, Implementation::new("native-example", "1"));
    let mut tools = client.list_tools().await?;
    for name in allowed {
        if !tools
            .iter()
            .any(|tool| tool.definition().name.as_ref() == name)
        {
            return Err(format!("allowed tool was not discovered: {name}").into());
        }
    }
    tools.retain(|tool| {
        allowed
            .iter()
            .any(|name| tool.definition().name.as_ref() == name)
    });
    let catalog = Catalog::new(tools.iter().map(|tool| {
        let definition = tool.definition();
        let mut entry = CatalogEntry::new(
            definition.name.to_string(),
            definition.description.as_deref().unwrap_or(""),
            Value::Object(definition.input_schema.as_ref().clone()),
        );
        if let Some(output) = &definition.output_schema {
            entry = entry.with_output_schema(Value::Object(output.as_ref().clone()));
        }
        entry
    }))?;
    let dispatcher =
        DynamicToolDispatcher::new(tools.into_iter().map(|tool| tool.into_dynamic_tool()))?;
    let code = CodeMode::builder(catalog, Arc::new(dispatcher)).build()?;
    let report = code.execute(ExecutionRequest::new(script)).await?;
    print!("{}", report.render_text());
    if report.status != ExecutionStatus::Completed {
        return Err(format!("script ended with status {:?}", report.status).into());
    }
    Ok(())
}
