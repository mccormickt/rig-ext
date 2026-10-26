//! Discover tools or call one tool on a configured JSON-only MCP endpoint.

use rig_celld::mcp::{Client, HttpTransport, model::Implementation};
use serde_json::Value;

const USAGE: &str =
    "Usage: mcp_call <endpoint> [<tool> <arguments-json>]\nOptional environment: MCP_BEARER_TOKEN";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if matches!(args.as_slice(), [arg] if arg == "--help") {
        println!("{USAGE}");
        return Ok(());
    }
    let (endpoint, call) = match args.as_slice() {
        [endpoint] => (endpoint, None),
        [endpoint, tool, arguments] => (
            endpoint,
            Some((tool, serde_json::from_str::<Value>(arguments)?)),
        ),
        _ => return Err(USAGE.into()),
    };
    let transport = HttpTransport::new(endpoint)?;
    let transport = match std::env::var("MCP_BEARER_TOKEN") {
        Ok(token) => transport.bearer_auth(token)?,
        Err(std::env::VarError::NotPresent) => transport,
        Err(_) => return Err("MCP_BEARER_TOKEN must contain Unicode text".into()),
    };
    let client = Client::new(transport, Implementation::new("native-example", "1"));
    let tools = client.list_tools().await?;
    if let Some((name, arguments)) = call {
        let tool = tools
            .iter()
            .find(|tool| tool.definition().name.as_ref() == name)
            .ok_or("requested tool was not discovered")?;
        let result = tool.call(arguments, &Default::default()).await?;
        // This host-facing result includes MCP metadata, not model output.
        println!("{}", serde_json::to_string_pretty(&result)?);
        if result.is_error == Some(true) {
            return Err("MCP tool returned an error".into());
        }
    } else {
        let definitions: Vec<_> = tools.iter().map(|tool| tool.definition()).collect();
        println!("{}", serde_json::to_string_pretty(&definitions)?);
    }
    Ok(())
}
