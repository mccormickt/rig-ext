//! MCP tools in code mode: catalog entries keep the server's output schemas,
//! and `OutputSchemaValidator` checks `structuredContent` at the dispatch
//! boundary. The MCP server here runs in-process over a duplex pipe so the
//! example needs no network.
//!
//! ```sh
//! cargo run -p rig-codemode --example mcp_tools --features quickjs,mcp
//! ```

use std::sync::Arc;

use rig_codemode::mcp::OutputSchemaValidator;
use rig_codemode::{Catalog, CatalogEntry, CodeMode, DynamicToolDispatcher, ExecutionRequest};
use rig_rmcp::McpTool;
use rmcp::model::*;
use rmcp::service::RequestContext;
use rmcp::{RoleServer, ServerHandler, ServiceExt};
use serde_json::json;

#[derive(Clone)]
struct WeatherServer;

impl ServerHandler for WeatherServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("weather", "0.1.0"))
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let object =
            |value: serde_json::Value| Arc::new(value.as_object().cloned().unwrap_or_default());
        let forecast = Tool::new(
            "forecast",
            "Forecast for a city",
            object(json!({
                "type": "object",
                "properties": { "city": { "type": "string" } },
                "required": ["city"]
            })),
        )
        .with_raw_output_schema(object(json!({
            "type": "object",
            "properties": {
                "city": { "type": "string" },
                "high_c": { "type": "number" },
                "conditions": { "type": "string", "enum": ["sunny", "rain", "snow"] }
            },
            "required": ["city", "high_c", "conditions"]
        })));
        Ok(ListToolsResult::with_all_items(vec![forecast]))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let city = request
            .arguments
            .as_ref()
            .and_then(|args| args.get("city"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("nowhere");
        let (high, conditions) = match city {
            "Oslo" => (4.0, "snow"),
            "Lisbon" => (21.5, "sunny"),
            // Violates the declared schema on purpose.
            "Atlantis" => {
                return Ok(CallToolResult::structured(
                    json!({"city": city, "high_c": "warm"}),
                ));
            }
            _ => (12.0, "rain"),
        };
        Ok(CallToolResult::structured(json!({
            "city": city, "high_c": high, "conditions": conditions
        })))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (client_to_server, server_from_client) = tokio::io::duplex(8192);
    let (server_to_client, client_from_server) = tokio::io::duplex(8192);
    let server = tokio::spawn(async move {
        if let Ok(running) = WeatherServer
            .serve((server_from_client, server_to_client))
            .await
        {
            let _ = running.waiting().await;
        }
    });
    let client = ClientInfo::default()
        .serve((client_from_server, client_to_server))
        .await?;

    let definitions = client.list_all_tools().await?;
    let mcp_tools = rig_rmcp::tools_from_server(definitions, client.peer());
    // Build the catalog from the MCP definitions: the Rig `ToolDefinition`
    // produced by `DynamicTool::from` has no output schema.
    let catalog = Catalog::new(
        mcp_tools
            .iter()
            .map(McpTool::definition)
            .map(CatalogEntry::from_mcp_definition),
    )?;
    let dispatcher = DynamicToolDispatcher::new(mcp_tools.into_iter().map(Into::into))?;
    let dispatcher = OutputSchemaValidator::new(dispatcher, &catalog)?;
    let codemode = CodeMode::builder(catalog, Arc::new(dispatcher)).build()?;

    println!(
        "== declarations ==\n{}",
        codemode.catalog().render_declarations(8 * 1024)
    );

    let report = codemode
        .execute(ExecutionRequest::new(
            r#"
            const cities = ["Oslo", "Lisbon", "Atlantis"];
            const results = await Promise.allSettled(cities.map(city => tools.forecast({ city })));
            for (const [i, r] of results.entries()) {
                if (r.status === "fulfilled") text(`${cities[i]}: ${r.value.high_c}°C, ${r.value.conditions}`);
                else text(`${cities[i]}: ${r.reason.message}`);
            }
            return results.filter(r => r.status === "fulfilled").map(r => r.value.city);
            "#,
        ))
        .await?;
    println!("== model-visible result ==\n{}", report.render_text());
    for call in &report.calls {
        println!("  #{} {} {:?}", call.ordinal, call.name, call.status);
    }
    server.abort();
    Ok(())
}
