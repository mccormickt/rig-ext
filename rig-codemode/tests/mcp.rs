//! MCP acceptance tests against an in-process fake rmcp server.

#![cfg(all(feature = "quickjs", feature = "mcp"))]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rig_codemode::mcp::OutputSchemaValidator;
use rig_codemode::{
    CallStatus, Catalog, CatalogEntry, CodeMode, DynamicToolDispatcher, ExecutionRequest,
    ExecutionStatus,
};
use rig_rmcp::McpTool;
use rmcp::model::*;
use rmcp::service::RequestContext;
use rmcp::{RoleServer, ServerHandler, ServiceExt};
use serde_json::json;

#[derive(Clone)]
struct FakeServer(Arc<AtomicUsize>);

const SECRET: &str = "UNIQUE_REDACTED_SCHEMA_SECRET_82391";
const SECRET_KEY: &str = "UNIQUE_REDACTED_PROPERTY_71503";

impl ServerHandler for FakeServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_protocol_version(ProtocolVersion::LATEST)
            .with_server_info(Implementation::new("rig-codemode-test", "0.1.0"))
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let output_schema = Arc::new(
            json!({
                "type": "object",
                "properties": { "answer": { "type": "integer" } },
                "required": ["answer"],
                "additionalProperties": false
            })
            .as_object()
            .unwrap()
            .clone(),
        );
        let input = Arc::new(json!({"type": "object"}).as_object().unwrap().clone());
        let typed = |name: &str, description: &str| {
            Tool::new(name.to_string(), description.to_string(), input.clone())
                .with_raw_output_schema(output_schema.clone())
        };
        Ok(ListToolsResult::with_all_items(vec![
            typed("answer", "Returns a structured answer"),
            typed(
                "wrong_shape",
                "Returns structured content of the wrong shape",
            ),
            typed("secret_key", "Returns an unexpected property"),
            typed("no_structured", "Declares a schema but returns only text"),
            typed(
                "structured_error",
                "Reports isError with structured content",
            ),
            Tool::new("plain", "No output schema; returns text", input.clone()),
        ]))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(match request.name.as_ref() {
            "answer" => CallToolResult::structured(json!({"answer": 42})),
            "wrong_shape" => CallToolResult::structured(json!({"answer": SECRET})),
            "secret_key" => CallToolResult::structured(json!({"answer": 42, (SECRET_KEY): true})),
            "no_structured" => CallToolResult::success(vec![ContentBlock::text("{\"answer\":42}")]),
            "structured_error" => {
                let mut result = CallToolResult::structured(json!({"answer": 7}));
                result.is_error = Some(true);
                result
            }
            "plain" => CallToolResult::success(vec![ContentBlock::text("plain text")]),
            other => {
                return Err(ErrorData::invalid_params(
                    format!("unknown tool {other}"),
                    None,
                ));
            }
        })
    }
}

struct Fixture {
    codemode: CodeMode,
    invocations: Arc<AtomicUsize>,
    _client: rmcp::service::RunningService<rmcp::service::RoleClient, ClientInfo>,
    server_task: tokio::task::JoinHandle<()>,
}

async fn fixture() -> Fixture {
    let (client_to_server, server_from_client) = tokio::io::duplex(8192);
    let (server_to_client, client_from_server) = tokio::io::duplex(8192);
    let invocations = Arc::new(AtomicUsize::new(0));
    let server = FakeServer(invocations.clone());
    let server_task = tokio::spawn(async move {
        let running = server
            .serve((server_from_client, server_to_client))
            .await
            .expect("server start");
        let _ = running.waiting().await;
    });
    let client = ClientInfo::default()
        .serve((client_from_server, client_to_server))
        .await
        .expect("client connect");
    let definitions = client.list_all_tools().await.expect("list tools");
    let mcp_tools = rig_rmcp::tools_from_server(definitions, client.peer());

    let catalog = Catalog::new(
        mcp_tools
            .iter()
            .map(McpTool::definition)
            .map(CatalogEntry::from_mcp_definition),
    )
    .unwrap();
    assert!(catalog.get("answer").unwrap().output_schema().is_some());
    assert!(catalog.get("plain").unwrap().output_schema().is_none());

    let dispatcher = DynamicToolDispatcher::new(mcp_tools.into_iter().map(Into::into))
        .unwrap()
        .with_result_policy(|invocation, result| {
            if matches!(invocation.name.as_str(), "wrong_shape" | "secret_key") {
                rig_core::tool::ToolResult::success(rig_core::tool::ToolOutput::text("redacted"))
            } else {
                result
            }
        });
    let dispatcher = OutputSchemaValidator::new(dispatcher, &catalog).unwrap();
    let codemode = CodeMode::builder(catalog, Arc::new(dispatcher))
        .build()
        .unwrap();
    Fixture {
        codemode,
        invocations,
        _client: client,
        server_task,
    }
}

#[tokio::test]
async fn structured_results_reach_scripts_as_values_and_schemas_render() {
    let f = fixture().await;
    let description = f.codemode.description();
    assert!(
        description
            .contains("\"answer\"(args: Record<string, unknown>): Promise<{ answer: number }>;"),
        "{description}"
    );
    let report = f
        .codemode
        .execute(ExecutionRequest::new(
            "const a = await tools.answer(); const p = await tools.plain(); return [a, typeof a, p];",
        ))
        .await
        .unwrap();
    assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
    assert_eq!(
        report.returned,
        Some(json!([{"answer": 42}, "object", "plain text"]))
    );
    f.server_task.abort();
}

#[tokio::test]
async fn schema_mismatch_is_a_typed_failure_without_retry() {
    #[derive(Clone, Default)]
    struct Capture(Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let capture = Capture::default();
    let writer = capture.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let f = fixture().await;
    let report = f
        .codemode
        .execute(ExecutionRequest::new(
            r#"
            const out = {};
            for (const name of ["wrong_shape", "secret_key", "no_structured"]) {
                try { out[name] = await tools[name](); }
                catch (e) { out[name] = { status: e.status, kind: e.kind, message: e.message }; }
            }
            out.raw = await tools.wrong_shape.raw();
            out.rawKey = await tools.secret_key.raw();
            return out;
            "#,
        ))
        .await
        .unwrap();
    assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
    let r = report.returned.clone().unwrap();
    assert_eq!(r["wrong_shape"]["status"], "error");
    assert_eq!(r["wrong_shape"]["kind"], "other");
    let message = r["wrong_shape"]["message"].as_str().unwrap();
    assert!(
        message.contains("does not match its declared output schema"),
        "{message}"
    );
    assert!(!message.contains("/answer"), "{message}");
    assert!(!format!("{report:?} {}", report.render_text()).contains(SECRET));
    assert!(!format!("{report:?} {}", report.render_text()).contains(SECRET_KEY));
    let trace = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    assert!(trace.contains("MCP output schema mismatch"), "{trace}");
    assert!(!trace.contains(SECRET), "{trace}");
    assert!(!trace.contains(SECRET_KEY), "{trace}");
    assert_eq!(r["no_structured"]["status"], "error");
    assert!(
        r["no_structured"]["message"]
            .as_str()
            .unwrap()
            .contains("no structuredContent"),
        "{r}"
    );
    assert_eq!(r["raw"]["status"], "error");
    assert_eq!(r["rawKey"]["status"], "error");
    assert_eq!(r["secret_key"]["status"], "error");
    assert_eq!(r["raw"]["error"]["retryable"], false);
    assert_eq!(
        f.invocations.load(Ordering::SeqCst),
        5,
        "each script call invokes the server exactly once"
    );
    assert!(
        report.calls.iter().all(|c| c.status == CallStatus::Failed),
        "{:?}",
        report.calls
    );
    f.server_task.abort();
}

#[tokio::test]
async fn is_error_with_structured_content_rejects_normally_and_keeps_data_in_raw() {
    let f = fixture().await;
    let report = f
        .codemode
        .execute(ExecutionRequest::new(
            r#"
            let normal;
            try { normal = { value: await tools.structured_error() }; }
            catch (e) { normal = { status: e.status, kind: e.kind, message: e.message }; }
            const raw = await tools.structured_error.raw();
            return { normal, raw };
            "#,
        ))
        .await
        .unwrap();
    assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
    let r = report.returned.clone().unwrap();
    assert_eq!(r["normal"]["status"], "error");
    assert_eq!(r["normal"]["kind"], "other");
    assert!(r["normal"].get("value").is_none());
    assert_eq!(r["raw"]["status"], "error");
    assert_eq!(r["raw"]["name"], "structured_error");
    assert_eq!(
        r["raw"]["content"][0],
        json!({"type": "json", "value": {"answer": 7}})
    );
    assert!(
        r["raw"]["error"]["message"].as_str().unwrap().contains("7"),
        "{r}"
    );
    assert!(r["raw"].get("_meta").is_none());
    f.server_task.abort();
}
