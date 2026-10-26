#![cfg(all(feature = "mcp", not(target_family = "wasm")))]
#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use rig_celld::mcp::{
    CallOptions, Client, Error, Limits, Request, Response, Transport,
    model::{Implementation, Tool},
};
use rig_core::{
    tool::{ToolContext, ToolErrorKind},
    wasm_compat::WasmBoxedFuture,
};
use serde_json::{Value, json};

type Handler = dyn Fn(&Value, &[(String, String)]) -> Value + Send + Sync;

#[derive(Clone)]
struct Endpoint {
    handler: Arc<Handler>,
    requests: Arc<Mutex<Vec<Value>>>,
}

impl Endpoint {
    fn new(handler: impl Fn(&Value, &[(String, String)]) -> Value + Send + Sync + 'static) -> Self {
        Self {
            handler: Arc::new(handler),
            requests: Arc::default(),
        }
    }
    fn client(&self) -> Client<Self> {
        Client::new(self.clone(), Implementation::new("test-client", "1"))
    }
}

impl Transport for Endpoint {
    fn post(&self, request: Request, _: Limits) -> WasmBoxedFuture<'_, Result<Response, Error>> {
        Box::pin(async move {
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            self.requests.lock().unwrap().push(body.clone());
            Ok(Response {
                status: 200,
                content_type: "application/json; charset=utf-8".into(),
                body: serde_json::to_vec(&(self.handler)(&body, &request.headers)).unwrap(),
            })
        })
    }
}

fn reply(request: &Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": request["id"], "result": result})
}

fn definition(name: &str) -> Tool {
    serde_json::from_value(json!({
        "name": name, "inputSchema": {"type": "object"},
        "outputSchema": {"type": "object", "properties": {"answer": {"type": "integer"}}, "required": ["answer"]}
    })).unwrap()
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

#[tokio::test]
async fn stateless_discovery_and_calls_mirror_metadata_and_parameter_headers() {
    let input = json!({"type": "object", "properties": {
        "tenant": {"type": "object", "properties": {
            "region": {"type": "string", "x-mcp-header": "Region"},
            "count": {"type": "integer", "x-mcp-header": "Count"},
            "enabled": {"type": "boolean", "x-mcp-header": "Enabled"}
        }}
    }});
    let schema = input.clone();
    let endpoint = Endpoint::new(move |request, headers| {
        assert_eq!(header(headers, "Accept"), Some("application/json"));
        assert_eq!(header(headers, "MCP-Protocol-Version"), Some("2026-07-28"));
        assert_eq!(header(headers, "Mcp-Method"), request["method"].as_str());
        assert!(header(headers, "Mcp-Session-Id").is_none());
        assert_eq!(
            request["params"]["_meta"],
            json!({
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientInfo": {"name": "test-client", "version": "1"},
                "io.modelcontextprotocol/clientCapabilities": {}
            })
        );
        if request["method"] == "tools/list" {
            let second = request["params"]["cursor"] == "next";
            let mut page = json!({"resultType": "complete", "ttlMs": 0, "cacheScope": "private", "tools": [{
                "name": if second { "other" } else { "lookup" }, "inputSchema": schema
            }]});
            if !second {
                page["nextCursor"] = json!("next");
            }
            reply(request, page)
        } else {
            assert_eq!(header(headers, "Mcp-Name"), Some("lookup"));
            assert_eq!(
                header(headers, "Idempotency-Key"),
                Some("run-1/turn-3/call-2")
            );
            assert_eq!(
                header(headers, "Mcp-Param-Region"),
                Some("=?base64?IHBhZGRlZCA=?=")
            );
            assert_eq!(
                header(headers, "Mcp-Param-Count"),
                Some("-9007199254740991")
            );
            assert_eq!(header(headers, "Mcp-Param-Enabled"), Some("false"));
            reply(
                request,
                json!({"resultType": "complete", "content": [], "structuredContent": {"answer": 37}}),
            )
        }
    });
    let tools = endpoint.client().list_tools().await.unwrap();
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.definition().name.as_ref())
            .collect::<Vec<_>>(),
        ["lookup", "other"]
    );
    let options = CallOptions {
        idempotency_key: Some("run-1/turn-3/call-2".into()),
    };
    let result = tools[0].call(json!({"tenant": {"region": " padded ", "count": -9_007_199_254_740_991i64, "enabled": false}}), &options).await.unwrap();
    assert_eq!(result.structured_content, Some(json!({"answer": 37})));
    assert!(matches!(
        tools[0]
            .call(
                json!({"tenant": {"count": 9_007_199_254_740_992u64}}),
                &options
            )
            .await,
        Err(Error::Arguments)
    ));
    let requests = endpoint.requests.lock().unwrap();
    let ids = requests
        .iter()
        .map(|request| request["id"].as_str().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids.len(), 3);
}

#[tokio::test]
async fn header_annotations_reject_ambiguous_paths_but_not_schema_data() {
    for input in [
        json!({"type": "object", "properties": {"x": {"type": "number", "x-mcp-header": "X"}}}),
        json!({"type": "object", "properties": {"x": {"type": "string", "x-mcp-header": "Bad\r\n"}}}),
        json!({"type": "object", "properties": {"x": {"type": "string", "x-mcp-header": "Same"}, "y": {"type": "boolean", "x-mcp-header": "same"}}}),
        json!({"type": "object", "allOf": [{"properties": {"x": {"type": "string", "x-mcp-header": "X"}}}]}),
        json!({"type": "object", "properties": {"xs": {"type": "array", "items": {"type": "string", "x-mcp-header": "X"}}}}),
        json!({"type": "object", "properties": {"payload": {
            "type": "string", "contentMediaType": "application/json",
            "contentSchema": {"type": "object", "properties": {
                "tenant": {"type": "string", "x-mcp-header": "Tenant"}
            }}
        }}}),
    ] {
        let endpoint = Endpoint::new(move |request, _| {
            reply(
                request,
                json!({"resultType": "complete", "tools": [
                    {"name": "invalid", "inputSchema": input},
                    {"name": "valid", "inputSchema": {"type": "object", "default": {"x-mcp-header": "just data"}}}
                ]}),
            )
        });
        let tools = endpoint.client().list_tools().await.unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].definition().name, "valid");
    }
}

#[tokio::test]
async fn result_validation_and_code_mode_keep_failures_and_metadata_private() {
    let endpoint = Endpoint::new(|request, _| {
        let mode = request["params"]["arguments"]["mode"]
            .as_str()
            .unwrap_or("ok");
        let result = match mode {
            "missing" => json!({"resultType": "complete", "content": []}),
            "bad" => {
                json!({"resultType": "complete", "content": [], "structuredContent": {"answer": "private-invalid-value"}})
            }
            "error" => {
                json!({"resultType": "complete", "isError": true, "content": [{"type": "text", "text": "permission denied", "_meta": {"private": "secret"}}]})
            }
            "mixed" => json!({"resultType": "complete", "content": [
                {"type": "audio", "data": "YQ==", "mimeType": "audio/wav", "_meta": {"private": "secret"}},
                {"type": "resource", "resource": {"uri": "test://record", "text": "public", "_meta": {"private": "secret"}}, "_meta": {"private": "secret"}},
                {"type": "resource_link", "uri": "test://link", "name": "record", "_meta": {"private": "secret"}}
            ], "structuredContent": {"answer": 37, "_meta": "application-data"}}),
            _ => {
                json!({"resultType": "complete", "content": [{"type": "text", "text": "{\"answer\":37}", "_meta": {"private": "secret"}}], "structuredContent": {"answer": 37}, "_meta": {"private": "secret"}})
            }
        };
        reply(request, result)
    });
    let tool = endpoint.client().tool(definition("lookup")).unwrap();
    for mode in ["missing", "bad"] {
        assert!(matches!(
            tool.call(json!({"mode": mode}), &CallOptions::default())
                .await,
            Err(Error::OutputSchema)
        ));
    }
    let dynamic = tool.into_dynamic_tool();
    let output = dynamic.execute(json!({})).await.unwrap();
    assert_eq!(output.as_json(), Some(&json!({"answer": 37})));
    let mixed = dynamic.execute(json!({"mode": "mixed"})).await.unwrap();
    let mixed = serde_json::to_string(&mixed).unwrap();
    assert!(!mixed.contains("secret"));
    assert!(mixed.contains("application-data"));
    assert!(mixed.contains("test://record") && mixed.contains("test://link"));
    let error = dynamic.execute(json!({"mode": "error"})).await.unwrap_err();
    assert_eq!(error.kind(), ToolErrorKind::Other);
    assert_eq!(error.retryable(), Some(false));
    assert!(
        !serde_json::to_string(&error.model_output())
            .unwrap()
            .contains("secret")
    );

    let dispatcher = rig_codemode::DynamicToolDispatcher::new([dynamic.clone()]).unwrap();
    let catalog = rig_codemode::Catalog::new([rig_codemode::CatalogEntry::from_definition(
        &dynamic.definition(),
    )])
    .unwrap();
    let code = rig_codemode::CodeMode::builder(catalog, Arc::new(dispatcher))
        .build()
        .unwrap();
    let report = code.execute(rig_codemode::ExecutionRequest::new(
        "const value = await tools.lookup({}); text(value.answer); const failure = await tools.lookup.raw({mode:'error'}); text(failure.status);"
    )).await.unwrap();
    assert_eq!(report.output.text, "37\nerror\n");
    assert!(!serde_json::to_string(&report).unwrap().contains("secret"));
}

#[tokio::test]
async fn code_mode_ignores_json_formatting_but_preserves_distinct_text() {
    use rig_core::message::ToolResultContent;

    let endpoint = Endpoint::new(|request, _| {
        reply(
            request,
            json!({
                "resultType": "complete",
                "content": [{"type": "text", "text": request["params"]["arguments"]["text"]}],
                "structuredContent": {"answer": 37, "region": "west"}
            }),
        )
    });
    let dynamic = endpoint
        .client()
        .tool(definition("lookup"))
        .unwrap()
        .into_dynamic_tool();
    let catalog = rig_codemode::Catalog::new([rig_codemode::CatalogEntry::from_definition(
        &dynamic.definition(),
    )])
    .unwrap();
    let dispatcher = rig_codemode::DynamicToolDispatcher::new([dynamic]).unwrap();
    let code = rig_codemode::CodeMode::builder(catalog, Arc::new(dispatcher))
        .build()
        .unwrap();
    for (text, equivalent) in [
        (r#"{"answer":37,"region":"west"}"#, true),
        ("{\n  \"answer\": 37,\n  \"region\": \"west\"\n}", true),
        (r#"{"region":"west","answer":37}"#, true),
        (r#"{"answer":38,"region":"west"}"#, false),
        ("Additional public detail", false),
    ] {
        let report = code
            .execute(rig_codemode::ExecutionRequest::new(format!(
                "return await tools.lookup({});",
                json!({"text": text})
            )))
            .await
            .unwrap();
        assert_eq!(report.status, rig_codemode::ExecutionStatus::Completed);
        let structured = json!({"answer": 37, "region": "west"});
        let expected = if equivalent {
            structured
        } else {
            json!({"content": [ToolResultContent::json(structured), ToolResultContent::text(text)]})
        };
        assert_eq!(report.returned, Some(expected), "text: {text}");
    }
}

#[tokio::test]
async fn non_ascii_names_and_sentinel_values_use_unambiguous_headers() {
    let endpoint = Endpoint::new(|request, headers| {
        assert_eq!(
            header(headers, "Mcp-Name"),
            Some("=?base64?SGVsbG8sIOS4lueVjA==?=")
        );
        assert_eq!(
            header(headers, "Mcp-Param-Text"),
            Some("=?base64?PT9iYXNlNjQ/bGl0ZXJhbD89?=")
        );
        assert!(header(headers, "Mcp-Param-Absent").is_none());
        reply(request, json!({"resultType": "complete", "content": []}))
    });
    let definition = serde_json::from_value(json!({
        "name": "Hello, 世界", "inputSchema": {"type": "object", "properties": {
            "text": {"type": "string", "x-mcp-header": "Text"},
            "absent": {"type": "boolean", "x-mcp-header": "Absent"}
        }}
    }))
    .unwrap();
    endpoint
        .client()
        .tool(definition)
        .unwrap()
        .call(json!({"text": "=?base64?literal?="}), &Default::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn dynamic_context_passes_only_explicit_idempotency_key() {
    let endpoint = Endpoint::new(|request, headers| {
        assert_eq!(header(headers, "Idempotency-Key"), Some("stable-key"));
        assert!(request["params"]["_meta"].get("credentials").is_none());
        reply(
            request,
            json!({"resultType": "complete", "content": [], "structuredContent": {"answer": 7}}),
        )
    });
    let tool = endpoint
        .client()
        .tool(definition("write"))
        .unwrap()
        .into_dynamic_tool();
    let mut context = ToolContext::new();
    context
        .insert(CallOptions {
            idempotency_key: Some("stable-key".into()),
        })
        .unwrap();
    assert_eq!(
        tool.execute_with(&mut context, json!({}))
            .await
            .unwrap()
            .as_json(),
        Some(&json!({"answer": 7}))
    );
}

#[tokio::test]
async fn envelopes_and_unsupported_results_fail_without_retry() {
    let cases = [
        json!({"jsonrpc": "2.0", "id": "wrong", "result": {"resultType": "complete", "tools": []}}),
        json!({"jsonrpc": "2.0", "id": "0", "result": {}, "error": {"code": -32022, "message": "private"}}),
        json!({"jsonrpc": "2.0", "id": "0", "result": {"tools": []}}),
        json!({"jsonrpc": "2.0", "id": "0", "result": {"resultType": "input_required"}}),
        json!({"jsonrpc": "2.0", "id": "0", "error": {"code": -32022, "message": "private", "data": {"secret": true}}}),
    ];
    for value in cases {
        let endpoint = Endpoint::new(move |_, _| value.clone());
        let error = endpoint.client().list_tools().await.err().unwrap();
        assert!(!error.to_string().contains("private"));
        assert_eq!(endpoint.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn discovery_limits_and_cursor_cycles_do_not_return_partial_catalogs() {
    let endpoint = Endpoint::new(|request, _| {
        reply(
            request,
            json!({"resultType": "complete", "tools": [], "nextCursor": "loop"}),
        )
    });
    assert!(matches!(
        endpoint.client().list_tools().await,
        Err(Error::Protocol("repeated pagination cursor"))
    ));
    for limits in [
        Limits {
            request_bytes: 8,
            ..Limits::default()
        },
        Limits {
            response_bytes: 8,
            ..Limits::default()
        },
        Limits {
            catalog_bytes: 8,
            ..Limits::default()
        },
        Limits {
            max_pages: 1,
            ..Limits::default()
        },
    ] {
        assert!(matches!(
            endpoint
                .client()
                .with_limits(limits)
                .unwrap()
                .list_tools()
                .await,
            Err(Error::Limit(_))
        ));
    }
    let endpoint = Endpoint::new(|request, _| {
        reply(
            request,
            json!({"resultType": "complete", "tools": [definition("a"), definition("b")]}),
        )
    });
    assert!(matches!(
        endpoint
            .client()
            .with_limits(Limits {
                max_tools: 1,
                ..Limits::default()
            })
            .unwrap()
            .list_tools()
            .await,
        Err(Error::Limit("tool count"))
    ));
}
