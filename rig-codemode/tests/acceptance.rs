//! Acceptance suite from the design document, run against the QuickJS
//! backend with mock tools. No model or network is involved.

#![cfg(feature = "quickjs")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rig_codemode::{
    CallStatus, Catalog, CatalogEntry, CodeMode, DynamicToolDispatcher, ExecutionRequest,
    ExecutionStatus, HostDispatcher, Invocation, LimitOverrides, Limits, Presentation,
    ScriptDelivery,
};
use rig_core::completion::message::ToolResultContent;
use rig_core::tool::{
    DynamicTool, ToolContext, ToolErrorKind, ToolExecutionError, ToolOutput, ToolResult,
};
use serde_json::{Value, json};

/// Shared counters so tests can prove which tool bodies ran.
#[derive(Default)]
struct Effects {
    counter: AtomicUsize,
    validate_bodies: AtomicUsize,
    hang_started: AtomicUsize,
    hang_dropped: AtomicUsize,
}

struct DropCounter(Arc<Effects>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.hang_dropped.fetch_add(1, Ordering::SeqCst);
    }
}

fn schema() -> Value {
    json!({"type": "object", "additionalProperties": true})
}

fn tool<F, Fut>(name: &str, description: &str, body: F) -> DynamicTool
where
    F: Fn(Value) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<ToolOutput, ToolExecutionError>> + Send + 'static,
{
    DynamicTool::new(name, description, schema(), move |args| {
        Box::pin(body(args))
    })
}

fn tools(effects: Arc<Effects>) -> Vec<DynamicTool> {
    let counter = effects.clone();
    let validate = effects.clone();
    let hang = effects.clone();
    vec![
        tool(
            "slow",
            "Sleeps 200ms, echoes arguments",
            |args| async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(ToolOutput::json(json!({"from": "slow", "args": args})))
            },
        ),
        tool("fast", "Sleeps 10ms, echoes arguments", |args| async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(ToolOutput::json(json!({"from": "fast", "args": args})))
        }),
        tool(
            "text_tool",
            "Returns the literal text {\"x\":1}",
            |_| async { Ok(ToolOutput::text("{\"x\":1}")) },
        ),
        tool("json_tool", "Returns the JSON object {x:1}", |_| async {
            Ok(ToolOutput::json(json!({"x": 1})))
        }),
        tool(
            "json_string_tool",
            "Returns a JSON string block",
            |_| async { Ok(ToolOutput::json(json!("{\"x\":1}"))) },
        ),
        tool("mixed", "Returns text then JSON", |_| async {
            ToolOutput::content(vec![
                ToolResultContent::text("hello"),
                ToolResultContent::json(json!({"n": 2})),
            ])
        }),
        tool("refuse", "Always refuses", |_| async {
            Err(ToolExecutionError::refused(
                "the tool declined this request",
            ))
        }),
        tool("fail", "Fails with an operator-only detail", |_| async {
            Err(ToolExecutionError::other("db password is hunter2")
                .with_model_feedback("the tool failed"))
        }),
        tool(
            "secret",
            "Returns a token the result policy must remove",
            |_| async { Ok(ToolOutput::json(json!({"token": "SECRET-123", "ok": true}))) },
        ),
        tool(
            "validate",
            "Requires a non-negative integer n",
            move |args| {
                let effects = validate.clone();
                async move {
                    let n = args.get("n").and_then(Value::as_i64).ok_or_else(|| {
                        ToolExecutionError::invalid_args("`n` must be an integer")
                    })?;
                    if n < 0 {
                        return Err(ToolExecutionError::invalid_args("`n` must not be negative"));
                    }
                    effects.validate_bodies.fetch_add(1, Ordering::SeqCst);
                    Ok(ToolOutput::json(json!({"n": n})))
                }
            },
        ),
        tool("counter", "Increments a shared counter", move |_| {
            let effects = counter.clone();
            async move {
                let value = effects.counter.fetch_add(1, Ordering::SeqCst) + 1;
                Ok(ToolOutput::json(json!({"count": value})))
            }
        }),
        tool("big", "Returns `size` bytes of text", |args| async move {
            let size = args.get("size").and_then(Value::as_u64).unwrap_or(0) as usize;
            Ok(ToolOutput::text("x".repeat(size)))
        }),
        tool("hang", "Never completes", move |_| {
            let effects = hang.clone();
            async move {
                effects.hang_started.fetch_add(1, Ordering::SeqCst);
                let _guard = DropCounter(effects);
                std::future::pending::<()>().await;
                Ok(ToolOutput::text("unreachable"))
            }
        }),
        tool("denied", "Denied by the call policy", |_| async {
            Ok(ToolOutput::text("should never run"))
        }),
        tool("hidden", "Registered but not in the catalog", |_| async {
            Ok(ToolOutput::text("should never run"))
        }),
        tool("a-b", "dash", |_| async { Ok(ToolOutput::text("dash")) }),
        tool("a_b", "underscore", |_| async {
            Ok(ToolOutput::text("underscore"))
        }),
        tool("__proto__", "prototype-like name", |_| async {
            Ok(ToolOutput::text("proto"))
        }),
    ]
}

fn dispatcher(effects: Arc<Effects>) -> DynamicToolDispatcher {
    DynamicToolDispatcher::new(tools(effects))
        .unwrap()
        .with_call_policy(|invocation: &mut Invocation| {
            if invocation.name == "denied" {
                return Err(ToolExecutionError::refused(
                    "policy: `denied` is not allowed here",
                ));
            }
            if invocation.name == "validate"
                && invocation.arguments.get("rewrite").and_then(Value::as_bool) == Some(true)
            {
                invocation.arguments = json!({"n": -1});
            }
            Ok(())
        })
        .with_result_policy(|invocation: &Invocation, result: ToolResult| {
            if invocation.name != "secret" {
                return result;
            }
            let mut value = result.output().as_json().cloned().unwrap_or(Value::Null);
            if let Some(object) = value.as_object_mut() {
                object.remove("token");
            }
            result.with_output(ToolOutput::json(value))
        })
}

fn catalog(dispatcher: &DynamicToolDispatcher) -> Catalog {
    Catalog::new(
        dispatcher
            .definitions()
            .iter()
            .filter(|definition| definition.name != "hidden")
            .map(|definition| {
                let entry = CatalogEntry::from_definition(definition);
                if definition.name == "big" {
                    entry.with_presentation(Presentation::Deferred)
                } else {
                    entry
                }
            }),
    )
    .unwrap()
}

fn limits() -> Limits {
    Limits {
        wall_time: Duration::from_secs(5),
        max_calls: 32,
        max_in_flight: 4,
        message_bytes: 64 * 1024,
        output_bytes: 4 * 1024,
        ..Limits::default()
    }
}

struct Fixture {
    effects: Arc<Effects>,
    dispatcher: DynamicToolDispatcher,
    codemode: CodeMode,
}

fn fixture() -> Fixture {
    fixture_with(limits())
}

fn fixture_with(limits: Limits) -> Fixture {
    let effects = Arc::new(Effects::default());
    let dispatcher = dispatcher(effects.clone());
    let codemode = CodeMode::builder(catalog(&dispatcher), Arc::new(dispatcher.clone()))
        .limits(limits)
        .build()
        .unwrap();
    Fixture {
        effects,
        dispatcher,
        codemode,
    }
}

async fn run(codemode: &CodeMode, code: &str) -> rig_codemode::ExecutionReport {
    codemode.execute(ExecutionRequest::new(code)).await.unwrap()
}

fn status_of<'a>(report: &'a rig_codemode::ExecutionReport, name: &str) -> Vec<&'a CallStatus> {
    report
        .calls
        .iter()
        .filter(|call| call.name == name)
        .map(|call| &call.status)
        .collect()
}

#[tokio::test]
async fn concurrent_calls_stay_paired_and_ordered() {
    let f = fixture();
    let report = run(
        &f.codemode,
        r#"
        const [a, b] = await Promise.all([tools.slow({ id: "A" }), tools.fast({ id: "B" })]);
        return [a, b];
        "#,
    )
    .await;
    assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
    let returned = report.returned.clone().unwrap();
    assert_eq!(returned[0]["from"], "slow");
    assert_eq!(returned[0]["args"]["id"], "A");
    assert_eq!(returned[1]["from"], "fast");
    assert_eq!(returned[1]["args"]["id"], "B");
    assert_eq!(report.calls.len(), 2);
    assert_eq!(report.calls[0].name, "slow");
    assert_eq!(report.calls[1].name, "fast");
    assert!(
        report
            .calls
            .iter()
            .all(|c| c.status == CallStatus::Succeeded)
    );
}

#[tokio::test]
async fn all_settled_keeps_success_and_marks_refusal() {
    let f = fixture();
    let report = run(
        &f.codemode,
        r#"
        const results = await Promise.allSettled([tools.json_tool(), tools.refuse()]);
        return results.map(r => r.status === "fulfilled"
            ? { ok: r.value }
            : { name: r.reason.name, kind: r.reason.kind, status: r.reason.status,
                tool: r.reason.tool, message: r.reason.message, isString: typeof r.reason === "string" });
        "#,
    )
    .await;
    assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
    let returned = report.returned.clone().unwrap();
    assert_eq!(returned[0], json!({"ok": {"x": 1}}));
    assert_eq!(returned[1]["name"], "CodeModeToolError");
    assert_eq!(returned[1]["kind"], "permission_denied");
    assert_eq!(returned[1]["status"], "denied");
    assert_eq!(returned[1]["tool"], "refuse");
    assert_eq!(returned[1]["message"], "the tool declined this request");
    assert_eq!(returned[1]["isString"], false);
    assert_eq!(status_of(&report, "refuse"), vec![&CallStatus::Refused]);
    assert_eq!(
        report
            .calls
            .iter()
            .find(|c| c.name == "refuse")
            .unwrap()
            .error_kind,
        Some(ToolErrorKind::PermissionDenied)
    );
}

#[tokio::test]
async fn text_stays_text_and_json_stays_structured() {
    let f = fixture();
    let report = run(
        &f.codemode,
        r#"
        const t = await tools.text_tool();
        const j = await tools.json_tool();
        const s = await tools.json_string_tool();
        const m = await tools.mixed();
        return { t, tType: typeof t, j, jType: typeof j, s, sType: typeof s, m };
        "#,
    )
    .await;
    assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
    let r = report.returned.clone().unwrap();
    assert_eq!(r["tType"], "string");
    assert_eq!(r["t"], "{\"x\":1}");
    assert_eq!(r["jType"], "object");
    assert_eq!(r["j"], json!({"x": 1}));
    assert_eq!(
        r["sType"], "string",
        "a JSON string block is the string value"
    );
    assert_eq!(
        r["m"]["content"][0],
        json!({"type": "text", "text": "hello"})
    );
    assert_eq!(
        r["m"]["content"][1],
        json!({"type": "json", "value": {"n": 2}})
    );
}

#[tokio::test]
async fn invalid_arguments_and_policy_rewrite_are_revalidated() {
    let f = fixture();
    let report = run(
        &f.codemode,
        r#"
        const out = [];
        try { await tools.validate({ n: "seven" }); } catch (e) { out.push([e.kind, e.message]); }
        try { await tools.validate({ n: 3, rewrite: true }); } catch (e) { out.push([e.kind, e.message]); }
        out.push(await tools.validate({ n: 4 }));
        try { await tools.validate(); } catch (e) { out.push([e.kind, e.message]); }
        return out;
        "#,
    )
    .await;
    assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
    let r = report.returned.clone().unwrap();
    assert_eq!(r[0], json!(["invalid_args", "`n` must be an integer"]));
    assert_eq!(
        r[1],
        json!(["invalid_args", "`n` must not be negative"]),
        "the rewritten arguments were validated by the tool"
    );
    assert_eq!(r[2], json!({"n": 4}));
    assert_eq!(
        r[3][0], "invalid_args",
        "undefined arguments become {{}} and still validate"
    );
    assert_eq!(
        status_of(&report, "validate")
            .iter()
            .filter(|s| ***s == CallStatus::Failed)
            .count(),
        3
    );
    assert_eq!(f.effects.validate_bodies.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn direct_and_nested_denials_match() {
    let f = fixture();
    let direct = f
        .dispatcher
        .dispatch(Invocation::new(
            "direct",
            0,
            "denied",
            json!({}),
            ToolContext::new(),
        ))
        .await;
    let direct_reason = direct.result.refusal().unwrap().model_output().render();

    let report = run(
        &f.codemode,
        r#"try { await tools.denied({}); return "ran"; } catch (e) { return { status: e.status, message: e.message }; }"#,
    )
    .await;
    let nested = report.returned.clone().unwrap();
    assert_eq!(nested["status"], "denied");
    assert_eq!(nested["message"], direct_reason);
    assert_eq!(direct_reason, "policy: `denied` is not allowed here");
    assert_eq!(status_of(&report, "denied"), vec![&CallStatus::Refused]);
}

#[tokio::test]
async fn result_policy_redaction_holds_everywhere() {
    let f = fixture();
    let report = run(
        &f.codemode,
        r#"
        const normal = await tools.secret();
        const raw = await tools.secret.raw();
        let failure;
        try { await tools.fail(); } catch (e) { failure = { message: e.message, stack: e.stack }; }
        const rawFailure = await tools.fail.raw();
        text(JSON.stringify({ normal, raw, failure, rawFailure }));
        return { normal, raw, failure, rawFailure };
        "#,
    )
    .await;
    assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
    let serialized = serde_json::to_string(&report).unwrap();
    assert!(!serialized.contains("SECRET-123"), "{serialized}");
    assert!(!serialized.contains("hunter2"), "{serialized}");
    let r = report.returned.clone().unwrap();
    assert_eq!(r["normal"], json!({"ok": true}));
    assert_eq!(r["raw"]["status"], "success");
    assert_eq!(r["raw"]["content"][0]["value"], json!({"ok": true}));
    assert_eq!(r["failure"]["message"], "the tool failed");
    assert_eq!(r["rawFailure"]["status"], "error");
    assert_eq!(r["rawFailure"]["error"]["message"], "the tool failed");
    assert_eq!(r["rawFailure"]["error"]["kind"], "other");
}

#[tokio::test]
async fn raw_mode_is_a_separate_invocation() {
    let f = fixture();
    let report = run(
        &f.codemode,
        r#"
        const first = await tools.counter();
        const second = await tools.counter.raw();
        return [first.count, second.content[0].value.count];
        "#,
    )
    .await;
    assert_eq!(report.returned.clone().unwrap(), json!([1, 2]));
    assert_eq!(f.effects.counter.load(Ordering::SeqCst), 2);
    assert_eq!(report.calls.len(), 2);
}

#[tokio::test]
async fn hidden_tool_is_undiscoverable_and_uncallable() {
    let f = fixture();
    let report = run(
        &f.codemode,
        r#"
        const found = searchTools("registered catalog").map(t => t.name);
        const described = describeTool("hidden");
        let error;
        try { await tools["hidden"]({}); } catch (e) { error = e.constructor.name; }
        return { found, described, error, has: "hidden" in tools };
        "#,
    )
    .await;
    assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
    let r = report.returned.clone().unwrap();
    assert_eq!(r["found"], json!([]));
    assert_eq!(r["described"], Value::Null);
    assert_eq!(r["error"], "TypeError");
    assert_eq!(r["has"], false);
    assert!(
        report.calls.is_empty(),
        "no dispatch happened: {:?}",
        report.calls
    );
    assert!(!f.codemode.description().contains("hidden"));
}

#[tokio::test]
async fn exact_names_route_without_aliases_or_inherited_capabilities() {
    let f = fixture();
    let report = run(
        &f.codemode,
        r#"
        return {
            dash: await tools["a-b"](),
            underscore: await tools["a_b"](),
            proto: await tools["__proto__"](),
            protoIsOwn: Object.prototype.hasOwnProperty.call(tools, "__proto__"),
            ctor: typeof tools["constructor"],
            toString: typeof tools["toString"],
            hasOwn: typeof tools["hasOwnProperty"],
            frozen: Object.isFrozen(tools),
            proto_of_tools: Object.getPrototypeOf(tools),
        };
        "#,
    )
    .await;
    assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
    let r = report.returned.clone().unwrap();
    assert_eq!(r["dash"], "dash");
    assert_eq!(r["underscore"], "underscore");
    assert_eq!(r["proto"], "proto");
    assert_eq!(r["protoIsOwn"], true);
    assert_eq!(r["ctor"], "undefined");
    assert_eq!(r["toString"], "undefined");
    assert_eq!(r["hasOwn"], "undefined");
    assert_eq!(r["frozen"], true);
    assert_eq!(r["proto_of_tools"], Value::Null);
    let names: Vec<&str> = report.calls.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["a-b", "a_b", "__proto__"]);
}

#[tokio::test]
async fn recursive_codemode_is_rejected_at_build() {
    let effects = Arc::new(Effects::default());
    let dispatcher = dispatcher(effects);
    let mut entries: Vec<CatalogEntry> = catalog(&dispatcher).into();
    entries.push(CatalogEntry::new("codemode", "recursive", schema()));
    let catalog = Catalog::new(entries).unwrap();
    let error = CodeMode::builder(catalog, Arc::new(dispatcher))
        .build()
        .unwrap_err();
    assert!(
        matches!(error, rig_codemode::BuildError::RecursiveTool(_)),
        "{error}"
    );
}

#[tokio::test]
async fn print_then_throw_keeps_partial_output_and_effects() {
    let f = fixture();
    let report = run(
        &f.codemode,
        "text(\"before\");\nawait tools.counter();\nthrow new Error(\"boom\");\ntext(\"after\");",
    )
    .await;
    assert_eq!(report.status, ExecutionStatus::ScriptError);
    assert_eq!(report.output.text, "before\n");
    let diagnostic = report.diagnostic.as_ref().unwrap();
    assert_eq!(diagnostic.message, "boom");
    assert_eq!(diagnostic.line, Some(3), "{diagnostic:?}");
    assert_eq!(
        f.effects.counter.load(Ordering::SeqCst),
        1,
        "completed effects stay"
    );
    assert_eq!(status_of(&report, "counter"), vec![&CallStatus::Succeeded]);
    let rendered = report.render_text();
    assert!(
        rendered.starts_with("before\nScript failed at line 3"),
        "{rendered}"
    );
    assert!(
        rendered.contains("Tool calls: 1 total, 1 succeeded"),
        "{rendered}"
    );
}

#[tokio::test]
async fn cpu_and_microtask_loops_hit_the_deadline() {
    let f = fixture_with(Limits {
        wall_time: Duration::from_millis(300),
        ..limits()
    });
    for code in [
        "while (true) {}",
        "while (true) { await null; }",
        "for (;;) { await Promise.resolve(); }",
    ] {
        let started = Instant::now();
        let report = run(&f.codemode, code).await;
        let elapsed = started.elapsed();
        assert_eq!(
            report.status,
            ExecutionStatus::TimedOut,
            "{code}: {report:?}"
        );
        assert!(elapsed < Duration::from_secs(2), "{code} took {elapsed:?}");
    }
    let report = run(&f.codemode, "return 1 + 1").await;
    assert_eq!(report.returned, Some(json!(2)), "the executor still works");
}

#[tokio::test]
async fn resource_exhaustion_fails_in_bounds_and_runtime_survives() {
    let f = fixture_with(Limits {
        wall_time: Duration::from_secs(5),
        memory_bytes: 32 * 1024 * 1024,
        message_bytes: 16 * 1024,
        ..limits()
    });
    let cases = [
        (
            "huge array",
            "const a = []; while (true) { a.push(new Array(1024).fill(1)); }",
        ),
        (
            "deep recursion",
            "function f(n) { return f(n + 1) + 1; } return f(0);",
        ),
        (
            "huge string",
            "let s = 'x'; for (let i = 0; i < 40; i++) { s = s + s; } return s.length;",
        ),
    ];
    for (label, code) in cases {
        let started = Instant::now();
        let report = run(&f.codemode, code).await;
        assert_eq!(
            report.status,
            ExecutionStatus::ScriptError,
            "{label}: {report:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(5), "{label}");
        let message = &report.diagnostic.as_ref().unwrap().message;
        assert!(
            message.contains("out of memory")
                || message.contains("stack")
                || message.contains("string length")
                || message.contains("string too long"),
            "{label}: {message}"
        );
    }
    let report = run(
        &f.codemode,
        r#"try { await tools.big({ size: 20000 }); return "delivered"; } catch (e) { return { status: e.status, kind: e.kind }; }"#,
    )
    .await;
    assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
    assert_eq!(
        report.returned.clone().unwrap(),
        json!({"status": "size", "kind": "invalid_args"})
    );
    assert_eq!(report.calls[0].status, CallStatus::Succeeded);
    assert_eq!(report.calls[0].delivery, ScriptDelivery::Oversized);

    let report = run(&f.codemode, r#"let e; try { await tools.big({ size: "x".repeat(20000) }); } catch (err) { e = err.status; } return e;"#).await;
    assert_eq!(
        report.returned.clone().unwrap(),
        "size",
        "oversized arguments never reach the host"
    );
    assert_eq!(report.calls[0].status, CallStatus::Rejected);

    let report = run(&f.codemode, "return (await tools.big({ size: 10 })).length").await;
    assert_eq!(report.returned, Some(json!(10)), "the executor still works");
}

#[tokio::test]
async fn unsettleable_promise_stalls_instead_of_hanging() {
    let f = fixture_with(Limits {
        wall_time: Duration::from_secs(10),
        ..limits()
    });
    let started = Instant::now();
    let report = run(&f.codemode, "await new Promise(() => {}); return 1;").await;
    assert_eq!(report.status, ExecutionStatus::Stalled, "{report:?}");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(report.diagnostic.unwrap().message.contains("settle"));
}

#[tokio::test]
async fn thrown_diagnostics_share_a_utf8_safe_budget() {
    let f = fixture_with(Limits {
        message_bytes: 103,
        output_bytes: 101,
        ..limits()
    });
    for source in [
        "throw 'é'.repeat(1024 * 1024)",
        "throw new Error('é'.repeat(1024 * 1024))",
        "const e = new Error('small'); e.name = 'é'.repeat(1024 * 1024); e.stack = '界'.repeat(1024 * 1024); throw e;",
    ] {
        let report = run(&f.codemode, source).await;
        assert_eq!(report.status, ExecutionStatus::ScriptError, "{report:?}");
        let diagnostic = report.diagnostic.unwrap();
        assert!(diagnostic.message.len() + diagnostic.stack.as_deref().unwrap_or("").len() <= 101);
        assert!(!diagnostic.message.is_empty());
    }
}

#[tokio::test]
async fn argument_serialization_rejects_promises_and_preserves_ordinals() {
    let f = fixture();
    for argument in [
        "1n",
        "(() => { const a = {}; a.self = a; return a; })()",
        "({toJSON() { throw new Error('bad json'); }})",
    ] {
        let report = run(
            &f.codemode,
            &format!(
                r#"
            let synchronous = false, pending, failure;
            try {{ pending = tools.fast({argument}); }} catch(e) {{ synchronous = true; }}
            try {{ await pending; }} catch(e) {{ failure = [e.name, e.status, e.kind]; }}
            await tools.fast({{}});
            return {{synchronous, failure}};
        "#
            ),
        )
        .await;
        assert_eq!(
            report.returned,
            Some(
                json!({"synchronous": false, "failure": ["CodeModeToolError", "rejected", "invalid_args"]})
            )
        );
        assert_eq!(
            report
                .calls
                .iter()
                .map(|call| (call.ordinal, call.status))
                .collect::<Vec<_>>(),
            [(0, CallStatus::Rejected), (1, CallStatus::Succeeded)]
        );
    }
}

#[tokio::test]
async fn discovery_bounds_queries_and_serialized_responses() {
    let f = fixture_with(Limits {
        message_bytes: 100,
        ..limits()
    });
    for expression in [
        "searchTools('fast '.repeat(100000))",
        "searchTools('fast '.repeat(33))",
        "searchTools('tool', {limit: 50})",
        "describeTool('fast')",
    ] {
        let report = run(
            &f.codemode,
            &format!("try {{ {expression}; return 'unbounded'; }} catch(e) {{ return e.name; }}"),
        )
        .await;
        assert_eq!(
            report.returned,
            Some(json!("RangeError")),
            "{expression}: {report:?}"
        );
    }
}

#[tokio::test]
async fn unawaited_call_and_early_return_request_cancellation() {
    let f = fixture();
    let report = run(
        &f.codemode,
        r#"
        tools.hang({});
        const p = tools.slow({ id: 1 });
        return "early";
        "#,
    )
    .await;
    assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
    assert_eq!(report.returned, Some(json!("early")));
    assert_eq!(report.calls.len(), 2);
    for call in &report.calls {
        assert!(
            matches!(
                call.status,
                CallStatus::CancellationRequested | CallStatus::NotStarted
            ),
            "{call:?}"
        );
        assert_eq!(call.delivery, ScriptDelivery::None);
    }
    let started = f.effects.hang_started.load(Ordering::SeqCst);
    assert!(started <= 1);
    assert_eq!(
        f.effects.hang_dropped.load(Ordering::SeqCst),
        started,
        "the in-flight future was dropped"
    );
}

#[tokio::test]
async fn queued_calls_do_not_start_after_cancellation() {
    let f = fixture_with(Limits {
        max_in_flight: 2,
        max_calls: 32,
        ..limits()
    });
    let report = run(
        &f.codemode,
        r#"
        const calls = [];
        for (let i = 0; i < 10; i++) calls.push(tools.hang({ i }));
        tools.fast({});
        return "done";
        "#,
    )
    .await;
    assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
    let started = f.effects.hang_started.load(Ordering::SeqCst);
    assert!(started <= 2, "at most max_in_flight started");
    assert_eq!(f.effects.hang_dropped.load(Ordering::SeqCst), started);
    assert_eq!(report.calls.len(), 11);
    assert!(
        status_of(&report, "hang")
            .iter()
            .filter(|s| ***s == CallStatus::CancellationRequested)
            .count()
            <= 2
    );
    assert!(
        status_of(&report, "hang")
            .iter()
            .filter(|s| ***s == CallStatus::NotStarted)
            .count()
            >= 8
    );
    assert_eq!(
        status_of(&report, "fast"),
        vec![&CallStatus::NotStarted],
        "fast queued behind hangs never ran"
    );

    // Caller cancellation: dropping the execute future.
    let before = f.effects.hang_started.load(Ordering::SeqCst);
    let future = f.codemode.execute(ExecutionRequest::new(
        "for (let i = 0; i < 10; i++) tools.hang({ i }); await new Promise(() => {});",
    ));
    let timed_out = tokio::time::timeout(Duration::from_millis(300), future).await;
    assert!(timed_out.is_err(), "the caller cancelled first");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(f.effects.hang_started.load(Ordering::SeqCst), before + 2);
    assert_eq!(
        f.effects.hang_dropped.load(Ordering::SeqCst),
        before + 2,
        "in-flight work was cancelled"
    );
}

#[tokio::test]
async fn utf8_output_truncates_cleanly() {
    let f = fixture_with(Limits {
        output_bytes: 10,
        ..limits()
    });
    let report = run(&f.codemode, r#"text("ééééé"); text("more"); return 1;"#).await;
    assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
    assert!(report.output.truncated);
    assert!(report.output.text.len() <= 10);
    assert!(report.output.text.starts_with("éééé"));
    assert!(
        report
            .render_text()
            .contains("[output truncated at the host limit]")
    );

    let report = run(&f.codemode, r#"let e; try { text({ a: "ééééééé" }); } catch (err) { e = String(err); } return [e, typeof e];"#).await;
    let r = report.returned.clone().unwrap();
    assert!(r[0].as_str().unwrap().contains("RangeError"), "{r}");
    assert!(
        !report.output.truncated,
        "structured values are rejected whole, not cut"
    );
    assert_eq!(report.output.text, "");

    let report = run(&f.codemode, r#"text("a"); text({ a: 1 }); return 1;"#).await;
    assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
    assert_eq!(report.output.text, "a\n{\"a\":1}\n");
    assert!(!report.output.truncated);
}

#[tokio::test]
async fn sessions_share_nothing() {
    let f = fixture();
    let first = run(
        &f.codemode,
        "globalThis.leak = 42; Object.prototype.polluted = true; return 1;",
    )
    .await;
    assert_eq!(first.status, ExecutionStatus::Completed, "{first:?}");
    let second = run(&f.codemode, "return [typeof leak, typeof ({}).polluted];").await;
    assert_eq!(second.returned, Some(json!(["undefined", "undefined"])));

    let (a, b) = tokio::join!(
        run(
            &f.codemode,
            "globalThis.x = 'a'; await tools.slow({}); return x;"
        ),
        run(
            &f.codemode,
            "globalThis.x = 'b'; await tools.fast({}); return x;"
        ),
    );
    assert_eq!(a.returned, Some(json!("a")));
    assert_eq!(b.returned, Some(json!("b")));
}

#[tokio::test]
async fn guest_has_no_io_or_module_access() {
    let f = fixture();
    let report = run(
        &f.codemode,
        r#"
        const names = ["fetch", "require", "process", "setTimeout", "setInterval", "std", "os",
                       "Deno", "XMLHttpRequest", "WebSocket", "Worker", "__host"];
        const types = Object.fromEntries(names.map(n => [n, typeof globalThis[n]]));
        let dynamicImport;
        try { await import("os"); dynamicImport = "loaded"; } catch (e) { dynamicImport = e.constructor.name; }
        const indirect = new Function("return this")();
        return { types, dynamicImport, sameGlobal: indirect === globalThis, hostOnIndirect: typeof indirect.__host };
        "#,
    )
    .await;
    assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
    let r = report.returned.clone().unwrap();
    for (name, kind) in r["types"].as_object().unwrap() {
        assert_eq!(kind, "undefined", "{name} is reachable");
    }
    assert_ne!(r["dynamicImport"], "loaded");
    assert_eq!(r["sameGlobal"], true);
    assert_eq!(r["hostOnIndirect"], "undefined");
}

#[tokio::test]
async fn discovery_searches_only_the_catalog_snapshot() {
    let f = fixture();
    let report = run(
        &f.codemode,
        r#"
        const found = searchTools("sleeps echoes arguments", { limit: 2 }).map(t => t.name);
        const all = searchTools("sleeps").length;
        const described = describeTool("validate");
        const capped = searchTools("tool", { limit: 1000 }).length;
        return { found, all, described, capped };
        "#,
    )
    .await;
    assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
    let r = report.returned.clone().unwrap();
    assert_eq!(r["found"].as_array().unwrap().len(), 2);
    assert!(
        r["found"]
            .as_array()
            .unwrap()
            .iter()
            .all(|n| n == "slow" || n == "fast"),
        "{r}"
    );
    assert_eq!(r["all"], 2);
    assert_eq!(r["described"]["name"], "validate");
    assert_eq!(
        r["described"]["description"],
        "Requires a non-negative integer n"
    );
    assert!(r["described"]["inputSchema"].is_object());
    assert!(r["capped"].as_u64().unwrap() <= 50);
    assert!(report.calls.is_empty(), "discovery is not a tool call");

    let description = f.codemode.description();
    assert!(description.contains("\"slow\"(args:"), "{description}");
    assert!(
        !description.contains("\"big\"(args:"),
        "deferred entries stay out of the prompt"
    );
    assert!(description.contains("more tool(s) are callable but not listed"));
}

#[tokio::test]
async fn call_budget_bounds_records() {
    let f = fixture_with(Limits {
        max_calls: 3,
        max_in_flight: 3,
        ..limits()
    });
    let report = run(
        &f.codemode,
        r#"
        const results = await Promise.allSettled([1, 2, 3, 4, 5].map(() => tools.counter()));
        return results.map(r => r.status === "fulfilled" ? "ok" : r.reason.status);
        "#,
    )
    .await;
    assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
    assert_eq!(
        report.returned.clone().unwrap(),
        json!(["ok", "ok", "ok", "rejected", "rejected"])
    );
    assert_eq!(report.calls.len(), 3);
    assert_eq!(report.over_budget_calls, 2);
    assert_eq!(f.effects.counter.load(Ordering::SeqCst), 3);
    assert!(report.call_summary().contains("2 over budget"));
}

#[tokio::test]
async fn request_overrides_only_lower_limits() {
    let f = fixture();
    let error = f
        .codemode
        .execute(
            ExecutionRequest::new("return 1").with_limits(LimitOverrides {
                wall_time: Some(Duration::from_secs(60)),
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, rig_codemode::ExecutionError::Limits(_)),
        "{error}"
    );

    let report = f
        .codemode
        .execute(
            ExecutionRequest::new("while (true) {}").with_limits(LimitOverrides {
                wall_time: Some(Duration::from_millis(200)),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(report.status, ExecutionStatus::TimedOut);

    let too_large = f
        .codemode
        .execute(ExecutionRequest::new(
            "x".repeat(f.codemode.limits().source_bytes + 1),
        ))
        .await
        .unwrap_err();
    assert!(matches!(
        too_large,
        rig_codemode::ExecutionError::SourceTooLarge { .. }
    ));
}

#[tokio::test]
async fn syntax_errors_are_reported_safely() {
    let f = fixture();
    let report = run(&f.codemode, "const = ;").await;
    assert_eq!(report.status, ExecutionStatus::ScriptError, "{report:?}");
    let diagnostic = report.diagnostic.unwrap();
    assert!(
        diagnostic.message.starts_with("SyntaxError"),
        "{diagnostic:?}"
    );
    assert_eq!(diagnostic.line, Some(1));
}

#[tokio::test]
async fn rig_tool_surface_maps_statuses() {
    let f = fixture();
    let tool = f.codemode.tool();
    assert_eq!(tool.name(), "codemode");
    let definition = tool.definition();
    assert_eq!(definition.parameters["required"], json!(["code"]));

    let output = tool
        .execute(json!({"code": "text(await tools.json_tool()); return 'r';"}))
        .await
        .unwrap();
    assert_eq!(
        output.as_text().unwrap(),
        "{\"x\":1}\nReturn value:\n\"r\"\nTool calls: 1 total, 1 succeeded\n"
    );

    let error = tool
        .execute(json!({"code": "text('partial'); throw new Error('nope');"}))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ToolErrorKind::Other);
    assert_eq!(error.retryable(), Some(false));
    let feedback = error.model_feedback().unwrap();
    assert!(
        feedback.starts_with("partial\nScript failed at line 1"),
        "{feedback}"
    );

    let error = tool
        .execute(json!({"code": "while(true){}"}))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ToolErrorKind::Timeout);
    assert_eq!(error.retryable(), Some(false));
    assert!(
        error.model_feedback().unwrap().contains("Script timed out"),
        "{error:?}"
    );

    let invalid = tool.execute(json!({"source": "x"})).await.unwrap_err();
    assert_eq!(invalid.kind(), ToolErrorKind::InvalidArgs);
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ApiToken(String);

impl rig_core::tool::ContextValue for ApiToken {
    const KEY: &'static str = "acceptance.api_token";
}

/// A dispatcher whose `whoami` tool reports the token it received from the
/// per-call context. The token must come from the script grant.
fn credentialed_fixture<P: rig_codemode::ScriptPolicy>(policy: P) -> (Arc<Effects>, CodeMode) {
    let effects = Arc::new(Effects::default());
    let mut all = tools(effects.clone());
    all.push(DynamicTool::new_with_context(
        "whoami",
        "Reports the api token from the context",
        schema(),
        |context: &mut ToolContext, _args: Value| {
            let token = context.get::<ApiToken>().unwrap().map(|t| t.0);
            Box::pin(async move { Ok(ToolOutput::json(json!({ "token": token }))) })
        },
    ));
    let dispatcher = DynamicToolDispatcher::new(all).unwrap();
    let codemode = CodeMode::builder(catalog(&dispatcher), Arc::new(dispatcher))
        .limits(limits())
        .script_policy(policy)
        .build()
        .unwrap();
    (effects, codemode)
}

#[tokio::test]
async fn script_grant_bounds_dynamic_access_and_keeps_literal_calls() {
    let (effects, codemode) = credentialed_fixture(|review: rig_codemode::ScriptReview<'_>| {
        assert_eq!(
            review.analysis.tools.iter().collect::<Vec<_>>(),
            ["fast", "json_tool"]
        );
        assert!(review.analysis.dynamic_tool_access);
        Ok(review.grant_referenced())
    });
    let report = codemode
        .execute(ExecutionRequest::new(
            r#"
            const name = ["coun", "ter"].join("");
            const a = await tools.fast({ id: 1 });
            const b = await tools["json_tool"]({});
            let denied;
            try { await tools[name]({}); } catch (e) { denied = { status: e.status, kind: e.kind, tool: e.tool, message: e.message }; }
            const raw = await tools[name].raw({});
            return { a: a.from, b, denied, raw: raw.status };
            "#,
        ))
        .await
        .unwrap();
    assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
    let returned = report.returned.clone().unwrap();
    assert_eq!(returned["a"], "fast");
    assert_eq!(returned["b"], json!({"x": 1}));
    assert_eq!(
        returned["denied"],
        json!({
            "status": "denied",
            "kind": "permission_denied",
            "tool": "counter",
            "message": "tool \"counter\" is not granted to this script",
        })
    );
    assert_eq!(returned["raw"], "denied");
    assert_eq!(
        effects.counter.load(Ordering::SeqCst),
        0,
        "counter body must not run"
    );
    assert_eq!(
        status_of(&report, "counter"),
        vec![&CallStatus::Refused, &CallStatus::Refused]
    );
    assert_eq!(status_of(&report, "fast"), vec![&CallStatus::Succeeded]);
}

#[tokio::test]
async fn script_policy_refusal_runs_nothing() {
    let (effects, codemode) = credentialed_fixture(|review: rig_codemode::ScriptReview<'_>| {
        if review.analysis.tools.contains("counter") {
            return Err(ToolExecutionError::refused(
                "scripts that call `counter` need approval",
            ));
        }
        Ok(rig_codemode::ScriptGrant::only(["fast"], review.context))
    });
    let error = codemode
        .execute(ExecutionRequest::new(
            "await tools.counter({}); text('ran');",
        ))
        .await
        .unwrap_err();
    assert!(
        matches!(error, rig_codemode::ExecutionError::Refused(_)),
        "{error:?}"
    );
    assert_eq!(effects.counter.load(Ordering::SeqCst), 0);

    for code in [
        r#"await globalThis["tools"].counter()"#,
        r#"await eval("tools.counter()")"#,
        r#"/"/; await tools.counter()"#,
    ] {
        let report = codemode.execute(ExecutionRequest::new(code)).await.unwrap();
        assert_eq!(report.calls[0].status, CallStatus::Refused, "{report:?}");
    }
    assert_eq!(effects.counter.load(Ordering::SeqCst), 0);

    let tool_error = codemode
        .tool()
        .execute(json!({"code": "await tools.counter({});"}))
        .await
        .unwrap_err();
    assert_eq!(tool_error.kind(), ToolErrorKind::PermissionDenied);
    assert_eq!(
        tool_error.model_output().render(),
        "scripts that call `counter` need approval"
    );

    let report = codemode
        .execute(ExecutionRequest::new("return (await tools.fast({})).from;"))
        .await
        .unwrap();
    assert_eq!(report.returned, Some(json!("fast")));
}

#[tokio::test]
async fn script_grant_context_reaches_every_child_call() {
    let (_, codemode) = credentialed_fixture(|mut review: rig_codemode::ScriptReview<'_>| {
        if review.analysis.tools.contains("whoami") {
            review.context.insert(ApiToken("tok-42".into())).unwrap();
        }
        Ok(review.grant_referenced())
    });
    let report = codemode
        .execute(ExecutionRequest::new(
            "return [await tools.whoami({}), await tools.whoami({})].map(r => r.token);",
        ))
        .await
        .unwrap();
    assert_eq!(report.returned, Some(json!(["tok-42", "tok-42"])));

    let report = codemode
        .execute(ExecutionRequest::new("return (await tools.fast({})).from;"))
        .await
        .unwrap();
    assert_eq!(report.returned, Some(json!("fast")));
}

#[tokio::test]
async fn generated_parent_ids_are_unique_across_executors() {
    let ids = Arc::new(std::sync::Mutex::new(Vec::new()));
    for _ in 0..2 {
        let captured = ids.clone();
        let (_, codemode) = credentialed_fixture(move |review: rig_codemode::ScriptReview<'_>| {
            captured
                .lock()
                .unwrap()
                .push(review.parent_call_id.to_owned());
            Ok(review.grant_referenced())
        });
        codemode
            .execute(ExecutionRequest::new("return 1"))
            .await
            .unwrap();
    }
    let ids = ids.lock().unwrap();
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
}

#[tokio::test]
async fn async_script_policy_can_await_an_approval() {
    struct Approver(tokio::sync::Mutex<tokio::sync::mpsc::Receiver<bool>>);

    impl rig_codemode::ScriptPolicy for Approver {
        fn review<'a>(
            &'a self,
            review: rig_codemode::ScriptReview<'a>,
        ) -> rig_codemode::ScriptPolicyFuture<'a> {
            Box::pin(async move {
                let approved = self.0.lock().await.recv().await.unwrap_or(false);
                if approved {
                    Ok(review.grant_referenced())
                } else {
                    Err(ToolExecutionError::refused("the operator declined"))
                }
            })
        }
    }

    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let (_, codemode) = credentialed_fixture(Approver(tokio::sync::Mutex::new(rx)));
    let pending = codemode.execute(ExecutionRequest::new("return (await tools.fast({})).from;"));
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut pending)
            .await
            .is_err(),
        "the script must wait for the approval"
    );
    tx.send(true).await.unwrap();
    let report = pending.await.unwrap();
    assert_eq!(report.returned, Some(json!("fast")));

    tx.send(false).await.unwrap();
    let error = codemode
        .execute(ExecutionRequest::new("return 1;"))
        .await
        .unwrap_err();
    assert!(matches!(error, rig_codemode::ExecutionError::Refused(_)));
}
