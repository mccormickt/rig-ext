#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#![allow(clippy::unwrap_used, clippy::arc_with_non_send_sync)]

use std::{cell::Cell, rc::Rc, sync::Arc, time::Duration};

use rig_codemode::{
    BuildError, Catalog, CodeMode, DynamicToolDispatcher, ExecutionReport, ExecutionStatus,
    HostDispatcher, Invocation, ScriptDiagnostic, ScriptGrant, ScriptPolicy, ScriptPolicyFuture,
    ScriptReview, analyze,
};
use rig_core::{
    message::ToolName,
    tool::{ContextValue, DynamicTool, ToolContext, ToolErrorKind, ToolExecutionError, ToolOutput},
    wasm_compat::sleep,
};
use serde_json::json;
use wasm_bindgen_test::wasm_bindgen_test;

struct LocalApproval(Rc<Cell<bool>>);

impl ScriptPolicy for LocalApproval {
    fn review<'a>(&'a self, review: ScriptReview<'a>) -> ScriptPolicyFuture<'a> {
        Box::pin(async move {
            sleep(Duration::from_millis(1)).await;
            if self.0.get() {
                Ok(ScriptGrant::only(["lookup"], review.context))
            } else {
                Err(ToolExecutionError::refused("approval denied"))
            }
        })
    }
}

#[wasm_bindgen_test]
async fn local_callbacks_keep_dispatch_and_policy_guarantees() {
    let calls = Rc::new(Cell::new(0));
    let counter = calls.clone();
    let tool = DynamicTool::new(
        ToolName::new("lookup").unwrap(),
        "",
        json!({"type": "object", "properties": {"id": {"type": "integer"}}, "required": ["id"]}),
        move |args| {
            let counter = counter.clone();
            Box::pin(async move {
                sleep(Duration::from_millis(1)).await;
                counter.set(counter.get() + 1);
                Ok(ToolOutput::json(
                    json!({"id": args["id"], "secret": "hidden"}),
                ))
            })
        },
    );
    let private_id = Rc::new(Cell::new(7));
    let redaction = Rc::new(Cell::new(true));
    let dispatcher: Arc<dyn HostDispatcher> = Arc::new(
        DynamicToolDispatcher::new([tool])
            .unwrap()
            .with_call_policy(move |call| {
                if call.arguments["id"] == private_id.get() {
                    return Err(ToolExecutionError::refused("private record"));
                }
                if call.arguments["rewrite"] == true {
                    call.arguments = json!({"id": "invalid rewrite"});
                }
                Ok(())
            })
            .with_result_policy(move |_, result| {
                if redaction.get() {
                    let id = result.output().as_json().unwrap()["id"].clone();
                    result.with_output(ToolOutput::json(json!({"id": id})))
                } else {
                    result
                }
            }),
    );
    let allowed = dispatcher
        .dispatch(Invocation::new(
            "p",
            0,
            "lookup",
            json!({"id": 3}),
            ToolContext::new(),
        ))
        .await;
    assert_eq!(allowed.result.output().as_json(), Some(&json!({"id": 3})));
    assert!(allowed.result.is_success());
    for (arguments, kind) in [
        (json!({"id": 7}), ToolErrorKind::PermissionDenied),
        (json!({"id": "bad"}), ToolErrorKind::InvalidArgs),
        (
            json!({"id": 3, "rewrite": true}),
            ToolErrorKind::InvalidArgs,
        ),
    ] {
        let outcome = dispatcher
            .dispatch(Invocation::new(
                "p",
                1,
                "lookup",
                arguments,
                ToolContext::new(),
            ))
            .await;
        let error = outcome
            .result
            .error()
            .or_else(|| outcome.result.refusal())
            .unwrap();
        assert_eq!(error.kind(), kind);
    }
    assert_eq!(calls.get(), 1);

    let approved = Rc::new(Cell::new(true));
    let policy = LocalApproval(approved.clone());
    let catalog = Catalog::default();
    let analysis = analyze("await tools.lookup({id: 3})");
    for allow in [true, false] {
        approved.set(allow);
        let result = policy
            .review(ScriptReview {
                code: "await tools.lookup({id: 3})",
                analysis: &analysis,
                catalog: &catalog,
                parent_call_id: "p",
                context: ToolContext::new(),
            })
            .await;
        if allow {
            let grant = result.unwrap();
            assert!(grant.tools.allows("lookup"));
            assert!(!grant.tools.allows("delete"));
        } else {
            assert_eq!(result.unwrap_err().kind(), ToolErrorKind::PermissionDenied);
        }
    }
    let build = CodeMode::builder(catalog, dispatcher)
        .script_policy(move |review: ScriptReview<'_>| {
            approved.set(true);
            Ok(review.grant_referenced())
        })
        .build();
    assert!(matches!(build, Err(BuildError::NoBackend)));

    let mut report = ExecutionReport::new(ExecutionStatus::ScriptError);
    report.output.text = "partial\n".into();
    report.diagnostic = Some(ScriptDiagnostic::new("guest failed"));
    let error = report.into_tool_result().unwrap_err();
    assert_eq!(error.kind(), ToolErrorKind::Other);
    assert_eq!(
        error.model_feedback(),
        Some("partial\nScript failed: guest failed\n")
    );
}

#[wasm_bindgen_test]
async fn contextual_local_callback_borrows_context_across_await() {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Tenant(String);
    impl ContextValue for Tenant {
        const KEY: &'static str = "app.tenant";
    }

    let suffix = Rc::new("local");
    let tool = DynamicTool::new_with_context(
        ToolName::new("tenant").unwrap(),
        "",
        json!({"type": "object"}),
        move |context: &mut ToolContext, _| {
            let suffix = suffix.clone();
            Box::pin(async move {
                sleep(Duration::from_millis(1)).await;
                let tenant = context.get::<Tenant>().unwrap().unwrap();
                Ok(ToolOutput::text(format!("{}:{suffix}", tenant.0)))
            })
        },
    );
    let dispatcher = DynamicToolDispatcher::new([tool]).unwrap();
    let mut context = ToolContext::new();
    context.insert(Tenant("tenant-17".into())).unwrap();
    let outcome = dispatcher
        .dispatch(Invocation::new("p", 0, "tenant", json!({}), context))
        .await;
    assert!(outcome.result.is_success());
    assert_eq!(outcome.result.output().as_text(), Some("tenant-17:local"));
}

#[wasm_bindgen_test]
fn dropping_dispatch_drops_pending_local_tool() {
    struct Dropped(Rc<Cell<bool>>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    let dropped = Rc::new(Cell::new(false));
    let completed = Rc::new(Cell::new(false));
    let redacted = Rc::new(Cell::new(false));
    let tool = DynamicTool::new(
        ToolName::new("pending").unwrap(),
        "",
        json!({"type": "object"}),
        {
            let dropped = dropped.clone();
            let completed = completed.clone();
            move |_| {
                let guard = Dropped(dropped.clone());
                let completed = completed.clone();
                Box::pin(async move {
                    std::future::pending::<()>().await;
                    drop(guard);
                    completed.set(true);
                    Ok(ToolOutput::text("done"))
                })
            }
        },
    );
    let dispatcher = DynamicToolDispatcher::new([tool])
        .unwrap()
        .with_result_policy({
            let redacted = redacted.clone();
            move |_, result| {
                redacted.set(true);
                result
            }
        });
    let mut dispatch = dispatcher.dispatch(Invocation::new(
        "p",
        0,
        "pending",
        json!({}),
        ToolContext::new(),
    ));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(dispatch.as_mut().poll(&mut context).is_pending());
    assert!(!dropped.get());
    drop(dispatch);
    assert!(dropped.get());
    assert!(!completed.get());
    assert!(!redacted.get());
}
