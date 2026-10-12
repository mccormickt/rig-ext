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
    tool::{DynamicTool, ToolContext, ToolErrorKind, ToolExecutionError, ToolOutput},
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
