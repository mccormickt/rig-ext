#![cfg(not(target_family = "wasm"))]
#![allow(clippy::unwrap_used)]

use rig_codemode::{
    CallStatus, Catalog, CatalogEntry, CodeMode, DynamicToolDispatcher, ExecutionRequest,
    ExecutionStatus, HostDispatcher, Invocation,
};
use rig_core::{
    test_utils::{MockHttpResponse, RecordingHttpClient, SequencedHttpClient},
    tool::{ToolContext, ToolErrorKind, ToolExecutionError, ToolOutput},
};
use rig_typesafeai::JevConfig;
use rig_typesafeai_tools::evaluation_tool;
use serde_json::{Value, json};
use std::sync::Arc;

#[tokio::test]
async fn dispatcher_accepts_optional_noul_criteria_and_rejects_malformed_objects() {
    let answers = json!({"ready": {"type": "noul", "noul": 0.73}});
    let http =
        RecordingHttpClient::new(json!({"model": "fixture", "answers": answers}).to_string());
    let dispatcher = DynamicToolDispatcher::new([evaluation_tool(
        JevConfig::new("unused").connect(http.clone()).evaluation(),
    )])
    .unwrap();
    for (criteria, valid) in [
        (None, true),
        (Some(Value::Null), true),
        (Some(json!({"true": "yes", "false": "no"})), true),
        (Some(json!({"true": "yes"})), false),
        (
            Some(json!({"true": "yes", "false": "no", "extra": "unknown"})),
            false,
        ),
    ] {
        let mut question = json!({"type": "noul", "instructions": "Ready?"});
        if let Some(criteria) = criteria {
            question["criteria"] = criteria;
        }
        let outcome = dispatcher
            .dispatch(Invocation::new(
                "request-1",
                0,
                "jev_evaluate",
                json!({"state": "ready", "questions": {"ready": question}}),
                ToolContext::new(),
            ))
            .await;
        if valid {
            assert!(outcome.result.is_success(), "{:?}", outcome.result);
            assert_eq!(
                outcome.result.output().as_json(),
                Some(&json!({"answers": answers, "model": "fixture", "usage": null}))
            );
        } else {
            assert_eq!(
                outcome.result.error().unwrap().kind(),
                ToolErrorKind::InvalidArgs
            );
        }
    }
    assert_eq!(http.requests().len(), 3);
}

#[tokio::test]
async fn scripts_compose_evaluations_under_host_call_and_result_policy() {
    let http = SequencedHttpClient::new([
        MockHttpResponse::success(
            json!({"model": "fixture", "answers": {
            "route": {"type": "choice", "choice": "review",
                "probabilities": {"accept": 0.14, "review": 0.86}, "confidence": 0.72}
        }, "usage": {"input_tokens": 17, "output_tokens": 5}})
            .to_string(),
        ),
        MockHttpResponse::success(
            json!({"model": "fixture", "answers": {
                "urgent": {"type": "noul", "noul": 0.93}
            }})
            .to_string(),
        ),
    ]);
    let tool = evaluation_tool(
        JevConfig::new("host-token")
            .connect(http.clone())
            .evaluation(),
    );
    let dispatcher = DynamicToolDispatcher::new([tool])
        .unwrap()
        .with_call_policy(|call| {
            if call.arguments["state"] == "private" {
                return Err(ToolExecutionError::refused("state access denied"));
            }
            Ok(())
        })
        .with_result_policy(|_, result| {
            let Some(mut value) = result.output().as_json().cloned() else {
                return result;
            };
            value.as_object_mut().unwrap().remove("usage");
            result.with_output(ToolOutput::json(value))
        });
    let catalog = Catalog::new(
        dispatcher
            .definitions()
            .iter()
            .map(CatalogEntry::from_definition),
    )
    .unwrap();
    let codemode = CodeMode::builder(catalog, Arc::new(dispatcher))
        .build()
        .unwrap();
    let report = codemode.execute(ExecutionRequest::new(r#"
        const first = await tools.jev_evaluate({state: "outage", questions: {
            route: {type: "choice", instructions: "Choose", criteria: {accept: "safe", review: "needs review"}}
        }});
        const second = await tools.jev_evaluate({state: {route: first.answers.route.choice}, questions: {
            urgent: {type: "noul", instructions: "Act now?"}
        }});
        const denied = await tools.jev_evaluate.raw({state: "private", questions: {
            urgent: {type: "noul", instructions: "Act now?"}
        }});
        return {route: first.answers.route.choice, probability: second.answers.urgent.noul,
            leakedUsage: "usage" in first, denied: denied.status};
    "#)).await.unwrap();
    assert_eq!(report.status, ExecutionStatus::Completed);
    assert_eq!(
        report.returned,
        Some(json!({
            "route": "review", "probability": 0.93, "leakedUsage": false, "denied": "denied"
        }))
    );
    assert_eq!(
        report
            .calls
            .iter()
            .map(|call| call.status)
            .collect::<Vec<_>>(),
        vec![
            CallStatus::Succeeded,
            CallStatus::Succeeded,
            CallStatus::Refused
        ]
    );
    let requests = http.requests();
    assert_eq!(requests.len(), 2);
    let second: Value = serde_json::from_slice(&requests[1].body).unwrap();
    assert_eq!(second["state"], json!({"route": "review"}));
    assert_eq!(http.remaining_responses(), 0);
}
