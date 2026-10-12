#![cfg(not(target_family = "wasm"))]
#![allow(clippy::unwrap_used)]

use rig_codemode::{
    CallStatus, Catalog, CatalogEntry, CodeMode, DynamicToolDispatcher, ExecutionRequest,
    ExecutionStatus,
};
use rig_core::{
    test_utils::{MockHttpResponse, SequencedHttpClient},
    tool::{ToolExecutionError, ToolOutput},
};
use rig_typesafeai::JevConfig;
use rig_typesafeai_tools::evaluation_tool;
use serde_json::{Value, json};
use std::sync::Arc;

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
