#![allow(clippy::unwrap_used)]

use rig_core::{
    http_client::StatusCode,
    test_utils::{MockHttpResponse, RecordingHttpClient},
    tool::ToolErrorKind,
};
use rig_typesafeai::JevConfig;
use rig_typesafeai_tools::evaluation_tool;
use serde_json::{Value, json};

fn questions() -> Value {
    json!({
        "route": {"type": "choice", "instructions": {"task": "Choose a route"},
            "criteria": {"review": null, "accept": ["Safe", "Complete"]}},
        "severity": {"type": "score", "instructions": "Rate impact",
            "criteria": ["low", {"impact": "medium"}, "high"]},
        "urgent": {"type": "noul", "instructions": "Act now?"}
    })
}

fn answers() -> Value {
    json!({
        "route": {"type": "choice", "choice": "review",
            "probabilities": {"review": 0.82, "accept": 0.18}, "confidence": 0.64},
        "severity": {"type": "score", "score": 1.7,
            "probabilities": {"0": 0.0, "1": 0.3, "2": 0.7},
            "legend": {"0": "low", "1": {"impact": "medium"}, "2": "high"}, "confidence": 0.53},
        "urgent": {"type": "noul", "noul": 0.91}
    })
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn preserves_native_answers_and_projects_only_model_and_usage() {
    let http = RecordingHttpClient::new(
        json!({
            "model": "jev-fixture", "answers": answers(),
            "usage": {"input_tokens": 37, "output_tokens": 11},
            "private": "not-for-the-script", "provider_request_id": "private-id"
        })
        .to_string(),
    );
    let tool = evaluation_tool(
        JevConfig::new("host-only-token")
            .model("host-selected-model")
            .with_endpoint("https://host.example/v1/systemone")
            .connect(http.clone())
            .evaluation(),
    );
    assert_eq!(tool.name().as_str(), "jev_evaluate");
    for (state, explicit_null) in [
        (json!("outage"), false),
        (json!({"event": "outage"}), true),
        (json!(["outage"]), false),
    ] {
        let mut input_questions = questions();
        if explicit_null {
            input_questions["urgent"]["criteria"] = Value::Null;
        }
        let output = tool
            .execute(json!({"state": state, "questions": input_questions}))
            .await
            .unwrap();
        assert_eq!(
            output.as_json(),
            Some(&json!({
                "model": "jev-fixture", "answers": answers(),
                "usage": {"input_tokens": 37, "output_tokens": 11}
            }))
        );
        let requests = http.requests();
        let request = requests.last().unwrap();
        assert_eq!(request.uri, "https://host.example/v1/systemone");
        assert_eq!(request.headers["authorization"], "Bearer host-only-token");
        assert_eq!(
            serde_json::from_slice::<Value>(&request.body).unwrap(),
            json!({
                "model": "host-selected-model", "state": state, "questions": questions()
            })
        );
    }
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn direct_calls_reject_invalid_arguments_without_provider_io() {
    let http = RecordingHttpClient::default();
    let tool = evaluation_tool(JevConfig::new("unused").connect(http.clone()).evaluation());
    let valid = json!({"state": "outage", "questions": questions()});
    let mut inputs = vec![json!(null), json!({}), json!([])];
    for state in [json!(null), json!(19), json!(true)] {
        let mut input = valid.clone();
        input["state"] = state;
        inputs.push(input);
    }
    for questions in [
        json!({}),
        json!({"": {"type": "noul", "instructions": "Empty ID"}}),
        json!({"q": {"type": "bool", "instructions": "Wrong type"}}),
        json!({"q": {"type": "noul", "instructions": true}}),
        json!({"q": {"type": "noul", "instructions": "Unknown field", "endpoint": "https://guest.invalid"}}),
        json!({"q": {"type": "noul", "instructions": "Bad criteria", "criteria": {"true": "yes"}}}),
        json!({"q": {"type": "noul", "instructions": "Bad criteria", "criteria": {"true": "yes", "false": "no", "extra": "unknown"}}}),
        json!({"q": {"type": "choice", "instructions": "Bad choices", "criteria": {"one": "alone"}}}),
        json!({"q": {"type": "choice", "instructions": "Bad content", "criteria": {"one": 123, "two": "valid"}}}),
        json!({"q": {"type": "score", "instructions": "Bad rubric", "criteria": ["alone"]}}),
    ] {
        inputs.push(json!({"state": "outage", "questions": questions}));
    }
    for name in ["model", "endpoint", "headers", "token"] {
        let mut input = valid.clone();
        input[name] = json!("guest-supplied");
        inputs.push(input);
    }
    for input in inputs {
        let error = tool.execute(input.clone()).await.unwrap_err();
        assert_eq!(error.kind(), ToolErrorKind::InvalidArgs, "{input}");
    }
    assert!(http.requests().is_empty());
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn provider_failures_and_invalid_answers_are_redacted_not_retried() {
    let http = RecordingHttpClient::default();
    let tool = evaluation_tool(
        JevConfig::new("host-only-token")
            .connect(http.clone())
            .evaluation(),
    );
    let mut responses = vec![
        MockHttpResponse::error(
            StatusCode::TOO_MANY_REQUESTS,
            "host-only-token private body",
        ),
        MockHttpResponse::success("malformed provider body with private data"),
    ];
    for corrupt in [
        json!({"private-id": {"type": "noul", "noul": 0.1}}),
        json!({"urgent": {"type": "noul", "noul": 1.5}}),
        json!({"urgent": {"type": "choice", "choice": "private-label"}}),
    ] {
        responses.push(MockHttpResponse::success(
            json!({
                "model": "jev-fixture", "answers": corrupt
            })
            .to_string(),
        ));
    }
    for (index, response) in responses.into_iter().enumerate() {
        http.set_response(response);
        let error = tool
            .execute(json!({"state": "outage", "questions": {
                "urgent": {"type": "noul", "instructions": "Act now?"}
            }}))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ToolErrorKind::Other);
        assert_eq!(error.retryable(), Some(false));
        assert_eq!(
            error.to_string(),
            "Jev evaluation failed or returned invalid answers"
        );
        let debug = format!("{error:?}");
        assert!(!debug.contains("private"));
        assert!(!debug.contains("host-only-token"));
        assert_eq!(http.requests().len(), index + 1);
    }
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn host_question_budgets_are_not_baked_into_the_adapter() {
    let questions: serde_json::Map<_, _> = (0..17)
        .map(|i| {
            (
                format!("{}{i}", "q".repeat(129)),
                json!({"type": "noul", "instructions": "Ready?"}),
            )
        })
        .collect();
    let answers: serde_json::Map<_, _> = questions
        .keys()
        .map(|id| (id.clone(), json!({"type": "noul", "noul": 0.23})))
        .collect();
    let http =
        RecordingHttpClient::new(json!({"model": "fixture", "answers": answers}).to_string());
    let tool = evaluation_tool(JevConfig::new("unused").connect(http).evaluation());
    let output = tool
        .execute(json!({"state": [], "questions": questions}))
        .await
        .unwrap();
    assert_eq!(
        output.as_json(),
        Some(&json!({"answers": answers, "model": "fixture", "usage": null}))
    );
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[wasm_bindgen_test::wasm_bindgen_test]
async fn local_model_keeps_rc_state_across_an_await() {
    use rig_core::{
        error::ProviderError,
        wasm_compat::{WasmCompatSync, sleep},
    };
    use rig_typesafeai::{Evaluate, EvaluationResult, Query};
    use serde::Serialize;
    use std::{cell::Cell, rc::Rc, time::Duration};

    #[derive(Clone)]
    struct LocalModel(Rc<Cell<u32>>);

    impl Evaluate for LocalModel {
        async fn evaluate<S: Serialize + WasmCompatSync, Q: Query>(
            &self,
            _state: &S,
            questions: Q,
        ) -> Result<EvaluationResult<Q::Output>, ProviderError> {
            let calls = self.0.clone();
            sleep(Duration::from_millis(1)).await;
            calls.set(calls.get() + 1);
            Ok(EvaluationResult {
                answers: questions.decode(serde_json::from_value(json!({
                    "ready": {"type": "noul", "noul": 0.73}
                }))?)?,
                model: "local-fixture".into(),
                usage: None,
                provider_request_id: None,
            })
        }
    }

    let calls = Rc::new(Cell::new(0));
    let tool = evaluation_tool(LocalModel(calls.clone()));
    let output = tool
        .execute(json!({"state": "ready", "questions": {
            "ready": {"type": "noul", "instructions": "Ready?"}
        }}))
        .await
        .unwrap();
    assert_eq!(
        output.as_json(),
        Some(&json!({"model": "local-fixture", "usage": null,
            "answers": {"ready": {"type": "noul", "noul": 0.73}}
        }))
    );
    assert_eq!(calls.get(), 1);
}
