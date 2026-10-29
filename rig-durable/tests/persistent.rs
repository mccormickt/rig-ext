#![allow(clippy::unwrap_used, clippy::panic)]

use rig::{
    completion::Message,
    effect::Answered,
    message::{AssistantContent, ToolCall, ToolFunction, ToolName},
    test_utils::{MockAddTool, MockCompletionModel, MockTurn},
    tool::{ToolOutput, ToolResult, ToolSet},
};
use rig_durable::{
    activities::completion,
    persistent::{CompletionOptions, Effect, Run},
};

fn options() -> CompletionOptions {
    let mut tools = ToolSet::from_tools(vec![MockAddTool])
        .tool_definitions()
        .into_iter()
        .map(|definition| (*definition).clone())
        .collect::<Vec<_>>();
    tools[0].annotations.read_only_hint = Some(true);
    CompletionOptions {
        preamble: Some("Use the supplied tools".into()),
        tools,
        temperature: Some(0.25),
        max_tokens: Some(512),
        tool_choice: None,
        additional_params: None,
    }
}

fn reopen(run: &Run) -> Run {
    serde_json::from_slice(&serde_json::to_vec(run).unwrap()).unwrap()
}

#[tokio::test]
async fn sparse_parallel_settlements_survive_reopen_and_drain_all_runner_wakes() {
    // More than one FuturesUnordered poll budget, with repeated provider IDs.
    let calls = (0..70)
        .map(|index| {
            AssistantContent::ToolCall(ToolCall::from_wire(
                "reused-provider-id",
                ToolFunction::new(
                    ToolName::new("add").unwrap(),
                    serde_json::json!({"x":index,"y":3}),
                ),
            ))
        })
        .collect::<Vec<_>>();
    let model = MockCompletionModel::from_turns([
        MockTurn::from_contents(calls),
        MockTurn::text("complete"),
    ]);
    let mut run = Run::new("add all")
        .with_history(vec![Message::user("earlier"), Message::assistant("noted")]);
    let Effect::Model { request, .. } = run.plan(options()).unwrap() else {
        panic!("model expected")
    };
    assert_eq!(request.tools[0].annotations.read_only_hint, Some(true));
    run.model_response(
        completion::complete(&model.clone().into(), request)
            .await
            .unwrap()
            .response,
    )
    .unwrap();
    let Effect::Tools { calls } = run.plan(options()).unwrap() else {
        panic!("tools expected")
    };
    assert_eq!(calls.len(), 70);
    for call in calls.iter().skip(1).rev() {
        run.settle(
            call.dispatch_id,
            Ok(Answered::ToolResult {
                result: ToolResult::success(ToolOutput::json(serde_json::json!(
                    call.call_index + 3
                ))),
            }),
        )
        .unwrap();
    }
    let mut changed_record = serde_json::to_value(&run).unwrap();
    assert!(changed_record["log"]["records"][1]["kind"]["args"].is_string());
    changed_record["log"]["records"][1]["kind"]["args"] = serde_json::json!("{\"x\":999,\"y\":3}");
    let mut diverged: Run = serde_json::from_value(changed_record).unwrap();
    assert!(diverged.plan(options()).is_err());
    let mut run = reopen(&run);
    let Effect::Tools { calls: remaining } = run.plan(options()).unwrap() else {
        panic!("unsettled first call expected")
    };
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].dispatch_id, calls[0].dispatch_id);
    assert_eq!(remaining[0].call_index, 0);
    run.settle(
        remaining[0].dispatch_id,
        Ok(Answered::ToolResult {
            result: ToolResult::success(ToolOutput::json(serde_json::json!(3))),
        }),
    )
    .unwrap();
    let mut run = reopen(&run);
    let Effect::Model { request, turn } = run.plan(options()).unwrap() else {
        panic!("second model expected")
    };
    assert_eq!(turn, 2);
    rig::conversation::pairing::validate(&request.chat_history).unwrap();
    let encoded = serde_json::to_string(&request.chat_history).unwrap();
    assert!(encoded.contains("earlier"));
    assert!(encoded.contains("72"));
    run.model_response(
        completion::complete(&model.clone().into(), request)
            .await
            .unwrap()
            .response,
    )
    .unwrap();
    let Effect::Done(response) = reopen(&run).plan(options()).unwrap() else {
        panic!("done expected")
    };
    assert_eq!(response.output(), "complete");
    assert_eq!(model.requests().len(), 2);
    let mut changed = options();
    changed.temperature = Some(0.5);
    assert!(
        run.plan(changed)
            .unwrap_err()
            .contains("configuration changed")
    );
}
