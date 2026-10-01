#![cfg(feature = "duroxide")]

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use duroxide::{Client, RetryPolicy, providers::sqlite::SqliteProvider, runtime};
use rig::{
    agent::PromptResponse,
    completion::Usage,
    test_utils::{MockAddTool, MockCompletionModel, MockStreamEvent, mock_final},
    tool::ToolSet,
};
use rig_durable::{
    AgentInput, CheckpointConfig, CheckpointPolicy, CompletionMode, DurableAgentConfig, StreamItem,
    activity_registry, catalog_from_toolset, names::ORCHESTRATION, orchestration_registry,
};

async fn run(
    model: MockCompletionModel,
    tools: ToolSet,
    retry: RetryPolicy,
    instance: &str,
) -> (Result<PromptResponse, String>, usize) {
    let catalog = catalog_from_toolset(&tools, RetryPolicy::new(1)).await;
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let runtime = runtime::Runtime::start_with_store(
        store.clone(),
        activity_registry(model.clone(), tools),
        orchestration_registry(DurableAgentConfig {
            tools: catalog,
            completion_mode: CompletionMode::Streaming,
            completion_retry: retry,
            ..Default::default()
        }),
    )
    .await;
    let client = Client::new(store);
    client
        .start_orchestration_typed(instance, ORCHESTRATION, AgentInput::new("test"))
        .await
        .unwrap();
    let result = client
        .wait_for_orchestration_typed(instance, Duration::from_secs(5))
        .await
        .unwrap();
    let count = model.requests().len();
    runtime.shutdown(None).await;
    (result, count)
}

#[tokio::test]
async fn text_deltas_produce_output_and_usage() {
    let model = MockCompletionModel::from_stream_turns([[
        MockStreamEvent::text("hel"),
        MockStreamEvent::text("lo"),
        MockStreamEvent::final_response_with_total_tokens(7),
    ]]);
    let (result, count) = run(
        model,
        ToolSet::default(),
        RetryPolicy::new(1),
        "stream-text",
    )
    .await;
    let response = result.unwrap();
    assert_eq!(response.output, "hello");
    assert_eq!(response.usage.total_tokens, 7);
    assert_eq!(count, 1);
}

#[tokio::test]
async fn explicit_message_id_takes_precedence_over_the_terminal_id() {
    let model = MockCompletionModel::from_stream_turns([[
        MockStreamEvent::message_id("message-event"),
        MockStreamEvent::text("done"),
        MockStreamEvent::FinalResponse(
            mock_final(Usage::new()).with_message_id("terminal-message"),
        ),
    ]]);
    let response = run(
        model,
        ToolSet::default(),
        RetryPolicy::new(1),
        "stream-message-id",
    )
    .await
    .0
    .unwrap();
    assert_eq!(
        response.completion_calls[0].message_id.as_deref(),
        Some("message-event")
    );
    let messages = serde_json::to_string(&response.messages).unwrap();
    assert!(messages.contains("message-event"));
    assert!(!messages.contains("terminal-message"));
}

#[tokio::test]
async fn complete_tool_call_executes_and_stream_continues() {
    let model = MockCompletionModel::from_stream_turns([
        vec![
            MockStreamEvent::message_id("assistant-message"),
            MockStreamEvent::tool_call("provider-id", "add", serde_json::json!({"x": 2, "y": 3}))
                .with_call_id("provider-call"),
            MockStreamEvent::final_response_with_default_usage(),
        ],
        vec![
            MockStreamEvent::text("5"),
            MockStreamEvent::final_response_with_default_usage(),
        ],
    ]);
    let (result, count) = run(
        model,
        ToolSet::from_tools(vec![MockAddTool]),
        RetryPolicy::new(1),
        "stream-tool",
    )
    .await;
    let response = result.unwrap();
    assert_eq!(response.output, "5");
    assert_eq!(count, 2);
    let history = serde_json::to_string(&response.messages).unwrap();
    assert!(history.contains("provider-id"));
    assert!(history.contains("provider-call"));
    assert!(history.contains("assistant-message"));
}

#[tokio::test]
async fn tool_call_deltas_assemble_and_missing_name_fails() {
    let valid = MockCompletionModel::from_stream_turns([
        vec![
            MockStreamEvent::tool_call_name_delta("id", "add"),
            MockStreamEvent::tool_call_arguments_delta("id", r#"{"x":4,"y":5}"#),
            MockStreamEvent::tool_call("id", "add", serde_json::json!({"x": 4, "y": 5})),
            MockStreamEvent::final_response_with_default_usage(),
        ],
        vec![
            MockStreamEvent::text("9"),
            MockStreamEvent::final_response_with_default_usage(),
        ],
    ]);
    assert_eq!(
        run(
            valid,
            ToolSet::from_tools(vec![MockAddTool]),
            RetryPolicy::new(1),
            "stream-delta"
        )
        .await
        .0
        .unwrap()
        .output,
        "9"
    );

    let invalid = MockCompletionModel::from_stream_turns([[
        MockStreamEvent::tool_call_arguments_delta("id", "{}"),
        MockStreamEvent::final_response_with_default_usage(),
    ]]);
    let error = run(
        invalid,
        ToolSet::from_tools(vec![MockAddTool]),
        RetryPolicy::new(1),
        "stream-bad-delta",
    )
    .await
    .0
    .unwrap_err();
    assert!(error.contains("validated tool name"), "{error}");
}

#[tokio::test]
async fn unknown_and_reasoning_are_durable_but_unknown_is_not_history() {
    let model = MockCompletionModel::from_stream_turns([[
        MockStreamEvent::reasoning_delta_with_id("r", "think"),
        MockStreamEvent::unknown(serde_json::json!({"native": true})),
        MockStreamEvent::text("ok"),
        MockStreamEvent::final_response_with_default_usage(),
    ]]);
    let response = run(
        model,
        ToolSet::default(),
        RetryPolicy::new(1),
        "stream-unknown",
    )
    .await
    .0
    .unwrap();
    let history = serde_json::to_string(&response.messages).unwrap();
    assert!(history.contains("think"));
    assert!(!history.contains("native"));
    assert_eq!(response.usage.total_tokens, 0);
}

#[tokio::test]
async fn truncated_streams_retry_without_committing_content_or_tools() {
    let model = MockCompletionModel::from_stream_turns([
        vec![MockStreamEvent::tool_call(
            "id",
            "add",
            serde_json::json!({"x": 4, "y": 5}),
        )],
        vec![
            MockStreamEvent::text("recovered"),
            MockStreamEvent::final_response_with_default_usage(),
        ],
    ]);
    let (response, requests) = run(
        model,
        ToolSet::from_tools(vec![MockAddTool]),
        RetryPolicy::new(2),
        "stream-truncated",
    )
    .await;

    assert_eq!(response.unwrap().output, "recovered");
    assert_eq!(requests, 2);
}

#[tokio::test]
async fn provider_errors_retry_and_unknown_tools_fail_closed() {
    let retrying = MockCompletionModel::from_stream_turns([
        vec![MockStreamEvent::error("temporary")],
        vec![
            MockStreamEvent::text("recovered"),
            MockStreamEvent::final_response_with_default_usage(),
        ],
    ]);
    let (response, requests) = run(
        retrying,
        ToolSet::default(),
        RetryPolicy::new(2),
        "stream-retry",
    )
    .await;
    assert_eq!(response.unwrap().output, "recovered");
    assert_eq!(requests, 2);

    let unknown = MockCompletionModel::from_stream_turns([[
        MockStreamEvent::tool_call("id", "not_registered", serde_json::json!({})),
        MockStreamEvent::final_response_with_default_usage(),
    ]]);
    let error = run(
        unknown,
        ToolSet::default(),
        RetryPolicy::new(1),
        "stream-unknown-tool",
    )
    .await
    .0
    .unwrap_err();
    assert!(error.contains("not_registered"), "{error}");
}

#[test]
fn transcript_items_are_tagged_provider_neutral_json() {
    let item = StreamItem::Unknown {
        value: serde_json::json!({"x": 1}),
    };
    let json = serde_json::to_string(&item).unwrap();
    assert!(json.contains(r#""type":"unknown""#));
    assert_eq!(serde_json::from_str::<StreamItem>(&json).unwrap(), item);
    assert!(!json.contains("StreamingResponse"));
}

#[tokio::test]
async fn checkpointing_streamed_turns_does_not_repeat_provider_calls() {
    let model = MockCompletionModel::from_stream_turns([
        vec![
            MockStreamEvent::tool_call("call", "add", serde_json::json!({"x": 20, "y": 22})),
            MockStreamEvent::final_response_with_default_usage(),
        ],
        vec![
            MockStreamEvent::text("42"),
            MockStreamEvent::final_response_with_total_tokens(3),
        ],
    ]);
    let tools = ToolSet::from_tools(vec![MockAddTool]);
    let catalog = catalog_from_toolset(&tools, RetryPolicy::new(1)).await;
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let runtime = runtime::Runtime::start_with_store(
        store.clone(),
        activity_registry(model.clone(), tools),
        orchestration_registry(DurableAgentConfig {
            tools: catalog,
            completion_mode: CompletionMode::Streaming,
            checkpoint: CheckpointConfig {
                policy: CheckpointPolicy::Every(NonZeroU32::new(1).unwrap()),
                target_version: None,
            },
            ..Default::default()
        }),
    )
    .await;
    let client = Client::new(store);
    client
        .start_orchestration_typed("stream-checkpoint", ORCHESTRATION, AgentInput::new("add"))
        .await
        .unwrap();
    let response = client
        .wait_for_orchestration_typed::<PromptResponse>("stream-checkpoint", Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(response.output, "42");
    assert_eq!(response.usage.total_tokens, 3);
    assert_eq!(model.request_count(), 2);
    assert_eq!(
        client.list_executions("stream-checkpoint").await.unwrap(),
        [1, 2, 3]
    );
    runtime.shutdown(None).await;
}
