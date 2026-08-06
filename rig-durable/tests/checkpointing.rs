use std::{num::NonZeroU32, sync::Arc, time::Duration};

use duroxide::{Client, RetryPolicy, providers::sqlite::SqliteProvider, runtime};
use rig::{
    agent::PromptResponse,
    test_utils::{MockAddTool, MockCompletionModel, MockTurn},
    tool::ToolSet,
};
use rig_duroxide::{
    AgentInput, CheckpointConfig, CheckpointPolicy, DurableAgentConfig, activity_registry,
    catalog_from_toolset, names::ORCHESTRATION, orchestration_registry,
};

async fn run(checkpoint: CheckpointPolicy, instance: &str) -> (PromptResponse, Vec<u64>, usize) {
    let model = MockCompletionModel::new([
        MockTurn::tool_call("call", "add", serde_json::json!({"x":20,"y":22})),
        MockTurn::text("42"),
    ]);
    let tools = ToolSet::from_tools(vec![MockAddTool]);
    let catalog = catalog_from_toolset(&tools, RetryPolicy::new(1)).await;
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let runtime = runtime::Runtime::start_with_store(
        store.clone(),
        activity_registry(model.clone(), tools),
        orchestration_registry(DurableAgentConfig {
            tools: catalog,
            checkpoint: CheckpointConfig {
                policy: checkpoint,
                target_version: None,
            },
            ..Default::default()
        }),
    )
    .await;
    let client = Client::new(store);
    client
        .start_orchestration_typed(instance, ORCHESTRATION, AgentInput::new("add"))
        .await
        .unwrap();
    let response = client
        .wait_for_orchestration_typed(instance, Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap();
    let executions = client.list_executions(instance).await.unwrap();
    let requests = model.requests().len();
    runtime.shutdown(None).await;
    (response, executions, requests)
}

#[tokio::test]
async fn disabled_keeps_one_execution() {
    let (response, executions, requests) = run(CheckpointPolicy::Disabled, "disabled").await;
    assert_eq!(response.output, "42");
    assert_eq!(executions, [1]);
    assert_eq!(requests, 2);
}

#[tokio::test]
async fn threshold_one_continues_at_each_nonterminal_boundary() {
    let (checkpointed, executions, requests) = run(
        CheckpointPolicy::Every(NonZeroU32::new(1).unwrap()),
        "checkpointed",
    )
    .await;
    let (plain, _, _) = run(CheckpointPolicy::Disabled, "plain").await;

    // Model, complete tool batch, then the final model response. The final
    // response is not followed by a redundant continuation.
    assert_eq!(executions, [1, 2, 3]);
    assert_eq!(requests, 2);
    assert_eq!(checkpointed.output, plain.output);
    assert_eq!(checkpointed.usage, plain.usage);
    assert_eq!(checkpointed.messages, plain.messages);
}
