#![cfg(feature = "duroxide")]

use std::{future::Future, sync::Arc, time::Duration};

use duroxide::{
    BackoffStrategy, Client, OrchestrationContext, RetryPolicy,
    providers::sqlite::SqliteProvider,
    runtime::{self, registry::OrchestrationRegistry},
};
use rig::{
    agent::PromptResponse,
    completion::ToolDefinition,
    test_utils::{MockCompletionModel, MockTurn},
    tool::ToolSet,
};
use rig_durable::{
    AgentInput, DurableAgentConfig, ToolCatalog, activity_registry, orchestration_registry,
    sub_orchestration_tool,
};

fn definition() -> ToolDefinition {
    ToolDefinition {
        name: "child".into(),
        description: "Run child".into(),
        parameters: serde_json::json!({"type":"object"}),
    }
}

async fn run_child_tool<F, Fut>(
    child: F,
    entry: rig_durable::ToolEntry,
) -> (Result<PromptResponse, String>, MockCompletionModel)
where
    F: Fn(OrchestrationContext, String) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<String, String>> + Send + 'static,
{
    let model = MockCompletionModel::new([
        MockTurn::tool_call("rig-id", "child", serde_json::json!({"value":7}))
            .with_call_id("provider-id"),
        MockTurn::text("done"),
    ]);
    let mut catalog = ToolCatalog::default();
    catalog.insert(entry);
    let base = orchestration_registry(DurableAgentConfig {
        tools: catalog,
        ..Default::default()
    });
    let orchestrations = OrchestrationRegistry::builder_from(&base)
        .register("Child", child)
        .build();
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let runtime = runtime::Runtime::start_with_store(
        store.clone(),
        activity_registry(model.clone(), ToolSet::default()),
        orchestrations,
    )
    .await;
    let client = Client::new(store);
    client
        .start_orchestration_typed(
            "parent",
            rig_durable::names::ORCHESTRATION,
            AgentInput::new("run"),
        )
        .await
        .unwrap();
    let result = client
        .wait_for_orchestration_typed("parent", Duration::from_secs(5))
        .await
        .unwrap();
    runtime.shutdown(None).await;
    (result, model)
}

#[tokio::test]
async fn json_output_and_correlations_reach_the_model() {
    let (result, model) = run_child_tool(
        |_ctx, input| async move {
            assert_eq!(input, r#"{"value":7}"#);
            Ok(r#"{"answer":42}"#.into())
        },
        sub_orchestration_tool(definition(), "Child", None),
    )
    .await;
    assert_eq!(result.unwrap().output, "done");
    let request = serde_json::to_value(&model.requests()[1])
        .unwrap()
        .to_string();
    assert!(request.contains("rig-id"));
    assert!(request.contains("provider-id"));
    assert!(request.contains("answer"));
}

#[tokio::test]
async fn raw_text_output_and_continue_as_new_complete() {
    let (result, model) = run_child_tool(
        |ctx, input| async move {
            if input.starts_with("next:") {
                Ok("plain child output".into())
            } else {
                ctx.continue_as_new(format!("next:{input}")).await
            }
        },
        sub_orchestration_tool(definition(), "Child", None),
    )
    .await;
    assert_eq!(result.unwrap().output, "done");
    let request = serde_json::to_value(&model.requests()[1])
        .unwrap()
        .to_string();
    assert!(request.contains("plain child output"));
}

#[tokio::test]
async fn child_failure_propagates() {
    let (result, _) = run_child_tool(
        |_ctx, _input| async { Err("child exploded".into()) },
        sub_orchestration_tool(definition(), "Child", None),
    )
    .await;
    assert!(result.unwrap_err().contains("child exploded"));
}

#[tokio::test]
async fn unsupported_configuration_fails_before_child_is_scheduled() {
    let mut entry = sub_orchestration_tool(definition(), "Child", None);
    entry.tag = Some("worker".into());
    let (result, _) = run_child_tool(
        |_ctx, _input| async { panic!("child must not be scheduled") },
        entry,
    )
    .await;
    assert!(result.unwrap_err().contains("does not support worker tags"));

    let mut entry = sub_orchestration_tool(definition(), "Child", None);
    entry.retry = RetryPolicy::new(2).with_backoff(BackoffStrategy::Fixed {
        delay: Duration::from_millis(1),
    });
    let (result, _) = run_child_tool(
        |_ctx, _input| async { panic!("child must not be scheduled") },
        entry,
    )
    .await;
    assert!(result.unwrap_err().contains("parent-side retries"));

    let mut entry = sub_orchestration_tool(definition(), "Child", None);
    entry.retry = RetryPolicy::new(1).with_timeout(Duration::from_secs(1));
    let (result, _) = run_child_tool(
        |_ctx, _input| async { panic!("child must not be scheduled") },
        entry,
    )
    .await;
    assert!(result.unwrap_err().contains("parent-side timeouts"));
}
