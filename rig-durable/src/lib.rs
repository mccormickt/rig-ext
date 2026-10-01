//! Durable, replay-safe execution of Rig's sans-I/O agent state machine.
//!
//! Model and tool I/O occurs only in activities. Registry configuration is not
//! persisted: deploy catalog and orchestration changes under a new orchestration
//! version, and keep old versions registered while instances can replay.

#[cfg(any(feature = "duroxide", feature = "temporal"))]
pub mod activities;
#[cfg(any(feature = "duroxide", feature = "temporal"))]
pub mod activity_types;
pub mod approval;
#[cfg(feature = "duroxide")]
pub mod config;
#[cfg(any(feature = "duroxide", feature = "temporal"))]
mod driver;
#[cfg(feature = "duroxide")]
mod facade;
#[cfg(feature = "duroxide")]
pub mod names;
#[cfg(feature = "duroxide")]
pub mod orchestration;
#[cfg(feature = "duroxide")]
pub mod registry;
#[cfg(feature = "duroxide")]
pub mod streaming;
#[cfg(feature = "temporal")]
pub mod temporal;
#[cfg(feature = "duroxide")]
pub mod tools;
#[cfg(feature = "duroxide")]
pub mod types;

#[cfg(feature = "duroxide")]
pub use activity_types::StreamingCompletionOutput;
#[cfg(any(feature = "duroxide", feature = "temporal"))]
pub use activity_types::{ToolActivityInput, ToolActivityOutput, ToolInvocation};
pub use approval::{ApprovalDecision, ApprovalRequest};
#[cfg(feature = "duroxide")]
pub use config::{
    ApprovalConfig, CheckpointConfig, CheckpointPolicy, CompletionMode, CompletionSettings,
    DEFAULT_APPROVAL_QUEUE, DurableAgentConfig,
};
#[cfg(feature = "duroxide")]
pub use facade::{
    AgentDefinition, AgentOrchestrator, AgentOrchestratorBuilder, AgentOrchestratorError,
    DurableAgent, DurableAgentBuilder, DurableRun, ToolOptions,
};
#[cfg(feature = "duroxide")]
pub use registry::{activity_registry, orchestration_registry};
#[cfg(feature = "duroxide")]
pub use streaming::{StreamItem, StreamTranscript};
#[cfg(feature = "duroxide")]
pub use tools::{
    ToolCatalog, ToolEntry, ToolRoute, activity_tool, catalog_from_toolset, sub_orchestration_tool,
};
#[cfg(feature = "duroxide")]
pub use types::AgentInput;

#[cfg(all(test, feature = "duroxide"))]
mod tests {
    use std::{
        convert::Infallible,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use duroxide::{
        Client, OrchestrationStatus, RetryPolicy, providers::sqlite::SqliteProvider, runtime,
    };
    use rig::{
        agent::PromptResponse,
        test_utils::{MockAddTool, MockCompletionModel, MockTurn},
        tool::{Tool, ToolContext, ToolSet},
    };
    use serde::Deserialize;
    use sha2::{Digest, Sha256};

    use crate::{
        AgentInput, ApprovalConfig, ApprovalDecision, ApprovalRequest, DurableAgentConfig,
        activity_registry, catalog_from_toolset, names::ORCHESTRATION, orchestration_registry,
    };

    #[derive(Clone)]
    struct CountingAdd(Arc<AtomicUsize>);

    #[derive(Deserialize)]
    struct AddArgs {
        x: i64,
        y: i64,
    }

    impl Tool for CountingAdd {
        const NAME: &'static str = "add";
        type Args = AddArgs;
        type Output = i64;
        type Error = Infallible;

        fn description(&self) -> String {
            "Add two integers".into()
        }
        fn parameters(&self) -> serde_json::Value {
            serde_json::json!({"type":"object","properties":{"x":{"type":"integer"},"y":{"type":"integer"}}})
        }
        async fn call(
            &self,
            _context: &mut ToolContext,
            args: AddArgs,
        ) -> Result<i64, Self::Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(args.x + args.y)
        }
    }

    async fn wait_for_approval(client: &Client, instance: &str) -> ApprovalRequest {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let OrchestrationStatus::Running {
                    custom_status: Some(status),
                    ..
                } = client.get_orchestration_status(instance).await.unwrap()
                {
                    let status: serde_json::Value = serde_json::from_str(&status).unwrap();
                    if status["phase"] == "approval" {
                        return serde_json::from_value(status["request"].clone()).unwrap();
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap()
    }

    fn approval_config() -> ApprovalConfig {
        ApprovalConfig {
            enabled: true,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn agent_run_executes_model_and_tool_as_activities() {
        let model = MockCompletionModel::new([
            MockTurn::tool_call("call-1", "add", serde_json::json!({"x": 20, "y": 22})),
            MockTurn::text("The answer is 42."),
        ]);
        let tools = ToolSet::from_tools(vec![MockAddTool]);
        let catalog = catalog_from_toolset(&tools, RetryPolicy::new(1)).await;
        let activities = activity_registry(model, tools);
        let orchestrations = orchestration_registry(DurableAgentConfig {
            tools: catalog,
            ..Default::default()
        });
        let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
        let runtime =
            runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
        let client = Client::new(store);

        client
            .start_orchestration_typed(
                "rig-tool-run",
                ORCHESTRATION,
                AgentInput::new("What is 20 + 22?"),
            )
            .await
            .unwrap();
        let response = client
            .wait_for_orchestration_typed::<PromptResponse>("rig-tool-run", Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(response.output, "The answer is 42.");
        runtime.shutdown(None).await;
    }

    #[tokio::test]
    async fn approved_tool_executes_exactly_once() {
        let count = Arc::new(AtomicUsize::new(0));
        let tools = ToolSet::from_tools(vec![CountingAdd(count.clone())]);
        let mut catalog = catalog_from_toolset(&tools, RetryPolicy::new(1)).await;
        catalog.0.get_mut("add").unwrap().requires_approval = true;
        let config = DurableAgentConfig {
            tools: catalog,
            approval: approval_config(),
            ..Default::default()
        };
        let model = MockCompletionModel::new([
            MockTurn::tool_call("call-approved", "add", serde_json::json!({"x": 2, "y": 3})),
            MockTurn::text("done"),
        ]);
        let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
        let runtime = runtime::Runtime::start_with_store(
            store.clone(),
            activity_registry(model, tools),
            orchestration_registry(config.clone()),
        )
        .await;
        let client = Client::new(store);
        client
            .start_orchestration_typed("approved", ORCHESTRATION, AgentInput::new("add"))
            .await
            .unwrap();
        let request = wait_for_approval(&client, "approved").await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
        client
            .enqueue_event_typed(
                "approved",
                &config.approval.queue_name,
                &ApprovalDecision::Approve {
                    approval_id: request.approval_id,
                },
            )
            .await
            .unwrap();
        let response = client
            .wait_for_orchestration_typed::<PromptResponse>("approved", Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.output, "done");
        assert_eq!(count.load(Ordering::SeqCst), 1);
        runtime.shutdown(None).await;
    }

    #[tokio::test]
    async fn denied_tool_is_not_executed_and_model_continues() {
        let count = Arc::new(AtomicUsize::new(0));
        let tools = ToolSet::from_tools(vec![CountingAdd(count.clone())]);
        let mut catalog = catalog_from_toolset(&tools, RetryPolicy::new(1)).await;
        catalog.0.get_mut("add").unwrap().requires_approval = true;
        let config = DurableAgentConfig {
            tools: catalog,
            approval: approval_config(),
            ..Default::default()
        };
        let model = MockCompletionModel::new([
            MockTurn::tool_call("call-denied", "add", serde_json::json!({"x": 2, "y": 3})),
            MockTurn::text("recovered from denial"),
        ]);
        let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
        let runtime = runtime::Runtime::start_with_store(
            store.clone(),
            activity_registry(model, tools),
            orchestration_registry(config.clone()),
        )
        .await;
        let client = Client::new(store);
        client
            .start_orchestration_typed("denied", ORCHESTRATION, AgentInput::new("add"))
            .await
            .unwrap();
        let request = wait_for_approval(&client, "denied").await;
        client
            .enqueue_event_typed(
                "denied",
                &config.approval.queue_name,
                &ApprovalDecision::Deny {
                    approval_id: request.approval_id,
                    reason: Some("not permitted".into()),
                },
            )
            .await
            .unwrap();
        let response = client
            .wait_for_orchestration_typed::<PromptResponse>("denied", Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.output, "recovered from denial");
        assert_eq!(count.load(Ordering::SeqCst), 0);
        runtime.shutdown(None).await;
    }

    #[tokio::test]
    async fn malformed_and_stale_decisions_are_ignored_and_early_match_is_consumed() {
        let count = Arc::new(AtomicUsize::new(0));
        let arguments = serde_json::json!({"x": 8, "y": 9});
        let tools = ToolSet::from_tools(vec![CountingAdd(count.clone())]);
        let mut catalog = catalog_from_toolset(&tools, RetryPolicy::new(1)).await;
        catalog.0.get_mut("add").unwrap().requires_approval = true;
        let config = DurableAgentConfig {
            tools: catalog,
            approval: approval_config(),
            ..Default::default()
        };
        let model = MockCompletionModel::new([
            MockTurn::tool_call("call-early", "add", arguments.clone()),
            MockTurn::text("early approved"),
        ]);
        let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
        let runtime = runtime::Runtime::start_with_store(
            store.clone(),
            activity_registry(model, tools),
            orchestration_registry(config.clone()),
        )
        .await;
        let client = Client::new(store);
        client
            .start_orchestration_typed("early", ORCHESTRATION, AgentInput::new("add"))
            .await
            .unwrap();
        client
            .enqueue_event("early", &config.approval.queue_name, "not-json")
            .await
            .unwrap();
        client
            .enqueue_event_typed(
                "early",
                &config.approval.queue_name,
                &ApprovalDecision::Approve {
                    approval_id: "stale".into(),
                },
            )
            .await
            .unwrap();
        let digest = Sha256::digest(serde_json::to_vec(&arguments).unwrap());
        let approval_id = format!("prompt-0-turn-1-call-0-call-early-{digest:x}");
        client
            .enqueue_event_typed(
                "early",
                &config.approval.queue_name,
                &ApprovalDecision::Approve { approval_id },
            )
            .await
            .unwrap();
        let response = client
            .wait_for_orchestration_typed::<PromptResponse>("early", Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.output, "early approved");
        assert_eq!(count.load(Ordering::SeqCst), 1);
        runtime.shutdown(None).await;
    }
}
