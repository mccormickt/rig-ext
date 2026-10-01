#![cfg(feature = "duroxide")]

use std::{
    convert::Infallible,
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use duroxide::{RetryPolicy, providers::sqlite::SqliteProvider, runtime::RuntimeOptions};
use rig::{
    test_utils::{MockAddTool, MockCompletionModel, MockStreamEvent, MockTurn},
    tool::{Tool, ToolContext},
};
use rig_durable::{
    AgentOrchestrator, AgentOrchestratorError, CheckpointConfig, CheckpointPolicy, CompletionMode,
    DurableAgent, ToolOptions,
};
use serde::Deserialize;

#[tokio::test]
async fn rig_first_prompt_tools_streaming_and_versions() {
    let v1 = DurableAgent::builder(
        "calculator",
        MockCompletionModel::from_turns([MockTurn::text("v1")]),
    )
    .version("1.0.0")
    .unwrap()
    .build()
    .unwrap();
    let v2 = DurableAgent::builder(
        "calculator",
        MockCompletionModel::from_stream_turns([vec![
            MockStreamEvent::text("v2"),
            MockStreamEvent::final_response_with_default_usage(),
        ]]),
    )
    .version("2.0.0")
    .unwrap()
    .completion_mode(CompletionMode::Streaming)
    .checkpoint(CheckpointConfig {
        policy: CheckpointPolicy::Every(NonZeroU32::new(1).unwrap()),
        target_version: None,
    })
    .tool(MockAddTool)
    .build()
    .unwrap();
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let orchestrator = AgentOrchestrator::builder(store)
        .register(v1)
        .unwrap()
        .register(v2)
        .unwrap()
        .start()
        .await
        .unwrap();

    assert_eq!(orchestrator.agent("calculator").unwrap().version().major, 2);
    assert_eq!(
        orchestrator
            .agent("calculator")
            .unwrap()
            .prompt("latest")
            .await
            .unwrap(),
        "v2"
    );
    assert_eq!(
        orchestrator
            .agent_version("calculator", "1.0.0")
            .unwrap()
            .prompt("old")
            .await
            .unwrap(),
        "v1"
    );
    orchestrator.shutdown(None).await;
}

#[tokio::test]
async fn approval_is_managed_through_the_run_handle() {
    let definition = DurableAgent::builder(
        "approved-calculator",
        MockCompletionModel::from_turns([
            MockTurn::tool_call("call", "add", serde_json::json!({"x": 20, "y": 22})),
            MockTurn::text("approved"),
        ]),
    )
    .tool_with(
        MockAddTool,
        ToolOptions::default()
            .retry(RetryPolicy::new(1))
            .require_approval(),
    )
    .build()
    .unwrap();
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let orchestrator = AgentOrchestrator::builder(store.clone())
        .register(definition)
        .unwrap()
        .start()
        .await
        .unwrap();
    let agent = orchestrator.agent("approved-calculator").unwrap();
    let run = agent.start_with_id("approval-facade", "add").await.unwrap();

    let request = run.next_approval().await.unwrap();
    assert_eq!(request.tool_name, "add");
    orchestrator.shutdown(None).await;

    let recovered = DurableAgent::builder(
        "approved-calculator",
        MockCompletionModel::from_turns([MockTurn::text("approved")]),
    )
    .tool_with(
        MockAddTool,
        ToolOptions::default()
            .retry(RetryPolicy::new(1))
            .require_approval(),
    )
    .build()
    .unwrap();
    let orchestrator = AgentOrchestrator::builder(store)
        .register(recovered)
        .unwrap()
        .start()
        .await
        .unwrap();
    let run = orchestrator
        .agent("approved-calculator")
        .unwrap()
        .run("approval-facade");
    let replayed_request = run.next_approval().await.unwrap();
    assert_eq!(replayed_request.approval_id, request.approval_id);
    run.approve(&replayed_request).await.unwrap();
    assert_eq!(run.wait().await.unwrap().output, "approved");
    orchestrator.shutdown(None).await;
}

#[tokio::test]
async fn steering_runs_as_a_follow_up_turn_before_completion() {
    let definition = DurableAgent::builder(
        "steerable",
        MockCompletionModel::from_turns([
            MockTurn::text("initial answer"),
            MockTurn::text("steered answer"),
        ]),
    )
    .build()
    .unwrap();
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let orchestrator = AgentOrchestrator::builder(store)
        .register(definition)
        .unwrap()
        .start()
        .await
        .unwrap();
    let run = orchestrator
        .agent("steerable")
        .unwrap()
        .start_with_id("steered-run", "initial prompt")
        .await
        .unwrap();

    run.steer("change direction").await.unwrap();
    let response = run.wait().await.unwrap();

    assert_eq!(response.output, "steered answer");
    assert_eq!(response.messages.unwrap().len(), 2);
    orchestrator.shutdown(None).await;
}

#[tokio::test]
async fn checkpoint_policy_applies_before_a_steered_turn() {
    let definition = DurableAgent::builder(
        "checkpointed-steering",
        MockCompletionModel::from_turns([
            MockTurn::text("initial answer"),
            MockTurn::text("steered answer"),
        ]),
    )
    .checkpoint(CheckpointConfig {
        policy: CheckpointPolicy::Every(NonZeroU32::new(1).unwrap()),
        target_version: None,
    })
    .build()
    .unwrap();
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let orchestrator = AgentOrchestrator::builder(store)
        .register(definition)
        .unwrap()
        .start()
        .await
        .unwrap();
    let run = orchestrator
        .agent("checkpointed-steering")
        .unwrap()
        .start_with_id("checkpointed-steering-run", "initial prompt")
        .await
        .unwrap();

    run.steer("change direction").await.unwrap();
    let response = run.wait().await.unwrap();

    assert_eq!(response.output, "steered answer");
    assert_eq!(response.messages.unwrap().len(), 2);
    assert_eq!(
        orchestrator
            .client()
            .list_executions(run.instance_id())
            .await
            .unwrap(),
        [1, 2]
    );
    orchestrator.shutdown(None).await;
}

#[tokio::test]
async fn steering_rejects_a_completed_run() {
    let definition = DurableAgent::builder(
        "completed-steering",
        MockCompletionModel::from_turns([MockTurn::text("done")]),
    )
    .build()
    .unwrap();
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let orchestrator = AgentOrchestrator::builder(store)
        .register(definition)
        .unwrap()
        .start()
        .await
        .unwrap();
    let run = orchestrator
        .agent("completed-steering")
        .unwrap()
        .start("initial prompt")
        .await
        .unwrap();
    assert_eq!(run.wait().await.unwrap().output, "done");

    assert!(matches!(
        run.steer("too late").await,
        Err(AgentOrchestratorError::SteeringNotAccepted(_))
    ));
    orchestrator.shutdown(None).await;
}

#[tokio::test]
async fn durable_sub_agent_is_registered_and_called_as_a_tool() {
    let child = DurableAgent::builder(
        "researcher",
        MockCompletionModel::from_turns([MockTurn::text("child report")]),
    )
    .description("Research a prompt in a durable child agent")
    .build()
    .unwrap();
    let parent = DurableAgent::builder(
        "assistant",
        MockCompletionModel::from_turns([
            MockTurn::tool_call(
                "research-call",
                "research",
                serde_json::json!({"prompt":"investigate durability"}),
            ),
            MockTurn::text("parent complete"),
        ]),
    )
    .sub_agent("research", child)
    .unwrap()
    .build()
    .unwrap();
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let orchestrator = AgentOrchestrator::builder(store)
        .register(parent)
        .unwrap()
        .start()
        .await
        .unwrap();

    assert_eq!(
        orchestrator
            .agent("assistant")
            .unwrap()
            .prompt("delegate")
            .await
            .unwrap(),
        "parent complete"
    );
    assert!(orchestrator.agent("researcher").is_ok());
    orchestrator.shutdown(None).await;
}

#[tokio::test]
async fn caller_run_ids_are_scoped_by_agent_and_version() {
    let first = DurableAgent::builder(
        "first",
        MockCompletionModel::from_turns([MockTurn::text("first output")]),
    )
    .build()
    .unwrap();
    let second = DurableAgent::builder(
        "second",
        MockCompletionModel::from_turns([MockTurn::text("second output")]),
    )
    .build()
    .unwrap();
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let orchestrator = AgentOrchestrator::builder(store)
        .register(first)
        .unwrap()
        .register(second)
        .unwrap()
        .start()
        .await
        .unwrap();
    let first_run = orchestrator
        .agent("first")
        .unwrap()
        .start_with_id("shared-caller-id", "first prompt")
        .await
        .unwrap();
    let second_run = orchestrator
        .agent("second")
        .unwrap()
        .start_with_id("shared-caller-id", "second prompt")
        .await
        .unwrap();

    assert_ne!(first_run.instance_id(), second_run.instance_id());
    assert_eq!(first_run.wait().await.unwrap().output, "first output");
    assert_eq!(second_run.wait().await.unwrap().output, "second output");
    orchestrator.shutdown(None).await;
}

#[tokio::test]
async fn invalid_version_compositions_fail_before_runtime_start() {
    let approval_v1 = DurableAgent::builder(
        "approval-versioned",
        MockCompletionModel::from_turns([MockTurn::text("v1")]),
    )
    .version("1.0.0")
    .unwrap()
    .approval_queue("queue-v1")
    .build()
    .unwrap();
    let approval_v2 = DurableAgent::builder(
        "approval-versioned",
        MockCompletionModel::from_turns([MockTurn::text("v2")]),
    )
    .version("2.0.0")
    .unwrap()
    .approval_queue("queue-v2")
    .build()
    .unwrap();
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let builder = AgentOrchestrator::builder(store)
        .register(approval_v1)
        .unwrap();
    assert!(matches!(
        builder.register(approval_v2),
        Err(AgentOrchestratorError::ApprovalQueueMismatch { .. })
    ));

    let missing_target = DurableAgent::builder(
        "checkpoint-versioned",
        MockCompletionModel::from_turns([MockTurn::text("v1")]),
    )
    .checkpoint(CheckpointConfig {
        policy: CheckpointPolicy::Every(NonZeroU32::new(1).unwrap()),
        target_version: Some("2.0.0".into()),
    })
    .build()
    .unwrap();
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    assert!(matches!(
        AgentOrchestrator::builder(store)
            .register(missing_target)
            .unwrap()
            .start()
            .await,
        Err(AgentOrchestratorError::AgentVersionNotFound { .. })
    ));
}

#[tokio::test]
async fn checkpoint_can_move_a_run_to_a_registered_agent_version() {
    let v1 = DurableAgent::builder(
        "migrating-calculator",
        MockCompletionModel::from_turns([MockTurn::tool_call(
            "call",
            "add",
            serde_json::json!({"x":20,"y":22}),
        )]),
    )
    .version("1.0.0")
    .unwrap()
    .tool(MockAddTool)
    .checkpoint(CheckpointConfig {
        policy: CheckpointPolicy::Every(NonZeroU32::new(1).unwrap()),
        target_version: Some("2.0.0".into()),
    })
    .build()
    .unwrap();
    let v2 = DurableAgent::builder(
        "migrating-calculator",
        MockCompletionModel::from_turns([MockTurn::text("migrated answer")]),
    )
    .version("2.0.0")
    .unwrap()
    .tool(MockAddTool)
    .build()
    .unwrap();
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let orchestrator = AgentOrchestrator::builder(store)
        .register(v1)
        .unwrap()
        .register(v2)
        .unwrap()
        .start()
        .await
        .unwrap();

    let response = orchestrator
        .agent_version("migrating-calculator", "1.0.0")
        .unwrap()
        .start_with_id("version-migration", "add")
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(response.output, "migrated answer");
    orchestrator.shutdown(None).await;
}

#[test]
fn approval_enabled_agents_are_rejected_as_sub_agents() {
    let child = DurableAgent::builder(
        "approved-child",
        MockCompletionModel::from_turns([MockTurn::text("child")]),
    )
    .tool_with(MockAddTool, ToolOptions::default().require_approval())
    .build()
    .unwrap();
    let result = DurableAgent::builder(
        "parent",
        MockCompletionModel::from_turns([MockTurn::text("parent")]),
    )
    .sub_agent("child", child);
    assert!(matches!(
        result,
        Err(AgentOrchestratorError::SubAgentApprovalUnsupported(name)) if name == "approved-child"
    ));
}

#[derive(Clone)]
struct InterruptedTool {
    calls: Arc<AtomicUsize>,
    block: bool,
    entered: Arc<tokio::sync::Notify>,
}

#[derive(Deserialize)]
struct NoArgs {}

impl Tool for InterruptedTool {
    const NAME: &'static str = "interruptible";
    type Args = NoArgs;
    type Output = &'static str;
    type Error = Infallible;

    fn description(&self) -> String {
        "A tool used to prove runtime restart recovery".into()
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({"type":"object"})
    }

    async fn call(
        &self,
        _context: &mut ToolContext,
        _args: NoArgs,
    ) -> Result<Self::Output, Self::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_waiters();
        if self.block {
            std::future::pending::<()>().await;
        }
        Ok("recovered")
    }
}

#[tokio::test]
async fn reconnects_to_a_run_after_runtime_restart_without_recalling_the_model_turn() {
    let first_calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let first = DurableAgent::builder(
        "restartable",
        MockCompletionModel::from_turns([MockTurn::tool_call(
            "call",
            "interruptible",
            serde_json::json!({}),
        )]),
    )
    .tool_with(
        InterruptedTool {
            calls: first_calls.clone(),
            block: true,
            entered: entered.clone(),
        },
        ToolOptions::default().retry(RetryPolicy::new(1)),
    )
    .build()
    .unwrap();
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let first_orchestrator = AgentOrchestrator::builder(store.clone())
        .register(first)
        .unwrap()
        .runtime_options(RuntimeOptions {
            worker_lock_timeout: Duration::from_secs(1),
            ..Default::default()
        })
        .start()
        .await
        .unwrap();
    first_orchestrator
        .agent("restartable")
        .unwrap()
        .start_with_id("restart-facade", "run")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    first_orchestrator.shutdown(Some(0)).await;

    let recovered_calls = Arc::new(AtomicUsize::new(0));
    let recovered = DurableAgent::builder(
        "restartable",
        MockCompletionModel::from_turns([MockTurn::text("finished after restart")]),
    )
    .tool_with(
        InterruptedTool {
            calls: recovered_calls.clone(),
            block: false,
            entered: Arc::new(tokio::sync::Notify::new()),
        },
        ToolOptions::default().retry(RetryPolicy::new(1)),
    )
    .build()
    .unwrap();
    let second_orchestrator = AgentOrchestrator::builder(store)
        .register(recovered)
        .unwrap()
        .runtime_options(RuntimeOptions {
            worker_lock_timeout: Duration::from_secs(1),
            ..Default::default()
        })
        .start()
        .await
        .unwrap();
    let response = second_orchestrator
        .agent("restartable")
        .unwrap()
        .run("restart-facade")
        .wait_timeout(Duration::from_secs(10))
        .await
        .unwrap();

    assert_eq!(response.output, "finished after restart");
    assert_eq!(recovered_calls.load(Ordering::SeqCst), 1);
    second_orchestrator.shutdown(None).await;
}
