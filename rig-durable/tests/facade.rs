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
    assert_eq!(run.wait().await.unwrap().output(), "approved");
    orchestrator.shutdown(None).await;
}

#[tokio::test]
async fn next_approval_times_out_with_running_non_json_custom_status() {
    use duroxide::{ClientError, OrchestrationStatus, runtime::registry::OrchestrationRegistry};

    let definition = DurableAgent::builder(
        "malformed-approval-status",
        MockCompletionModel::from_turns([MockTurn::text("unused")]),
    )
    .build()
    .unwrap();
    let raw = OrchestrationRegistry::builder()
        .register("NonJsonStatus", |ctx, _input| async move {
            ctx.set_custom_status("not JSON");
            ctx.schedule_wait("never-released").await;
            Ok("done".into())
        })
        .build();
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let orchestrator = AgentOrchestrator::builder(store)
        .register(definition)
        .unwrap()
        .merge_orchestrations(raw)
        .unwrap()
        .start()
        .await
        .unwrap();
    let run = orchestrator
        .agent("malformed-approval-status")
        .unwrap()
        .run("malformed-status-deadline");
    orchestrator
        .client()
        .start_orchestration(run.instance_id(), "NonJsonStatus", "")
        .await
        .unwrap();

    // Observe persisted malformed status, not merely a scheduled/not-found run,
    // before pausing time or starting the approval deadline.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let OrchestrationStatus::Running {
                custom_status: Some(status),
                ..
            } = orchestrator
                .client()
                .get_orchestration_status(run.instance_id())
                .await
                .unwrap()
                && status == "not JSON"
            {
                assert!(serde_json::from_str::<serde_json::Value>(&status).is_err());
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("raw orchestration must be running with non-JSON custom status");

    tokio::time::pause();
    let approval = run.next_approval();
    tokio::pin!(approval);
    // Poll the same future before advancing past its internally created deadline.
    assert!(
        tokio::time::timeout(Duration::from_secs(1), &mut approval)
            .await
            .is_err(),
        "a running malformed status must keep waiting before the deadline"
    );
    tokio::time::advance(Duration::from_secs(300)).await;
    // SQLite uses real worker threads; give them a real-time budget rather than
    // letting paused Tokio time auto-advance the outer guard ahead of their IO.
    tokio::time::resume();
    let result = tokio::time::timeout(Duration::from_secs(1), &mut approval)
        .await
        .expect("malformed status must not bypass the approval deadline");
    assert!(matches!(
        result,
        Err(AgentOrchestratorError::Client(ClientError::Timeout))
    ));
    orchestrator.shutdown(Some(0)).await;
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

    assert_eq!(response.output(), "steered answer");
    assert_eq!(response.messages.len(), 2);
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

    assert_eq!(response.output(), "steered answer");
    assert_eq!(response.messages.len(), 2);
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
    assert_eq!(run.wait().await.unwrap().output(), "done");

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
async fn logical_child_uses_versioned_registration_until_its_first_checkpoint() {
    let child = DurableAgent::builder(
        "child",
        MockCompletionModel::from_turns([
            MockTurn::tool_call("add", "add", serde_json::json!({"x":1,"y":2})),
            MockTurn::text("report"),
        ]),
    )
    .invocation_contract(rig_durable::InvocationContract::Logical)
    .preamble("registered child config")
    .tool(MockAddTool)
    .checkpoint(CheckpointConfig {
        policy: CheckpointPolicy::Every(NonZeroU32::new(1).unwrap()),
        target_version: None,
    })
    .build()
    .unwrap();
    let parent = DurableAgent::builder(
        "parent",
        MockCompletionModel::from_turns([
            MockTurn::tool_call("child", "child", serde_json::json!({"prompt":"work"})),
            MockTurn::text("done"),
        ]),
    )
    .invocation_contract(rig_durable::InvocationContract::Logical)
    .sub_agent("child", child)
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
    let run = orchestrator
        .agent("parent")
        .unwrap()
        .start("delegate")
        .await
        .unwrap();
    assert_eq!(run.wait().await.unwrap().output(), "done");
    let history = orchestrator
        .client()
        .read_execution_history(run.instance_id(), 1)
        .await
        .unwrap();
    let input = history
        .iter()
        .find_map(|event| match &event.kind {
            duroxide::EventKind::SubOrchestrationScheduled { input, .. } => Some(input),
            _ => None,
        })
        .unwrap();
    let input: rig_durable::AgentInput = serde_json::from_str(input).unwrap();
    assert!(input.snapshot.is_none());
    let instances = orchestrator.client().list_all_instances().await.unwrap();
    assert_eq!(instances.len(), 2);
    let child_id = instances
        .iter()
        .find(|id| id.as_str() != run.instance_id())
        .unwrap();
    let history = orchestrator
        .client()
        .read_execution_history(child_id, 2)
        .await
        .unwrap();
    let duroxide::EventKind::OrchestrationStarted { input, .. } = &history[0].kind else {
        panic!("missing child input")
    };
    let input: rig_durable::AgentInput = serde_json::from_str(input).unwrap();
    assert_eq!(
        input.snapshot.unwrap().preamble.as_deref(),
        Some("registered child config")
    );
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
    // Reconnecting to the same application identity cannot attach to an old
    // Rig generation's run or session, even at the same application semver.
    use sha2::{Digest, Sha256};
    let mut old_digest = Sha256::new();
    for part in ["first", "1.0.0", "shared-caller-id"] {
        old_digest.update((part.len() as u64).to_be_bytes());
        old_digest.update(part.as_bytes());
    }
    let old_hash = format!("{:x}", old_digest.finalize());
    assert_ne!(first_run.instance_id(), format!("rig-agent-{old_hash}"));
    assert_ne!(
        orchestrator
            .agent("first")
            .unwrap()
            .session("shared-caller-id")
            .instance_id(),
        format!("rig-session-{old_hash}")
    );
    assert_eq!(first_run.wait().await.unwrap().output(), "first output");
    assert_eq!(second_run.wait().await.unwrap().output(), "second output");
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
    assert_eq!(response.output(), "migrated answer");
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

    assert_eq!(response.output(), "finished after restart");
    assert_eq!(recovered_calls.load(Ordering::SeqCst), 1);
    second_orchestrator.shutdown(None).await;
}

#[cfg(feature = "codemode")]
#[tokio::test]
async fn code_mode_recovers_after_parallel_children_without_repeating_completed_work() {
    use rig::test_utils::MockBarrierTool;
    use rig_durable::{InvocationContract, ToolPolicy};
    struct RecordKey {
        tool: InterruptedTool,
        keys: Arc<std::sync::Mutex<Vec<String>>>,
    }
    impl Tool for RecordKey {
        const NAME: &'static str = "interruptible";
        type Args = NoArgs;
        type Output = &'static str;
        type Error = Infallible;
        fn description(&self) -> String {
            self.tool.description()
        }
        fn parameters(&self) -> serde_json::Value {
            self.tool.parameters()
        }
        async fn call(
            &self,
            context: &mut ToolContext,
            args: NoArgs,
        ) -> Result<Self::Output, Infallible> {
            let invocation = context.require::<rig_durable::ToolInvocation>().unwrap();
            self.keys
                .lock()
                .unwrap()
                .push(invocation.logical_key.unwrap().digest());
            self.tool.call(context, args).await
        }
    }
    let code = r#"
        const answers = await Promise.all([
            tools.research({prompt: "facts"}), tools.assess({prompt: "risks"})
        ]);
        if (answers[1] === "backup") {
            await tools.interruptible({});
            return await tools.review({prompt: answers.join("|")});
        }
        return "skipped";
    "#;
    let research_model = MockCompletionModel::from_turns([
        MockTurn::tool_call("barrier", "barrier_tool", serde_json::json!({})),
        MockTurn::text("staged rollout"),
    ]);
    let assess_model = MockCompletionModel::from_turns([
        MockTurn::tool_call("barrier", "barrier_tool", serde_json::json!({})),
        MockTurn::text("backup"),
    ]);
    let review_model = MockCompletionModel::from_turns([MockTurn::text("approved")]);
    let parent_model = MockCompletionModel::from_turns([
        MockTurn::tool_call("script", "codemode", serde_json::json!({"code":code})),
        MockTurn::text("complete"),
    ]);
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let keys = Arc::new(std::sync::Mutex::new(Vec::new()));
    let build = |block| {
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        DurableAgent::builder("workflow", parent_model.clone())
            .invocation_contract(InvocationContract::Logical)
            .sub_agent(
                "research",
                DurableAgent::builder("researcher", research_model.clone())
                    .tool(MockBarrierTool::new(barrier.clone()))
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .sub_agent(
                "assess",
                DurableAgent::builder("assessor", assess_model.clone())
                    .tool(MockBarrierTool::new(barrier))
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .sub_agent(
                "review",
                DurableAgent::builder("reviewer", review_model.clone())
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .tool_with(
                RecordKey {
                    tool: InterruptedTool {
                        calls: calls.clone(),
                        block,
                        entered: entered.clone(),
                    },
                    keys: keys.clone(),
                },
                ToolOptions::default()
                    .retry(RetryPolicy::new(1))
                    .policy(ToolPolicy::read_only()),
            )
            .code_mode(rig_codemode::Limits::default())
            .build()
            .unwrap()
    };
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let options = RuntimeOptions {
        worker_lock_timeout: Duration::from_secs(1),
        ..Default::default()
    };
    let first = AgentOrchestrator::builder(store.clone())
        .register(build(true))
        .unwrap()
        .runtime_options(options.clone())
        .start()
        .await
        .unwrap();
    first
        .agent("workflow")
        .unwrap()
        .start_with_id("recover-script", "plan")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .unwrap();
    assert_eq!(research_model.requests().len(), 2);
    assert_eq!(assess_model.requests().len(), 2);
    assert_eq!(review_model.requests().len(), 0);
    first.shutdown(Some(0)).await;
    let second = AgentOrchestrator::builder(store)
        .register(build(false))
        .unwrap()
        .runtime_options(options)
        .start()
        .await
        .unwrap();
    let response = second
        .agent("workflow")
        .unwrap()
        .run("recover-script")
        .wait_timeout(Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(response.output(), "complete");
    let attempts = calls.load(Ordering::SeqCst);
    assert!(attempts >= 2, "the unfinished activity must be redelivered");
    {
        let keys = keys.lock().unwrap();
        assert_eq!(keys.len(), attempts);
        assert!(
            keys.iter().all(|key| key == &keys[0]),
            "redelivery must keep the logical call identity"
        );
    }
    assert_eq!(research_model.requests().len(), 2);
    assert_eq!(assess_model.requests().len(), 2);
    assert_eq!(review_model.requests().len(), 1);
    let review_request = serde_json::to_string(&review_model.requests()[0]).unwrap();
    assert!(review_request.contains("staged rollout|backup"));
    let requests = parent_model.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0]
            .tools
            .iter()
            .map(|t| t.name.as_str())
            .collect::<Vec<_>>(),
        ["codemode"]
    );
    let report = serde_json::to_string(&requests[1]).unwrap();
    assert!(report.contains("approved"));
    assert!(report.contains("completed"));
    second.shutdown(None).await;
}

#[cfg(feature = "codemode")]
#[tokio::test]
async fn code_mode_fast_result_advances_script_and_drains_race_loser() {
    use rig_codemode::{ExecutionReport, ScriptDelivery};
    use tokio::sync::Notify;

    #[derive(Deserialize)]
    struct Args {
        key: String,
    }
    struct Probe {
        started: Arc<Notify>,
        release: Arc<Notify>,
        followed: Arc<Notify>,
        calls: Arc<AtomicUsize>,
    }
    impl Tool for Probe {
        const NAME: &'static str = "probe";
        type Args = Args;
        type Output = String;
        type Error = Infallible;
        fn description(&self) -> String {
            "Return a key after its release".into()
        }
        fn parameters(&self) -> serde_json::Value {
            serde_json::json!({"type":"object","properties":{"key":{"type":"string"}},"required":["key"]})
        }
        async fn call(&self, _: &mut ToolContext, args: Args) -> Result<String, Infallible> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if args.key == "slow" {
                self.started.notify_one();
                self.release.notified().await;
            } else if args.key == "follow:fast" {
                self.followed.notify_one();
            }
            Ok(args.key)
        }
    }
    let code = r#"
        const slow = tools.probe({key: "slow"});
        const invalid = tools.probe({key: 12}).catch(error => error.kind);
        const schemaWinner = await Promise.race([slow, invalid]);
        await tools.probe({key: "follow:" + schemaWinner});
        const fast = tools.probe({key: "fast"});
        const winner = await Promise.race([slow, fast]);
        await tools.probe({key: "follow:" + winner});
        return {schemaWinner, winner};
    "#;
    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call("script", "codemode", serde_json::json!({"code":code})),
        MockTurn::text("done"),
    ]);
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let followed = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let definition = DurableAgent::builder("race", model.clone())
        .tool_with(
            Probe {
                started: started.clone(),
                release: release.clone(),
                followed: followed.clone(),
                calls: calls.clone(),
            },
            ToolOptions::default().retry(RetryPolicy::new(1)),
        )
        .code_mode(rig_codemode::Limits {
            max_in_flight: 2,
            ..Default::default()
        })
        .build()
        .unwrap();
    let runtime =
        AgentOrchestrator::builder(Arc::new(SqliteProvider::new_in_memory().await.unwrap()))
            .register(definition)
            .unwrap()
            .start()
            .await
            .unwrap();
    let run = runtime.agent("race").unwrap().start("run").await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        started.notified().await;
        followed.notified().await;
    })
    .await
    .expect("fast result must start follow-up before slow completion");
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), run.wait())
            .await
            .is_err(),
        "the host must retain the unawaited race loser"
    );
    release.notify_one();
    assert_eq!(
        run.wait_timeout(Duration::from_secs(10))
            .await
            .unwrap()
            .output(),
        "done"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    let requests = model.requests();
    fn find(value: &serde_json::Value) -> Option<ExecutionReport> {
        if let Ok(report) = serde_json::from_value::<ExecutionReport>(value.clone()) {
            return Some(report);
        }
        match value {
            serde_json::Value::Object(map) => map.values().find_map(find),
            serde_json::Value::Array(items) => items.iter().find_map(find),
            serde_json::Value::String(text) => {
                serde_json::from_str(text).ok().as_ref().and_then(find)
            }
            _ => None,
        }
    }
    let report = find(&serde_json::to_value(&requests[1].chat_history).unwrap())
        .expect("coordinator must receive an execution report");
    assert_eq!(
        report.returned,
        Some(serde_json::json!({"schemaWinner":"invalid_args", "winner":"fast"}))
    );
    assert_eq!(report.calls[0].delivery, ScriptDelivery::Discarded);
    assert_eq!(report.calls[3].delivery, ScriptDelivery::Delivered);
    runtime.shutdown(None).await;
}
