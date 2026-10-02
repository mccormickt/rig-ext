#![cfg(feature = "temporal")]

use std::{
    convert::Infallible,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use rig::{
    completion::Message,
    test_utils::{MockAddTool, MockCompletionModel, MockTurn},
    tool::{Tool, ToolContext, ToolExecutionError},
};
use rig_durable::{
    ApprovalDecision, InMemoryGuardStore, InvocationContract, InvocationGuardStore, LogicalCallKey,
    ToolInvocation, ToolPolicy,
    temporal::{
        TemporalAgent, TemporalAgentError, TemporalAgentInput, TemporalAgentSessionSnapshot,
        TemporalAgentSessionWorkflow, TemporalAgentStatus, TemporalAgentWorkflow,
    },
};
use serde::Deserialize;
use temporalio_client::{
    Client, ClientOptions, Connection, WorkflowExecuteUpdateOptions, WorkflowGetResultOptions,
    WorkflowQueryOptions, WorkflowSignalOptions, WorkflowStartOptions,
    envconfig::LoadClientConfigProfileOptions,
};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};
use tokio::sync::Notify;

#[derive(Deserialize)]
struct LookupArgs {
    key: String,
}

#[derive(Clone)]
struct FlakyLookup {
    attempts: Arc<AtomicUsize>,
    fail_until: usize,
    invocations: Arc<Mutex<Vec<ToolInvocation>>>,
}

impl Tool for FlakyLookup {
    const NAME: &'static str = "lookup";
    type Args = LookupArgs;
    type Output = String;
    type Error = io::Error;

    fn description(&self) -> String {
        "Look up a value from a temporarily unreliable service".into()
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {"key": {"type": "string"}},
            "required": ["key"]
        })
    }

    fn map_error(&self, error: Self::Error) -> ToolExecutionError {
        ToolExecutionError::provider(error.to_string()).with_source(error)
    }

    async fn call(
        &self,
        context: &mut ToolContext,
        args: LookupArgs,
    ) -> Result<Self::Output, Self::Error> {
        self.invocations
            .lock()
            .unwrap()
            .push(context.require::<ToolInvocation>().unwrap().clone());
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt <= self.fail_until {
            return Err(io::Error::other("temporary upstream failure"));
        }
        Ok(format!("{}=available", args.key))
    }
}

#[derive(Clone)]
struct BlockingLookup {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

impl Tool for BlockingLookup {
    const NAME: &'static str = "blocking_lookup";
    type Args = LookupArgs;
    type Output = String;
    type Error = Infallible;

    fn description(&self) -> String {
        "Wait for an external release, then return a value".into()
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {"key": {"type": "string"}},
            "required": ["key"]
        })
    }

    async fn call(
        &self,
        _context: &mut ToolContext,
        args: LookupArgs,
    ) -> Result<Self::Output, Self::Error> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(format!("{}=released", args.key))
    }
}

async fn live_client() -> (Runtime, Client) {
    let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
    let (connection_options, client_options) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default()).unwrap();
    let connection = Connection::connect(connection_options).await.unwrap();
    let client = Client::new(connection, client_options).unwrap();
    (runtime, client)
}

#[test]
fn temporal_agent_builds_reusable_inputs_and_registers_a_worker() {
    let agent = TemporalAgent::new(MockCompletionModel::from_turns([MockTurn::text("done")]))
        .preamble("Use tools when needed.")
        .tool(MockAddTool);

    let first = agent.input("first");
    let second = agent.input("second");
    assert_eq!(
        first.config.preamble.as_deref(),
        Some("Use tools when needed.")
    );
    assert_eq!(first.config.tools[0].definition.name, "add");
    assert!(!first.config.tools[0].requires_approval);
    assert_ne!(first.prompt, second.prompt);
    let session = agent.session_input(Vec::new());
    assert_eq!(session.config.tools[0].definition.name, "add");

    let mut options = WorkerOptions::new("rig-temporal-test").build();
    agent.register(&mut options).unwrap();
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_session_accepts_multiple_prompts_and_closes() {
    let (runtime, client) = live_client().await;
    let agent = TemporalAgent::new(MockCompletionModel::from_turns([
        MockTurn::text("first answer"),
        MockTurn::text("second answer"),
    ]));
    let input = agent.session_input(Vec::new());
    let task_queue = "rig-temporal-session-live-test";
    let mut options = WorkerOptions::new(task_queue).build();
    agent.register(&mut options).unwrap();
    let mut worker = Worker::new(&runtime, client.clone(), options).unwrap();
    let shutdown = worker.shutdown_handle();
    let run = async move {
        let handle = client
            .start_workflow(
                TemporalAgentSessionWorkflow::run,
                input,
                WorkflowStartOptions::new(
                    task_queue,
                    format!("rig-temporal-session-test-{}", uuid::Uuid::new_v4()),
                )
                .build(),
            )
            .await
            .unwrap();
        let first = handle
            .execute_update(
                TemporalAgentSessionWorkflow::prompt,
                "first prompt".into(),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
            .unwrap();
        let second = handle
            .execute_update(
                TemporalAgentSessionWorkflow::prompt,
                "second prompt".into(),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
            .unwrap();
        handle
            .signal(
                TemporalAgentSessionWorkflow::close,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        let result = handle
            .get_result(WorkflowGetResultOptions::default())
            .await
            .unwrap();
        shutdown();
        (first, second, result)
    };
    let (worker_result, (first, second, result)) = tokio::join!(worker.run(), run);

    worker_result.unwrap();
    assert_eq!(first.output, "first answer");
    assert_eq!(second.output, "second answer");
    assert_eq!(result.history.len(), 4);
}

#[test]
fn temporal_approval_is_opt_in_per_tool() {
    let agent = TemporalAgent::new(MockCompletionModel::from_turns([MockTurn::text("done")]))
        .approval_tool(MockAddTool);

    let input = agent.input("add");
    assert!(input.config.tools[0].requires_approval);
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_server_executes_model_and_tool_activities() {
    let (runtime, client) = live_client().await;
    let agent = TemporalAgent::new(MockCompletionModel::from_turns([
        MockTurn::tool_call("call-1", "add", serde_json::json!({"x": 20, "y": 22})),
        MockTurn::text("The answer is 42."),
    ]))
    .tool(MockAddTool);
    let input = agent.input("What is 20 + 22?");
    let mut options = WorkerOptions::new("rig-temporal-live-test").build();
    agent.register(&mut options).unwrap();
    let mut worker = Worker::new(&runtime, client.clone(), options).unwrap();
    let shutdown = worker.shutdown_handle();
    let run = async move {
        let handle = client
            .start_workflow(
                TemporalAgentWorkflow::run,
                input,
                WorkflowStartOptions::new(
                    "rig-temporal-live-test",
                    format!("rig-temporal-test-{}", uuid::Uuid::new_v4()),
                )
                .build(),
            )
            .await
            .unwrap();
        let response = handle
            .get_result(WorkflowGetResultOptions::default())
            .await
            .unwrap();
        shutdown();
        response
    };
    let (worker_result, response) = tokio::join!(worker.run(), run);

    worker_result.unwrap();
    assert_eq!(response.output, "The answer is 42.");
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_retries_tools_with_stable_invocation_identity() {
    let (runtime, client) = live_client().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let invocations = Arc::new(Mutex::new(Vec::new()));
    let agent = TemporalAgent::new(MockCompletionModel::from_turns([
        MockTurn::tool_call("lookup-1", "lookup", serde_json::json!({"key": "service"})),
        MockTurn::text("available"),
    ]))
    .activity_max_attempts(3)
    .tool(FlakyLookup {
        attempts: Arc::clone(&attempts),
        fail_until: 1,
        invocations: Arc::clone(&invocations),
    });
    let input = agent.input("check service");
    let task_queue = format!("rig-temporal-retry-test-{}", uuid::Uuid::new_v4());
    let mut options = WorkerOptions::new(task_queue.clone()).build();
    agent.register(&mut options).unwrap();
    let mut worker = Worker::new(&runtime, client.clone(), options).unwrap();
    let shutdown = worker.shutdown_handle();
    let run = async move {
        let handle = client
            .start_workflow(
                TemporalAgentWorkflow::run,
                input,
                WorkflowStartOptions::new(
                    task_queue,
                    format!("rig-temporal-retry-test-{}", uuid::Uuid::new_v4()),
                )
                .build(),
            )
            .await
            .unwrap();
        let result = handle
            .get_result(WorkflowGetResultOptions::default())
            .await
            .unwrap();
        shutdown();
        result
    };
    let (worker_result, response) = tokio::join!(worker.run(), run);

    worker_result.unwrap();
    assert_eq!(response.output, "available");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let invocations = invocations.lock().unwrap();
    assert_eq!(invocations.len(), 2);
    let logical = |invocation: &ToolInvocation| {
        (
            invocation.execution_id.clone(),
            invocation.prompt_index,
            invocation.turn,
            invocation.call_index,
            invocation
                .logical_key
                .clone()
                .expect("logical contract supplies a key"),
        )
    };
    assert_eq!(logical(&invocations[0]), logical(&invocations[1]));
    assert_eq!(
        invocations[0].logical_key.as_ref().unwrap().submission_id,
        "prompt-0"
    );
    let attempt = |invocation: &ToolInvocation| {
        invocation
            .attempt
            .as_ref()
            .and_then(|attempt| attempt.activity_attempt)
    };
    assert_eq!(attempt(&invocations[0]), Some(1));
    assert_eq!(attempt(&invocations[1]), Some(2));
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_fails_after_tool_retries_are_exhausted() {
    let (runtime, client) = live_client().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let agent = TemporalAgent::new(MockCompletionModel::from_turns([MockTurn::tool_call(
        "lookup-1",
        "lookup",
        serde_json::json!({"key": "service"}),
    )]))
    .activity_max_attempts(2)
    .tool(FlakyLookup {
        attempts: Arc::clone(&attempts),
        fail_until: usize::MAX,
        invocations: Arc::new(Mutex::new(Vec::new())),
    });
    let input = agent.input("check service");
    let task_queue = format!("rig-temporal-exhausted-test-{}", uuid::Uuid::new_v4());
    let mut options = WorkerOptions::new(task_queue.clone()).build();
    agent.register(&mut options).unwrap();
    let mut worker = Worker::new(&runtime, client.clone(), options).unwrap();
    let shutdown = worker.shutdown_handle();
    let run = async move {
        let handle = client
            .start_workflow(
                TemporalAgentWorkflow::run,
                input,
                WorkflowStartOptions::new(
                    task_queue,
                    format!("rig-temporal-exhausted-test-{}", uuid::Uuid::new_v4()),
                )
                .build(),
            )
            .await
            .unwrap();
        let result = handle.get_result(WorkflowGetResultOptions::default()).await;
        shutdown();
        result
    };
    let (worker_result, result) = tokio::join!(worker.run(), run);

    worker_result.unwrap();
    assert!(result.is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_approval_executes_only_approved_tools() {
    let (runtime, client) = live_client().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let agent = TemporalAgent::new(MockCompletionModel::from_turns([
        MockTurn::tool_call(
            "approved-call",
            "lookup",
            serde_json::json!({"key": "approved"}),
        ),
        MockTurn::text("approved"),
        MockTurn::tool_call(
            "denied-call",
            "lookup",
            serde_json::json!({"key": "denied"}),
        ),
        MockTurn::text("denied"),
    ]))
    .approval_tool(FlakyLookup {
        attempts: Arc::clone(&attempts),
        fail_until: 0,
        invocations: Arc::new(Mutex::new(Vec::new())),
    });
    let approved_input = agent.input("approve lookup");
    let denied_input = agent.input("deny lookup");
    let task_queue = format!("rig-temporal-approval-test-{}", uuid::Uuid::new_v4());
    let mut options = WorkerOptions::new(task_queue.clone()).build();
    agent.register(&mut options).unwrap();
    let mut worker = Worker::new(&runtime, client.clone(), options).unwrap();
    let shutdown = worker.shutdown_handle();
    let run = async move {
        let approved = client
            .start_workflow(
                TemporalAgentWorkflow::run,
                approved_input,
                WorkflowStartOptions::new(
                    task_queue.clone(),
                    format!("rig-temporal-approved-test-{}", uuid::Uuid::new_v4()),
                )
                .build(),
            )
            .await
            .unwrap();
        let approved_request = loop {
            let status = approved
                .query(
                    TemporalAgentWorkflow::status,
                    (),
                    WorkflowQueryOptions::default(),
                )
                .await
                .unwrap();
            if let TemporalAgentStatus::Approval { request } = status {
                break request;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        approved
            .signal(
                TemporalAgentWorkflow::approval,
                ApprovalDecision::Approve {
                    approval_id: approved_request.approval_id,
                },
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        let approved_response = approved
            .get_result(WorkflowGetResultOptions::default())
            .await
            .unwrap();

        let denied = client
            .start_workflow(
                TemporalAgentWorkflow::run,
                denied_input,
                WorkflowStartOptions::new(
                    task_queue,
                    format!("rig-temporal-denied-test-{}", uuid::Uuid::new_v4()),
                )
                .build(),
            )
            .await
            .unwrap();
        let denied_request = loop {
            let status = denied
                .query(
                    TemporalAgentWorkflow::status,
                    (),
                    WorkflowQueryOptions::default(),
                )
                .await
                .unwrap();
            if let TemporalAgentStatus::Approval { request } = status {
                break request;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        denied
            .signal(
                TemporalAgentWorkflow::approval,
                ApprovalDecision::Deny {
                    approval_id: denied_request.approval_id,
                    reason: Some("not permitted".into()),
                },
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        let denied_response = denied
            .get_result(WorkflowGetResultOptions::default())
            .await
            .unwrap();
        shutdown();
        (approved_response, denied_response)
    };
    let (worker_result, (approved, denied)) = tokio::join!(worker.run(), run);

    worker_result.unwrap();
    assert_eq!(approved.output, "approved");
    assert_eq!(denied.output, "denied");
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_session_consumes_idle_steering_before_close() {
    let (runtime, client) = live_client().await;
    let agent = TemporalAgent::new(MockCompletionModel::from_turns([MockTurn::text(
        "steered answer",
    )]));
    let input = agent.session_input(Vec::new());
    let task_queue = format!("rig-temporal-idle-steer-test-{}", uuid::Uuid::new_v4());
    let mut options = WorkerOptions::new(task_queue.clone()).build();
    agent.register(&mut options).unwrap();
    let mut worker = Worker::new(&runtime, client.clone(), options).unwrap();
    let shutdown = worker.shutdown_handle();
    let run = async move {
        let handle = client
            .start_workflow(
                TemporalAgentSessionWorkflow::run,
                input,
                WorkflowStartOptions::new(
                    task_queue,
                    format!("rig-temporal-idle-steer-test-{}", uuid::Uuid::new_v4()),
                )
                .build(),
            )
            .await
            .unwrap();
        handle
            .signal(
                TemporalAgentSessionWorkflow::steer,
                Message::from("idle steering"),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let snapshot: TemporalAgentSessionSnapshot = handle
                    .query(
                        TemporalAgentSessionWorkflow::snapshot,
                        (),
                        WorkflowQueryOptions::default(),
                    )
                    .await
                    .unwrap();
                if snapshot.history.len() == 2 && !snapshot.busy {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        handle
            .signal(
                TemporalAgentSessionWorkflow::close,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        let result = handle
            .get_result(WorkflowGetResultOptions::default())
            .await
            .unwrap();
        shutdown();
        result
    };
    let (worker_result, result) = tokio::join!(worker.run(), run);

    worker_result.unwrap();
    assert_eq!(result.history.len(), 2);
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_session_serializes_updates_and_drains_active_steering_on_close() {
    let (runtime, client) = live_client().await;
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let agent = TemporalAgent::new(MockCompletionModel::from_turns([
        MockTurn::tool_call(
            "blocking-call",
            "blocking_lookup",
            serde_json::json!({"key": "service"}),
        ),
        MockTurn::text("first answer"),
        MockTurn::text("steered answer"),
    ]))
    .tool(BlockingLookup {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
    });
    let input = agent.session_input(Vec::new());
    let task_queue = format!("rig-temporal-control-test-{}", uuid::Uuid::new_v4());
    let mut options = WorkerOptions::new(task_queue.clone()).build();
    agent.register(&mut options).unwrap();
    let mut worker = Worker::new(&runtime, client.clone(), options).unwrap();
    let shutdown = worker.shutdown_handle();
    let run = async move {
        let handle = client
            .start_workflow(
                TemporalAgentSessionWorkflow::run,
                input,
                WorkflowStartOptions::new(
                    task_queue,
                    format!("rig-temporal-control-test-{}", uuid::Uuid::new_v4()),
                )
                .build(),
            )
            .await
            .unwrap();
        let first = handle.execute_update(
            TemporalAgentSessionWorkflow::prompt,
            Message::from("first prompt"),
            WorkflowExecuteUpdateOptions::default(),
        );
        tokio::pin!(first);
        tokio::select! {
            result = &mut first => panic!("first update completed before tool release: {result:?}"),
            result = tokio::time::timeout(Duration::from_secs(20), started.notified()) => {
                result.unwrap();
            }
        }
        let concurrent = handle
            .execute_update(
                TemporalAgentSessionWorkflow::prompt,
                Message::from("concurrent prompt"),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await;
        assert!(concurrent.is_err());
        handle
            .signal(
                TemporalAgentSessionWorkflow::steer,
                Message::from("active steering"),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        handle
            .signal(
                TemporalAgentSessionWorkflow::close,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        release.notify_one();
        let response = first.await.unwrap();
        let result = handle
            .get_result(WorkflowGetResultOptions::default())
            .await
            .unwrap();
        shutdown();
        (response, result)
    };
    let (worker_result, (response, result)) = tokio::join!(worker.run(), run);

    worker_result.unwrap();
    assert_eq!(response.output, "steered answer");
    assert_eq!(result.history.len(), 6);
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_session_rejects_oversized_prompts_and_can_close() {
    let (runtime, client) = live_client().await;
    let agent = TemporalAgent::new(MockCompletionModel::from_turns([MockTurn::text("unused")]))
        .session_history_max_bytes(8);
    let input = agent.session_input(Vec::new());
    let task_queue = format!("rig-temporal-size-test-{}", uuid::Uuid::new_v4());
    let mut options = WorkerOptions::new(task_queue.clone()).build();
    agent.register(&mut options).unwrap();
    let mut worker = Worker::new(&runtime, client.clone(), options).unwrap();
    let shutdown = worker.shutdown_handle();
    let run = async move {
        let handle = client
            .start_workflow(
                TemporalAgentSessionWorkflow::run,
                input,
                WorkflowStartOptions::new(
                    task_queue,
                    format!("rig-temporal-size-test-{}", uuid::Uuid::new_v4()),
                )
                .build(),
            )
            .await
            .unwrap();
        let update = handle
            .execute_update(
                TemporalAgentSessionWorkflow::prompt,
                Message::from("this prompt is too large"),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await;
        assert!(update.is_err());
        handle
            .signal(
                TemporalAgentSessionWorkflow::close,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();
        let result = handle
            .get_result(WorkflowGetResultOptions::default())
            .await
            .unwrap();
        shutdown();
        result
    };
    let (worker_result, result) = tokio::join!(worker.run(), run);

    worker_result.unwrap();
    assert!(result.history.is_empty());
}

/// External service with a persistent effect ledger. `write` records the
/// effect and then holds the attempt open until released, so the activity
/// times out after the effect was accepted but before completion was recorded.
#[derive(Clone)]
struct LedgerWrite {
    ledger: Arc<Mutex<Vec<String>>>,
    hold_first_attempt: Arc<Notify>,
    hold: bool,
}

impl Tool for LedgerWrite {
    const NAME: &'static str = "write";
    type Args = LookupArgs;
    type Output = String;
    type Error = Infallible;

    fn description(&self) -> String {
        "Write a value to an external ledger".into()
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {"key": {"type": "string"}},
            "required": ["key"]
        })
    }

    async fn call(
        &self,
        context: &mut ToolContext,
        args: LookupArgs,
    ) -> Result<Self::Output, Self::Error> {
        let invocation = context.require::<ToolInvocation>().unwrap();
        let logical_key = invocation.logical_key.as_ref().unwrap().digest();
        let first = {
            let mut ledger = self.ledger.lock().unwrap();
            if ledger.iter().any(|entry| entry == &logical_key) {
                false
            } else {
                ledger.push(logical_key);
                true
            }
        };
        if first && self.hold {
            self.hold_first_attempt.notified().await;
        }
        Ok(format!("{}=written", args.key))
    }
}

#[test]
fn temporal_registration_fails_closed_on_missing_contract_or_guard() {
    let legacy = TemporalAgent::new(MockCompletionModel::from_turns([MockTurn::text("done")]))
        .invocation_contract(InvocationContract::Legacy)
        .tool_with_policy(MockAddTool, ToolPolicy::read_only());
    let mut options = WorkerOptions::new("rig-temporal-legacy-policy").build();
    assert!(matches!(
        legacy.register(&mut options),
        Err(TemporalAgentError::InvocationContractRequired(name)) if name == "add"
    ));

    let unguarded = TemporalAgent::new(MockCompletionModel::from_turns([MockTurn::text("done")]))
        .tool_with_policy(MockAddTool, ToolPolicy::interrupt_on_uncertain());
    let mut options = WorkerOptions::new("rig-temporal-unguarded").build();
    assert!(matches!(
        unguarded.register(&mut options),
        Err(TemporalAgentError::GuardStoreRequired(name)) if name == "add"
    ));

    let guarded = TemporalAgent::new(MockCompletionModel::from_turns([MockTurn::text("done")]))
        .invocation_guard(Arc::new(InMemoryGuardStore::new()))
        .tool_with_policy(MockAddTool, ToolPolicy::interrupt_on_uncertain());
    let mut options = WorkerOptions::new("rig-temporal-guarded").build();
    guarded.register(&mut options).unwrap();
}

#[test]
fn temporal_inputs_recorded_before_policies_keep_the_legacy_contract() {
    let agent = TemporalAgent::new(MockCompletionModel::from_turns([MockTurn::text("done")]))
        .tool(MockAddTool);
    let fresh = agent.input("hello");
    assert_eq!(fresh.config.contract, InvocationContract::Logical);

    // An input recorded before contracts and policies existed has neither field.
    let mut recorded = serde_json::to_value(&fresh).unwrap();
    recorded["config"]
        .as_object_mut()
        .unwrap()
        .remove("contract")
        .expect("new inputs carry the contract");
    recorded["config"]["tools"][0]
        .as_object_mut()
        .unwrap()
        .remove("policy")
        .expect("new inputs carry the policy");
    let input: TemporalAgentInput = serde_json::from_value(recorded).unwrap();
    assert_eq!(input.config.contract, InvocationContract::Legacy);
    assert_eq!(input.config.tools[0].policy, ToolPolicy::default());
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_guarded_write_is_not_repeated_after_an_uncertain_attempt() {
    let (runtime, client) = live_client().await;
    let ledger = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(Notify::new());
    let guard = Arc::new(InMemoryGuardStore::new());
    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call("write-1", "write", serde_json::json!({"key": "order"})),
        MockTurn::text("finished"),
    ]);
    let agent = TemporalAgent::new(model.clone())
        .activity_timeout(Duration::from_secs(1))
        .activity_max_attempts(3)
        .invocation_guard(guard.clone())
        .tool_with_policy(
            LedgerWrite {
                ledger: Arc::clone(&ledger),
                hold_first_attempt: Arc::clone(&release),
                hold: true,
            },
            ToolPolicy::interrupt_on_uncertain(),
        );
    let input = agent.input("write the order");
    let task_queue = format!("rig-temporal-guarded-test-{}", uuid::Uuid::new_v4());
    let mut options = WorkerOptions::new(task_queue.clone()).build();
    agent.register(&mut options).unwrap();
    let mut worker = Worker::new(&runtime, client.clone(), options).unwrap();
    let shutdown = worker.shutdown_handle();
    let workflow_id = format!("rig-temporal-guarded-test-{}", uuid::Uuid::new_v4());
    let run = async move {
        let handle = client
            .start_workflow(
                TemporalAgentWorkflow::run,
                input,
                WorkflowStartOptions::new(task_queue, workflow_id.clone()).build(),
            )
            .await
            .unwrap();
        let response = handle
            .get_result(WorkflowGetResultOptions::default())
            .await
            .unwrap();
        release.notify_waiters();
        release.notify_one();
        shutdown();
        (workflow_id, response)
    };
    let (worker_result, (workflow_id, response)) = tokio::join!(worker.run(), run);

    worker_result.unwrap();
    assert_eq!(response.output, "finished");
    assert_eq!(ledger.lock().unwrap().len(), 1, "one external effect");
    let key = LogicalCallKey {
        logical_execution_id: workflow_id,
        submission_id: "prompt-0".into(),
        model_turn: 1,
        call_index: 0,
    };
    let record = guard.get(&key).await.unwrap().expect("claim recorded");
    assert_eq!(record.claim.attempt.activity_attempt, Some(1));
    let requests = model.requests();
    assert_eq!(requests.len(), 2);
    let text = serde_json::to_string(&requests[1].chat_history).unwrap();
    assert!(text.contains("was interrupted"), "{text}");
    assert!(!text.contains("=written"), "{text}");
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_idempotent_write_keeps_its_key_across_retries() {
    let (runtime, client) = live_client().await;
    let ledger = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(Notify::new());
    let agent = TemporalAgent::new(MockCompletionModel::from_turns([
        MockTurn::tool_call("write-1", "write", serde_json::json!({"key": "order"})),
        MockTurn::text("finished"),
    ]))
    .activity_timeout(Duration::from_secs(1))
    .activity_max_attempts(3)
    .tool_with_policy(
        LedgerWrite {
            ledger: Arc::clone(&ledger),
            hold_first_attempt: Arc::clone(&release),
            hold: true,
        },
        ToolPolicy::idempotent(),
    );
    let input = agent.input("write the order");
    let task_queue = format!("rig-temporal-idempotent-test-{}", uuid::Uuid::new_v4());
    let mut options = WorkerOptions::new(task_queue.clone()).build();
    agent.register(&mut options).unwrap();
    let mut worker = Worker::new(&runtime, client.clone(), options).unwrap();
    let shutdown = worker.shutdown_handle();
    let run = async move {
        let handle = client
            .start_workflow(
                TemporalAgentWorkflow::run,
                input,
                WorkflowStartOptions::new(
                    task_queue,
                    format!("rig-temporal-idempotent-test-{}", uuid::Uuid::new_v4()),
                )
                .build(),
            )
            .await
            .unwrap();
        let response = handle
            .get_result(WorkflowGetResultOptions::default())
            .await
            .unwrap();
        release.notify_waiters();
        release.notify_one();
        shutdown();
        response
    };
    let (worker_result, response) = tokio::join!(worker.run(), run);

    worker_result.unwrap();
    assert_eq!(response.output, "finished");
    assert_eq!(ledger.lock().unwrap().len(), 1, "retries reuse one key");
}
