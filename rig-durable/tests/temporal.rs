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
    ApprovalDecision, ToolInvocation,
    temporal::{
        TemporalAgent, TemporalAgentSessionSnapshot, TemporalAgentSessionWorkflow,
        TemporalAgentStatus, TemporalAgentWorkflow,
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
    let agent = TemporalAgent::new(MockCompletionModel::new([MockTurn::text("done")]))
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
    let agent = TemporalAgent::new(MockCompletionModel::new([
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
    let agent = TemporalAgent::new(MockCompletionModel::new([MockTurn::text("done")]))
        .approval_tool(MockAddTool);

    let input = agent.input("add");
    assert!(input.config.tools[0].requires_approval);
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_server_executes_model_and_tool_activities() {
    let (runtime, client) = live_client().await;
    let agent = TemporalAgent::new(MockCompletionModel::new([
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
    let agent = TemporalAgent::new(MockCompletionModel::new([
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
    assert_eq!(invocations[0], invocations[1]);
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_fails_after_tool_retries_are_exhausted() {
    let (runtime, client) = live_client().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let agent = TemporalAgent::new(MockCompletionModel::new([MockTurn::tool_call(
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
    let agent = TemporalAgent::new(MockCompletionModel::new([
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
    let agent = TemporalAgent::new(MockCompletionModel::new([MockTurn::text("steered answer")]));
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
    let agent = TemporalAgent::new(MockCompletionModel::new([
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
    let agent = TemporalAgent::new(MockCompletionModel::new([MockTurn::text("unused")]))
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
