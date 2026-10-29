#![cfg(feature = "temporal")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

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
    completion::{CompletionRequest, Message},
    message::{AssistantContent, UserContent},
    test_utils::{MockAddTool, MockCompletionModel, MockTurn},
    tool::{Tool, ToolContext, ToolExecutionError},
};
use rig_durable::{
    ApprovalDecision, Compaction, InMemoryGuardStore, InvocationContract, InvocationGuardStore,
    LogicalCallKey, ModelCompactor, ModelSummary, OutcomeSource, SubmissionMode, SubmissionState,
    SubmitInput, ToolDisposition, ToolInvocation, ToolOutcome, ToolPolicy,
    temporal::{
        TemporalAgent, TemporalAgentError, TemporalAgentInput, TemporalAgentSessionSnapshot,
        TemporalAgentSessionWorkflow, TemporalAgentStatus, TemporalAgentWorkflow,
        TemporalSubmission,
    },
};
use rig_memory::recent::LastMessages;
use serde::Deserialize;
use temporalio_client::{
    Client, ClientOptions, Connection, WorkflowExecuteUpdateOptions, WorkflowGetResultOptions,
    WorkflowHandle, WorkflowQueryOptions, WorkflowSignalOptions, WorkflowStartOptions,
    envconfig::LoadClientConfigProfileOptions, errors::WorkflowUpdateError,
};
use temporalio_common::HasWorkflowDefinition;
use temporalio_sdk::{
    Runtime, Worker, WorkerOptions,
    testing::{EphemeralExe, LocalWorkflowEnvironmentOptions, WorkflowEnvironment},
    workflow_replayer::{WorkflowReplayer, WorkflowReplayerOptions},
};
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

fn local_environment_options() -> LocalWorkflowEnvironmentOptions {
    let executable = std::env::split_paths(&std::env::var_os("PATH").unwrap())
        .map(|directory| directory.join("temporal"))
        .find(|path| path.is_file())
        .expect("Temporal CLI must be on PATH");
    LocalWorkflowEnvironmentOptions::builder()
        .server_executable(EphemeralExe::ExistingPath(
            executable.to_string_lossy().into_owned(),
        ))
        .build()
}

#[tokio::test]
#[ignore = "requires the Temporal CLI on PATH"]
async fn temporal_runner_sparse_batch_restart_and_malformed_outcomes() {
    use rig::message::{ToolCall, ToolFunction, ToolName};
    use temporalio_common::protos::temporal::api::history::v1::history_event::Attributes;

    let env = WorkflowEnvironment::start_local(local_environment_options())
        .await
        .unwrap();
    let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
    let client = env.client();
    let call = |name: &str, args| {
        AssistantContent::ToolCall(ToolCall::from_wire(
            "reused-provider-id",
            ToolFunction::new(ToolName::new(name).unwrap(), args),
        ))
    };
    let mut calls = vec![
        call("lookup", serde_json::json!("invalid")),
        call("blocking_lookup", serde_json::json!({"key":"slow"})),
    ];
    // Exercise the SDK join's large-batch path and settlements after a missing
    // earlier sibling. Provider IDs cannot identify these logical calls.
    calls.extend((2..37).map(|index| call("lookup", serde_json::json!({"key":index.to_string()}))));
    calls.push(call("lookup", serde_json::json!([1, 2])));
    let model = MockCompletionModel::from_turns([
        MockTurn::from_contents(calls),
        MockTurn::from_contents([call("lookup", serde_json::json!("invalid again"))]),
        MockTurn::text("complete"),
    ]);
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let invocations = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let make_agent = || {
        TemporalAgent::new(model.clone())
            .tool(BlockingLookup {
                started: started.clone(),
                release: release.clone(),
            })
            .tool(FlakyLookup {
                attempts: attempts.clone(),
                fail_until: 0,
                invocations: invocations.clone(),
            })
    };
    let queue = format!("runner-batch-{}", uuid::Uuid::new_v4());
    let agent = make_agent();
    let input = agent.input("run batch");
    let mut options = WorkerOptions::new(&queue).build();
    assert!(options.detect_nondeterministic_futures);
    agent.register(&mut options).unwrap();
    let mut worker = Worker::new(&runtime, client.clone(), options).unwrap();
    let stop = worker.shutdown_handle();
    let run = async {
        let handle = client
            .start_workflow(
                TemporalAgentWorkflow::run,
                input,
                WorkflowStartOptions::new(&queue, &queue).build(),
            )
            .await
            .unwrap();
        let completed = tokio::time::timeout(Duration::from_secs(45), async {
            started.notified().await;
            loop {
                let events = handle
                    .fetch_history(Default::default())
                    .into_events()
                    .await
                    .unwrap();
                let completed = events
                    .iter()
                    .filter(|event| {
                        matches!(
                            event.attributes,
                            Some(Attributes::ActivityTaskCompletedEventAttributes(_))
                        )
                    })
                    .count();
                if completed == 36 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        assert!(
            completed.is_ok(),
            "fast siblings did not commit: {}",
            String::from_utf8_lossy(
                &handle
                    .fetch_history(Default::default())
                    .to_json()
                    .await
                    .unwrap()
            )
        );
        assert_eq!(model.requests().len(), 1);
        assert_eq!(attempts.load(Ordering::SeqCst), 35);
        stop();
        release.notify_one();
        handle
    };
    let (result, handle) = tokio::join!(worker.run(), run);
    result.unwrap();
    let before = invocations.lock().unwrap().clone();
    let mut options = WorkerOptions::new(&queue).build();
    make_agent().register(&mut options).unwrap();
    let mut worker = Worker::new(&runtime, client.clone(), options).unwrap();
    let stop = worker.shutdown_handle();
    let finish = async {
        let response = tokio::time::timeout(
            Duration::from_secs(45),
            handle.get_result(Default::default()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.output(), "complete");
        let outcomes = handle
            .query(TemporalAgentWorkflow::tool_outcomes, (), Default::default())
            .await
            .unwrap();
        assert_eq!(outcomes.len(), 39);
        for (index, outcome) in outcomes.iter().enumerate() {
            assert_eq!(
                (outcome.prompt_index, outcome.turn, outcome.call_index),
                if index == 38 {
                    (0, 2, 0)
                } else {
                    (0, 1, index)
                }
            );
            assert_eq!(
                (outcome.disposition, outcome.source),
                if [0, 37, 38].contains(&index) {
                    (ToolDisposition::Error, OutcomeSource::Preresolved)
                } else {
                    (ToolDisposition::Success, OutcomeSource::Retained)
                }
            );
        }
        stop();
    };
    let (result, ()) = tokio::join!(worker.run(), finish);
    result.unwrap();
    assert_eq!(
        *invocations.lock().unwrap(),
        before,
        "settled siblings must not execute on restart"
    );
    let mut indices = before
        .iter()
        .map(|call| call.logical_key.as_ref().unwrap().call_index)
        .collect::<Vec<_>>();
    indices.sort_unstable();
    assert_eq!(indices, (2..37).collect::<Vec<_>>());
    let requests = model.requests();
    assert_eq!(requests.len(), 3);
    for request in requests.iter().skip(1) {
        rig::conversation::pairing::validate(&request.chat_history).unwrap();
    }
    let feedback = serde_json::to_string(&requests[1].chat_history).unwrap();
    for index in 2..37 {
        assert!(
            feedback.contains(&format!("{index}=available")),
            "{feedback}"
        );
    }
    assert!(feedback.contains("slow=released"));
    let history = handle
        .fetch_history(Default::default())
        .to_json()
        .await
        .unwrap();
    if let Ok(directory) = std::env::var("RIG_DURABLE_RECORD_DIR") {
        std::fs::write(format!("{directory}/temporal-runner-batch.json"), &history).unwrap();
    }
    let mut options = WorkflowReplayerOptions::new().namespace("default").build();
    assert!(options.detect_nondeterministic_futures);
    options
        .register_workflow::<TemporalAgentWorkflow>()
        .unwrap();
    WorkflowReplayer::new(options)
        .unwrap()
        .replay_workflow(temporalio_client::WorkflowHistory::from_json(&history).unwrap())
        .await
        .unwrap();
    env.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the Temporal CLI on PATH"]
async fn temporal_runner_cancellation_during_approval_and_tools() {
    let env = WorkflowEnvironment::start_local(local_environment_options())
        .await
        .unwrap();
    let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
    for route in [
        "approval",
        "tool",
        #[cfg(feature = "codemode")]
        "codemode",
    ] {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let turn = if route == "codemode" {
            MockTurn::tool_call(
                "script",
                "codemode",
                serde_json::json!({
                    "code":"return await tools.blocking_lookup({key: 'x'});"
                }),
            )
        } else {
            MockTurn::tool_call("blocked", "blocking_lookup", serde_json::json!({"key":"x"}))
        };
        let model = MockCompletionModel::from_turns([turn, MockTurn::text("must not run")]);
        let tool = BlockingLookup {
            started: started.clone(),
            release: release.clone(),
        };
        let agent = TemporalAgent::new(model.clone());
        let agent = if route == "approval" {
            agent.approval_tool(tool)
        } else {
            agent.tool(tool)
        };
        #[cfg(feature = "codemode")]
        let agent = if route == "codemode" {
            agent.code_mode(Default::default())
        } else {
            agent
        };
        let input = agent.input("cancel this run");
        let queue = format!("cancel-{}", uuid::Uuid::new_v4());
        let mut options = WorkerOptions::new(&queue).build();
        agent.register(&mut options).unwrap();
        let mut worker = Worker::new(&runtime, env.client().clone(), options).unwrap();
        let stop = worker.shutdown_handle();
        let run = async {
            let handle = env
                .client()
                .start_workflow(
                    TemporalAgentWorkflow::run,
                    input,
                    WorkflowStartOptions::new(&queue, &queue).build(),
                )
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(30), async {
                if route == "approval" {
                    loop {
                        if matches!(
                            handle
                                .query(TemporalAgentWorkflow::status, (), Default::default())
                                .await
                                .unwrap(),
                            TemporalAgentStatus::Approval { .. }
                        ) {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                } else {
                    started.notified().await;
                }
            })
            .await
            .unwrap();
            handle.cancel(Default::default()).await.unwrap();
            let result = tokio::time::timeout(
                Duration::from_secs(30),
                handle.get_result(Default::default()),
            )
            .await
            .unwrap();
            assert!(
                matches!(
                    result,
                    Err(temporalio_client::errors::WorkflowGetResultError::Cancelled { .. })
                ),
                "expected cancellation, got {result:?}"
            );
            assert_eq!(model.requests().len(), 1);
            let history = handle
                .fetch_history(Default::default())
                .to_json()
                .await
                .unwrap();
            stop();
            release.notify_one();
            history
        };
        let (result, history) = tokio::join!(worker.run(), run);
        result.unwrap();
        let mut options = WorkflowReplayerOptions::new().namespace("default").build();
        options
            .register_workflow::<TemporalAgentWorkflow>()
            .unwrap();
        WorkflowReplayer::new(options)
            .unwrap()
            .replay_workflow(temporalio_client::WorkflowHistory::from_json(&history).unwrap())
            .await
            .unwrap();
    }
    env.shutdown().await.unwrap();
}

#[cfg(feature = "codemode")]
#[test]
fn temporal_code_mode_snapshots_catalog_and_registers_independent_child() {
    let child = TemporalAgent::new(MockCompletionModel::from_turns([MockTurn::text("child")]))
        .preamble("child instructions");
    let agent = TemporalAgent::new(MockCompletionModel::from_turns([MockTurn::text("done")]))
        .code_mode(rig_codemode::Limits::default())
        .tool(MockAddTool)
        .sub_agent("research", &child, "child-queue")
        .unwrap();
    let input = agent.input("task");
    let json = serde_json::to_value(&input).unwrap();
    assert_eq!(
        json["config"]["code_mode"]["definitions"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(input.config.tools.len(), 2);
    let advertised = input.config.advertised_tools().unwrap();
    assert_eq!(advertised.len(), 1);
    assert_eq!(advertised[0].name, "codemode");
    let route = input.config.tools[1].child.as_ref().unwrap();
    assert_eq!(route.task_queue, "child-queue");
    assert_eq!(route.config.preamble.as_deref(), Some("child instructions"));
    let restored: TemporalAgentInput = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(serde_json::to_value(restored).unwrap(), json);
    let mut old = json;
    old["config"].as_object_mut().unwrap().remove("code_mode");
    old["config"]["tools"][1]
        .as_object_mut()
        .unwrap()
        .remove("child");
    let old: TemporalAgentInput = serde_json::from_value(old).unwrap();
    assert_eq!(old.config.advertised_tools().unwrap().len(), 2);
    assert!(old.config.tools[1].child.is_none());
    agent
        .register(&mut WorkerOptions::new("parent-queue").build())
        .unwrap();
    child
        .register(&mut WorkerOptions::new("child-queue").build())
        .unwrap();
}

#[cfg(feature = "codemode")]
#[test]
fn temporal_code_mode_rejects_approval_and_invalid_limits() {
    let make = || TemporalAgent::new(MockCompletionModel::from_turns([MockTurn::text("done")]));
    let mut options = WorkerOptions::new("invalid-code-mode").build();
    assert!(matches!(
        make()
            .approval_tool(MockAddTool)
            .code_mode(Default::default())
            .register(&mut options),
        Err(TemporalAgentError::InvalidConfiguration(_))
    ));
    let limits = rig_codemode::Limits {
        max_in_flight: 0,
        ..Default::default()
    };
    assert!(matches!(
        make()
            .tool(MockAddTool)
            .code_mode(limits)
            .register(&mut options),
        Err(TemporalAgentError::InvalidConfiguration(_))
    ));
    let child = make();
    assert!(make().sub_agent("codemode", &child, "child").is_err());
    let child = make().approval_tool(MockAddTool);
    assert!(make().sub_agent("child", &child, "child").is_err());
}

#[cfg(feature = "codemode")]
#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_code_mode_planning_failure_stops_the_coordinator() {
    let (runtime, client) = live_client().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let invocations = Arc::new(Mutex::new(Vec::new()));
    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call(
            "code",
            "codemode",
            serde_json::json!({"code": r#"
                await tools.lookup({key: "committed"});
                tools.lookup({key: "unawaited"});
                return "done";
            "#}),
        ),
        MockTurn::text("must not continue"),
    ]);
    let agent = TemporalAgent::new(model.clone())
        .code_mode(Default::default())
        .activity_max_attempts(1)
        .tool(FlakyLookup {
            attempts: attempts.clone(),
            fail_until: 0,
            invocations: invocations.clone(),
        });
    let input = agent.input("run script");
    let suffix = uuid::Uuid::new_v4();
    let queue = format!("rig-code-mode-failure-{suffix}");
    let mut options = WorkerOptions::new(queue.clone()).build();
    agent.register(&mut options).unwrap();
    let mut worker = Worker::new(&runtime, client.clone(), options).unwrap();
    let shutdown = worker.shutdown_handle();
    let run = async {
        let handle = client
            .start_workflow(
                TemporalAgentWorkflow::run,
                input,
                WorkflowStartOptions::new(queue, format!("rig-code-mode-failure-{suffix}")).build(),
            )
            .await
            .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            handle.get_result(WorkflowGetResultOptions::default()),
        )
        .await
        .unwrap();
        shutdown();
        result
    };
    let (worker_result, result) = tokio::join!(worker.run(), run);
    worker_result.unwrap();
    assert!(result.is_err(), "a planning failure must fail the workflow");
    let error = format!("{:?}", result.unwrap_err());
    assert!(error.contains("must await every new tool call"), "{error}");
    assert_eq!(model.requests().len(), 1);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(invocations.lock().unwrap().len(), 1);
}

#[cfg(feature = "codemode")]
#[derive(Clone)]
struct CodeModeProbe {
    invocations: Arc<Mutex<Vec<(String, ToolInvocation)>>>,
    followup: Arc<Notify>,
    loser_started: Arc<Notify>,
    loser_release: Arc<Notify>,
    loser_finished: Arc<AtomicUsize>,
}

#[cfg(feature = "codemode")]
impl Tool for CodeModeProbe {
    const NAME: &'static str = "probe";
    type Args = LookupArgs;
    type Output = String;
    type Error = Infallible;
    fn description(&self) -> String {
        "Return the key immediately".into()
    }
    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({"type":"object","properties":{"key":{"type":"string"}},"required":["key"]})
    }
    async fn call(
        &self,
        context: &mut ToolContext,
        args: LookupArgs,
    ) -> Result<String, Infallible> {
        self.invocations.lock().unwrap().push((
            args.key.clone(),
            context.require::<ToolInvocation>().unwrap().clone(),
        ));
        if args.key.starts_with("followup:") {
            self.followup.notify_one();
        }
        if args.key == "loser" {
            self.loser_started.notify_one();
            self.loser_release.notified().await;
            self.loser_finished.fetch_add(1, Ordering::SeqCst);
        }
        Ok(args.key)
    }
}

#[cfg(feature = "codemode")]
#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_code_mode_out_of_order_child_race_restart_and_replay() {
    use temporalio_sdk::workflow_replayer::{WorkflowReplayer, WorkflowReplayerOptions};
    let (runtime, client) = live_client().await;
    let namespace = client.options().namespace.clone();
    let suffix = uuid::Uuid::new_v4();
    let queue = format!("rig-code-mode-parent-{suffix}");
    let child_queue = format!("rig-code-mode-child-{suffix}");
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let followup = Arc::new(Notify::new());
    let loser_started = Arc::new(Notify::new());
    let loser_release = Arc::new(Notify::new());
    let loser_finished = Arc::new(AtomicUsize::new(0));
    let invocations = Arc::new(Mutex::new(Vec::new()));
    let child = TemporalAgent::new(MockCompletionModel::from_turns([
        MockTurn::tool_call("slow", "blocking_lookup", serde_json::json!({"key":"slow"})),
        MockTurn::text("slow"),
    ]))
    .tool(BlockingLookup {
        started: started.clone(),
        release: release.clone(),
    });
    let script = r#"
        const slow = tools.child({prompt: "slow"});
        const fast = tools.probe({key: "fast"});
        const winner = await Promise.race([slow, fast]);
        await tools.probe({key: "followup:" + winner});
        const ordered = await Promise.all([slow, fast]);
        text({winner, ordered});
        const lastWinner = await Promise.race([
            tools.probe({key: "loser"}), tools.probe({key: "last-fast"})
        ]);
        const refusal = await tools.disposition.raw({mode: "refused"});
        return {winner, ordered, lastWinner, refusal: refusal.error.kind};
    "#;
    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call("code", "codemode", serde_json::json!({"code":script})),
        MockTurn::text("finished"),
    ]);
    let make_parent = || {
        TemporalAgent::new(model.clone())
            .code_mode(Default::default())
            .tool(CodeModeProbe {
                invocations: invocations.clone(),
                followup: followup.clone(),
                loser_started: loser_started.clone(),
                loser_release: loser_release.clone(),
                loser_finished: loser_finished.clone(),
            })
            .tool(Disposition)
            .sub_agent("child", &child, child_queue.clone())
            .unwrap()
    };
    let parent = make_parent();
    let input = parent.input("run race");
    let mut options = WorkerOptions::new(queue.clone()).build();
    parent.register(&mut options).unwrap();
    let mut worker = Worker::new(&runtime, client.clone(), options).unwrap();
    let stop = worker.shutdown_handle();
    let mut child_options = WorkerOptions::new(child_queue.clone()).build();
    // Build the replacement before the independently registered child is consumed.
    let replacement = make_parent();
    child.register(&mut child_options).unwrap();
    let mut child_worker = Worker::new(&runtime, client.clone(), child_options).unwrap();
    let stop_child = child_worker.shutdown_handle();
    let run = async {
        let handle = client
            .start_workflow(
                TemporalAgentWorkflow::run,
                input,
                WorkflowStartOptions::new(queue.clone(), format!("rig-code-mode-{suffix}")).build(),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(30), async {
            started.notified().await;
            followup.notified().await;
        })
        .await
        .expect("fast-result follow-up must start while the slow child is blocked");
        assert_eq!(invocations.lock().unwrap()[1].0, "followup:fast");
        stop();
        handle
    };
    let parent_run = async {
        let (worker_result, handle) = tokio::join!(worker.run(), run);
        worker_result.unwrap();
        let before = invocations.lock().unwrap().clone();
        let mut options = WorkerOptions::new(queue.clone()).build();
        replacement.register(&mut options).unwrap();
        let mut worker = Worker::new(&runtime, client.clone(), options).unwrap();
        let stop = worker.shutdown_handle();
        let finish = async {
            release.notify_one();
            tokio::time::timeout(Duration::from_secs(30), loser_started.notified())
                .await
                .unwrap();
            assert_eq!(loser_finished.load(Ordering::SeqCst), 0);
            // This loser is not awaited after Promise.race. The scheduler must
            // retain it, drain it without cancellation, then run the final plan.
            tokio::time::timeout(Duration::from_secs(30), async {
                use temporalio_common::protos::temporal::api::history::v1::history_event::Attributes;
                loop {
                    let events = handle.fetch_history(Default::default()).into_events().await.unwrap();
                    let provisional_done = events.iter().any(|event| {
                        let Some(Attributes::ActivityTaskCompletedEventAttributes(activity)) = &event.attributes else { return false; };
                        let is_plan = events.iter().any(|scheduled| {
                            scheduled.event_id == activity.scheduled_event_id
                                && matches!(&scheduled.attributes, Some(Attributes::ActivityTaskScheduledEventAttributes(plan)) if plan.activity_type.as_ref().is_some_and(|kind| kind.name == "RigTemporalCodeModeStepG3V1"))
                        });
                        is_plan && activity.result.as_ref().is_some_and(|result| result.payloads.iter().any(|payload| {
                            serde_json::from_slice::<serde_json::Value>(&payload.data).is_ok_and(|value| {
                                value["Done"]["returned"]["lastWinner"] == "last-fast"
                                    && value["Done"]["calls"].as_array().is_some_and(|calls| calls.iter().any(|call| call["ordinal"] == 3 && call["delivery"] == "none"))
                            })
                        }))
                    });
                    if provisional_done { break; }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }).await.expect("provisional report must be recorded while race loser remains blocked");
            assert_eq!(loser_finished.load(Ordering::SeqCst), 0);
            loser_release.notify_one();
            let response = tokio::time::timeout(
                Duration::from_secs(30),
                handle.get_result(WorkflowGetResultOptions::default()),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(response.output(), "finished");
            assert_eq!(loser_finished.load(Ordering::SeqCst), 1);
            stop();
            stop_child();
        };
        let (worker_result, ()) = tokio::join!(worker.run(), finish);
        worker_result.unwrap();
        let after = invocations.lock().unwrap().clone();
        assert_eq!(
            before,
            after[..2],
            "restart must not rerun settled calls or change their identity"
        );
        assert_eq!(after.len(), 4);
        assert_eq!(after[0].1.logical_key.as_ref().unwrap().call_index, 1);
        assert_eq!(after[1].1.logical_key.as_ref().unwrap().call_index, 2);
        assert!(
            after[0]
                .1
                .logical_key
                .as_ref()
                .unwrap()
                .logical_execution_id
                .ends_with("/codemode/0/1/0")
        );
        let requests = model.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].tools.len(), 1);
        assert_eq!(requests[0].tools[0].name, "codemode");
        let feedback = serde_json::to_string(&requests[1].chat_history).unwrap();
        assert!(
            feedback.contains("fast") && feedback.contains("slow"),
            "{feedback}"
        );
        // Check structured report output rather than merely final model prose.
        let transcript = serde_json::to_value(&requests[1].chat_history).unwrap();
        fn find_order(value: &serde_json::Value) -> bool {
            match value {
                serde_json::Value::Object(map) => {
                    map.get("returned").is_some_and(|returned| {
                        returned["ordered"] == serde_json::json!(["slow", "fast"])
                            && returned["winner"] == "fast"
                            && returned["refusal"] == "permission_denied"
                    }) && map
                        .get("calls")
                        .and_then(serde_json::Value::as_array)
                        .is_some_and(|calls| {
                            calls
                                .iter()
                                .any(|call| call["ordinal"] == 3 && call["delivery"] == "discarded")
                        })
                        || map.values().any(find_order)
                }
                serde_json::Value::Array(items) => items.iter().any(find_order),
                serde_json::Value::String(text) => serde_json::from_str::<serde_json::Value>(text)
                    .is_ok_and(|value| find_order(&value)),
                _ => false,
            }
        }
        assert!(find_order(&transcript), "{transcript}");
        assert!(
            serde_json::to_string(&transcript)
                .unwrap()
                .contains("last-fast"),
            "{transcript}"
        );
        if let Ok(directory) = std::env::var("RIG_DURABLE_RECORD_DIR") {
            std::fs::write(
                format!("{directory}/temporal-codemode-race.json"),
                handle
                    .fetch_history(Default::default())
                    .to_json()
                    .await
                    .unwrap(),
            )
            .unwrap();
        }
        let mut replay_options = WorkflowReplayerOptions::new().namespace(namespace).build();
        replay_options
            .register_workflow::<TemporalAgentWorkflow>()
            .unwrap();
        WorkflowReplayer::new(replay_options)
            .unwrap()
            .replay_workflow(handle.fetch_history(Default::default()))
            .await
            .unwrap();
    };
    let ((), child_result) = tokio::join!(parent_run, child_worker.run());
    child_result.unwrap();
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
    assert_eq!(first.output(), "first answer");
    assert_eq!(second.output(), "second answer");
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
    assert_eq!(response.output(), "The answer is 42.");
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
        if let Ok(directory) = std::env::var("RIG_DURABLE_RECORD_DIR") {
            std::fs::write(
                format!("{directory}/temporal-retry.json"),
                handle
                    .fetch_history(Default::default())
                    .to_json()
                    .await
                    .unwrap(),
            )
            .unwrap();
        }
        shutdown();
        result
    };
    let (worker_result, response) = tokio::join!(worker.run(), run);

    worker_result.unwrap();
    assert_eq!(response.output(), "available");
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

        if let Ok(directory) = std::env::var("RIG_DURABLE_RECORD_DIR") {
            std::fs::write(
                format!("{directory}/temporal-approval.json"),
                approved
                    .fetch_history(Default::default())
                    .to_json()
                    .await
                    .unwrap(),
            )
            .unwrap();
        }
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
    assert_eq!(approved.output(), "approved");
    assert_eq!(denied.output(), "denied");
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
        if let Ok(directory) = std::env::var("RIG_DURABLE_RECORD_DIR") {
            std::fs::write(
                format!("{directory}/temporal-steering.json"),
                handle
                    .fetch_history(Default::default())
                    .to_json()
                    .await
                    .unwrap(),
            )
            .unwrap();
        }
        shutdown();
        (response, result)
    };
    let (worker_result, (response, result)) = tokio::join!(worker.run(), run);

    worker_result.unwrap();
    assert_eq!(response.output(), "steered answer");
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
fn temporal_input_requires_an_explicit_invocation_contract() {
    let agent = TemporalAgent::new(MockCompletionModel::from_turns([MockTurn::text("done")]))
        .tool(MockAddTool);
    let fresh = agent.input("hello");
    assert_eq!(fresh.config.contract, InvocationContract::Logical);

    let mut input = serde_json::to_value(&fresh).unwrap();
    input["config"]
        .as_object_mut()
        .unwrap()
        .remove("contract")
        .expect("new inputs carry the contract");
    assert!(
        serde_json::from_value::<TemporalAgentInput>(input)
            .unwrap_err()
            .to_string()
            .contains("missing field `contract`")
    );
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_guarded_write_is_not_repeated_after_an_uncertain_attempt() {
    let (runtime, client) = live_client().await;
    let namespace = client.options().namespace.clone();
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
        (handle.run_id().unwrap().to_owned(), response)
    };
    let (worker_result, (run_id, response)) = tokio::join!(worker.run(), run);

    worker_result.unwrap();
    assert_eq!(response.output(), "finished");
    assert_eq!(ledger.lock().unwrap().len(), 1, "one external effect");
    let key = LogicalCallKey {
        logical_execution_id: format!("temporal:{}:{namespace}:{run_id}", namespace.len()),
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
    assert_eq!(response.output(), "finished");
    assert_eq!(ledger.lock().unwrap().len(), 1, "retries reuse one key");
}

type SessionHandle =
    WorkflowHandle<Client, <TemporalAgentSessionWorkflow as HasWorkflowDefinition>::Run>;

/// Start a worker and a session workflow for `agent`, run `body` against the
/// session, then shut the worker down.
async fn with_session<T, Fut>(
    agent: TemporalAgent,
    prefix: &str,
    body: impl FnOnce(SessionHandle) -> Fut,
) -> T
where
    Fut: Future<Output = T>,
{
    let (runtime, client) = live_client().await;
    let input = agent.session_input(Vec::new());
    let task_queue = format!("{prefix}-{}", uuid::Uuid::new_v4());
    let mut options = WorkerOptions::new(task_queue.clone()).build();
    agent.register(&mut options).unwrap();
    let mut worker = Worker::new(&runtime, client.clone(), options).unwrap();
    let shutdown = worker.shutdown_handle();
    let run = async move {
        let handle = client
            .start_workflow(
                TemporalAgentSessionWorkflow::run,
                input,
                WorkflowStartOptions::new(task_queue, format!("{prefix}-{}", uuid::Uuid::new_v4()))
                    .build(),
            )
            .await
            .unwrap();
        let output = body(handle).await;
        shutdown();
        output
    };
    let (worker_result, output) = tokio::join!(worker.run(), run);
    worker_result.unwrap();
    output
}

async fn submit(
    handle: &SessionHandle,
    input: SubmitInput,
) -> Result<TemporalSubmission, WorkflowUpdateError> {
    handle
        .execute_update(
            TemporalAgentSessionWorkflow::submit,
            input,
            WorkflowExecuteUpdateOptions::default(),
        )
        .await
}

async fn wait_queued(handle: &SessionHandle, count: usize) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let snapshot = handle
                .query(
                    TemporalAgentSessionWorkflow::snapshot,
                    (),
                    Default::default(),
                )
                .await
                .unwrap();
            if snapshot.queued_submissions == count {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn full_audit_bytes_limit_admission_and_completed_prompts_after_compaction() {
    let model = MockCompletionModel::from_turns([
        MockTurn::text("x".repeat(500)),
        MockTurn::text("short summary"),
        MockTurn::text("y".repeat(500)),
    ]);
    let agent = TemporalAgent::new(model.clone())
        .activity_max_attempts(1)
        .session_history_max_bytes(900)
        .compaction(compaction(&model, 1));
    with_session(agent, "audit-limit", |handle| async move {
        assert_eq!(prompt(&handle, "one").await.len(), 500);
        let large = "z".repeat(400);
        assert!(
            handle
                .execute_update(
                    TemporalAgentSessionWorkflow::prompt,
                    large.clone().into(),
                    Default::default()
                )
                .await
                .is_err()
        );
        let error = submit(&handle, SubmitInput::new("too-large", large))
            .await
            .unwrap_err();
        assert!(update_failure_message(&error).contains("history exceeds"));
        let second = submit(&handle, SubmitInput::new("second", "two"))
            .await
            .unwrap();
        assert!(matches!(
            second.submission.state,
            SubmissionState::Failed { .. }
        ));
        assert!(handle.get_result(Default::default()).await.is_err());
        let snapshot = handle
            .query(
                TemporalAgentSessionWorkflow::snapshot,
                (),
                Default::default(),
            )
            .await
            .unwrap();
        assert_eq!(snapshot.history.len(), 2);
    })
    .await;
    assert_eq!(model.requests().len(), 3);
}

#[tokio::test]
#[ignore = "requires the Temporal CLI on PATH"]
async fn continuation_preserves_failed_compaction_and_execution_chain_identity() {
    use temporalio_common::protos::temporal::api::history::v1::history_event::Attributes;
    let mut environment_options = local_environment_options();
    environment_options.extra_args = vec![
        "--dynamic-config-value".into(),
        "limit.historyCount.suggestContinueAsNew=12".into(),
    ];
    let env = WorkflowEnvironment::start_local(environment_options)
        .await
        .unwrap();
    let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
    let client = env.client();
    let invocations = Arc::new(Mutex::new(Vec::new()));
    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call("same", "lookup", serde_json::json!({"key":"x"})),
        MockTurn::text("one"),
        MockTurn::tool_call("same", "lookup", serde_json::json!({"key":"x"})),
        MockTurn::text("two"),
    ]);
    let summaries = MockCompletionModel::from_turns([
        MockTurn::error("summary down"),
        MockTurn::error("summary down again"),
    ]);
    let agent = TemporalAgent::new(model)
        .activity_max_attempts(1)
        .compaction(compaction(&summaries, 1))
        .tool(FlakyLookup {
            attempts: Arc::new(AtomicUsize::new(0)),
            fail_until: 0,
            invocations: invocations.clone(),
        });
    let input = agent.session_input(Vec::new());
    let queue = format!("continue-{}", uuid::Uuid::new_v4());
    let id = queue.clone();
    let mut options = WorkerOptions::new(queue.clone()).build();
    agent.register(&mut options).unwrap();
    let mut worker = Worker::new(&runtime, client.clone(), options).unwrap();
    let shutdown = worker.shutdown_handle();
    let run = async {
        let initial = client
            .start_workflow(
                TemporalAgentSessionWorkflow::run,
                input,
                WorkflowStartOptions::new(queue, &id).build(),
            )
            .await
            .unwrap();
        let first_id = initial.run_id().unwrap().to_owned();
        let handle: SessionHandle = client.get_workflow_handle(&id);
        assert_eq!(
            submit(&handle, SubmitInput::new("one", "one"))
                .await
                .unwrap()
                .submission
                .state,
            SubmissionState::Answered
        );
        tokio::time::timeout(Duration::from_secs(20), async {
            while handle.describe(Default::default()).await.unwrap().run_id() == first_id {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let snapshot = handle
            .query(
                TemporalAgentSessionWorkflow::snapshot,
                (),
                Default::default(),
            )
            .await
            .unwrap();
        let prior = initial
            .query(
                TemporalAgentSessionWorkflow::snapshot,
                (),
                Default::default(),
            )
            .await
            .unwrap();
        assert!(prior.compaction_error.is_some());
        assert_eq!(snapshot.compaction_error, prior.compaction_error);
        assert_eq!(snapshot.history.len(), 4);
        assert_eq!(
            summaries.requests().len(),
            1,
            "unchanged transcript must not retry compaction"
        );
        let ledger = handle
            .query(TemporalAgentSessionWorkflow::ledger, (), Default::default())
            .await
            .unwrap();
        assert!(ledger.logical_session_id.ends_with(&first_id));
        assert_eq!(
            submit(&handle, SubmitInput::new("two", "two"))
                .await
                .unwrap()
                .submission
                .state,
            SubmissionState::Answered
        );
        close(&handle).await;
        shutdown();
        initial
    };
    let (result, mut history_handle) = tokio::join!(worker.run(), run);
    result.unwrap();
    let mut windows = 0;
    loop {
        let events = history_handle
            .fetch_history(Default::default())
            .into_events()
            .await
            .unwrap();
        let next = events.iter().find_map(|event| match &event.attributes {
            Some(Attributes::WorkflowExecutionContinuedAsNewEventAttributes(attributes)) => {
                Some(attributes.new_execution_run_id.clone())
            }
            _ => None,
        });
        let mut options = WorkflowReplayerOptions::new().namespace("default").build();
        options
            .register_workflow::<TemporalAgentSessionWorkflow>()
            .unwrap();
        WorkflowReplayer::new(options)
            .unwrap()
            .replay_workflow(history_handle.fetch_history(Default::default()))
            .await
            .unwrap();
        windows += 1;
        let Some(next) = next else {
            break;
        };
        let mut info = history_handle.info().clone();
        info.run_id = Some(next);
        history_handle = WorkflowHandle::new(client.clone(), info);
    }
    assert!(windows >= 2, "replay must cover continue-as-new");
    env.shutdown().await.unwrap();
    let calls = invocations.lock().unwrap();
    assert_eq!(calls.len(), 2);
    let first = calls[0].logical_key.as_ref().unwrap();
    let second = calls[1].logical_key.as_ref().unwrap();
    assert_eq!(first.logical_execution_id, second.logical_execution_id);
    assert_ne!(first.submission_id, second.submission_id);
    assert_ne!(
        calls[0].attempt.as_ref().unwrap().backend_execution_id,
        calls[1].attempt.as_ref().unwrap().backend_execution_id
    );
    assert_eq!(summaries.requests().len(), 2);
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn failed_prompt_cancels_an_already_admitted_follow_up() {
    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call("block", "blocking_lookup", serde_json::json!({"key":"x"})),
        MockTurn::error("active prompt failed"),
        MockTurn::text("must not run"),
    ]);
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let agent = TemporalAgent::new(model.clone())
        .activity_max_attempts(1)
        .tool(BlockingLookup {
            started: started.clone(),
            release: release.clone(),
        });
    with_session(agent, "failed-prompt-queued", |handle| async move {
        let active = handle.execute_update(
            TemporalAgentSessionWorkflow::prompt,
            "first".into(),
            Default::default(),
        );
        tokio::pin!(active);
        tokio::select! {
            result = &mut active => panic!("prompt completed before release: {result:?}"),
            _ = started.notified() => {}
        }
        let queued = submit(&handle, SubmitInput::new("follow-up", "must not run"));
        tokio::pin!(queued);
        tokio::select! {
            result = &mut queued => panic!("queued prompt completed before release: {result:?}"),
            _ = wait_queued(&handle, 1) => {}
        }
        handle
            .signal(
                TemporalAgentSessionWorkflow::steer,
                "must not steer".into(),
                Default::default(),
            )
            .await
            .unwrap();
        release.notify_one();
        assert!(active.await.is_err());
        assert_eq!(
            queued.await.unwrap().submission.state,
            SubmissionState::Cancelled
        );
        let result = handle.get_result(Default::default()).await.unwrap();
        assert!(result.history.is_empty());
    })
    .await;
    assert_eq!(model.requests().len(), 2);
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn queued_prompts_and_steering_compact_before_each_next_prompt() {
    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call("block", "blocking_lookup", serde_json::json!({"key":"x"})),
        MockTurn::text("answer 1"),
        MockTurn::text("S1"),
        MockTurn::text("answer 2"),
        MockTurn::text("S2"),
        MockTurn::text("answer 3"),
        MockTurn::text("S3"),
        MockTurn::text("answer 4"),
        MockTurn::text("S4"),
    ]);
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let agent = TemporalAgent::new(model.clone())
        .activity_max_attempts(1)
        .compaction(compaction(&model, 1))
        .tool(BlockingLookup {
            started: started.clone(),
            release: release.clone(),
        });
    with_session(agent, "queued-compaction", |handle| async move {
        let active = handle.execute_update(
            TemporalAgentSessionWorkflow::prompt,
            "first".into(),
            Default::default(),
        );
        tokio::pin!(active);
        tokio::select! {
            result = &mut active => panic!("prompt completed before release: {result:?}"),
            _ = started.notified() => {}
        }
        let pending = futures::future::join_all([
            submit(&handle, SubmitInput::new("q1", "queued 1")),
            submit(&handle, SubmitInput::new("q2", "queued 2")),
        ]);
        tokio::pin!(pending);
        tokio::select! {
            result = &mut pending => panic!("queued prompts completed before release: {result:?}"),
            _ = wait_queued(&handle, 2) => {}
        }
        handle
            .signal(
                TemporalAgentSessionWorkflow::steer,
                "steering".into(),
                Default::default(),
            )
            .await
            .unwrap();
        release.notify_one();
        assert_eq!(active.await.unwrap().output(), "answer 2");
        for result in pending.await {
            assert_eq!(result.unwrap().submission.state, SubmissionState::Answered);
        }
        close(&handle).await
    })
    .await;
    let requests = model.requests();
    assert_eq!(requests.len(), 9);
    for (index, summary) in [(3, "S1"), (5, "S2"), (7, "S3")] {
        rig::conversation::pairing::validate(&requests[index].chat_history).unwrap();
        assert_eq!(
            user_texts(&requests[index].chat_history)[0],
            format!("{SUMMARY_HEADER}{summary}")
        );
    }
    assert_eq!(
        user_texts(&requests[3].chat_history).last().unwrap(),
        "steering"
    );
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn a_reused_workflow_id_has_a_new_guard_identity() {
    let (runtime, client) = live_client().await;
    let effects = Arc::new(Mutex::new(Vec::new()));
    let agent = TemporalAgent::new(MockCompletionModel::from_turns([
        MockTurn::tool_call("same", "write", serde_json::json!({"key":"x"})),
        MockTurn::text("one"),
        MockTurn::tool_call("same", "write", serde_json::json!({"key":"x"})),
        MockTurn::text("two"),
    ]))
    .invocation_guard(Arc::new(InMemoryGuardStore::new()))
    .tool_with_policy(
        LedgerWrite {
            ledger: effects.clone(),
            hold_first_attempt: Arc::new(Notify::new()),
            hold: false,
        },
        ToolPolicy::interrupt_on_uncertain(),
    );
    let input = agent.input("write");
    let id = format!("reused-{}", uuid::Uuid::new_v4());
    let queue = format!("reused-{}", uuid::Uuid::new_v4());
    let mut options = WorkerOptions::new(queue.clone()).build();
    agent.register(&mut options).unwrap();
    let mut worker = Worker::new(&runtime, client.clone(), options).unwrap();
    let shutdown = worker.shutdown_handle();
    let run = async {
        for answer in ["one", "two"] {
            let handle = client
                .start_workflow(
                    TemporalAgentWorkflow::run,
                    input.clone(),
                    WorkflowStartOptions::new(&queue, &id).build(),
                )
                .await
                .unwrap();
            assert_eq!(
                handle
                    .get_result(Default::default())
                    .await
                    .unwrap()
                    .output(),
                answer
            );
        }
        shutdown();
    };
    let (result, ()) = tokio::join!(worker.run(), run);
    result.unwrap();
    assert_eq!(effects.lock().unwrap().len(), 2);
}

async fn prompt(handle: &SessionHandle, text: &str) -> String {
    handle
        .execute_update(
            TemporalAgentSessionWorkflow::prompt,
            Message::from(text),
            WorkflowExecuteUpdateOptions::default(),
        )
        .await
        .unwrap()
        .output()
}

async fn close(handle: &SessionHandle) -> rig_durable::temporal::TemporalAgentSessionResult {
    handle
        .signal(
            TemporalAgentSessionWorkflow::close,
            (),
            WorkflowSignalOptions::default(),
        )
        .await
        .unwrap();
    handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .unwrap()
}

fn update_failure_message(error: &WorkflowUpdateError) -> String {
    match error {
        WorkflowUpdateError::Failed(failure) => failure.message.clone(),
        other => other.to_string(),
    }
}

fn user_texts(messages: &[impl std::borrow::Borrow<Message>]) -> Vec<String> {
    messages
        .iter()
        .flat_map(|message| match message.borrow() {
            Message::User { content } => content
                .iter()
                .filter_map(|item| match item {
                    UserContent::Text(text) => Some(text.text.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        })
        .collect()
}

fn has_tool_call(messages: &[impl std::borrow::Borrow<Message>]) -> bool {
    messages.iter().any(|message| {
        matches!(
            message.borrow(),
            Message::Assistant(assistant)
                if assistant.content.iter().any(|item| matches!(item, AssistantContent::ToolCall(_)))
        )
    })
}

const SUMMARY_PROMPT: &str = "Summarize the conversation above for an assistant that will \
continue it. Reply with the summary only.";

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_duplicate_submission_returns_receipt_and_altered_duplicate_is_rejected() {
    let model = MockCompletionModel::from_turns([MockTurn::text("only answer")]);
    let agent = TemporalAgent::new(model.clone()).activity_max_attempts(1);
    let result = with_session(agent, "rig-temporal-dedup-test", |handle| async move {
        let first = submit(&handle, SubmitInput::new("req-1", "hello"))
            .await
            .unwrap();
        assert_eq!(first.submission.prompt_index, 0);
        assert_eq!(first.submission.state, SubmissionState::Answered);
        let response = first.response.expect("retained result");
        assert_eq!(response.output(), "only answer");

        // The same request receives the same receipt and result without a
        // second model call.
        let again = submit(&handle, SubmitInput::new("req-1", "hello"))
            .await
            .unwrap();
        assert_eq!(again.submission, first.submission);
        assert_eq!(again.response.unwrap().output(), "only answer");

        let altered = submit(&handle, SubmitInput::new("req-1", "changed"))
            .await
            .unwrap_err();
        let message = update_failure_message(&altered);
        assert!(message.starts_with("conflict:"), "{message}");

        let busy_mode = submit(
            &handle,
            SubmitInput::new("req-1", "hello").mode(SubmissionMode::RejectIfBusy),
        )
        .await
        .unwrap_err();
        assert!(update_failure_message(&busy_mode).starts_with("conflict:"));

        let receipt = handle
            .query(
                TemporalAgentSessionWorkflow::receipt,
                "req-1".to_string(),
                WorkflowQueryOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(receipt, Some(first.submission));
        close(&handle).await
    })
    .await;
    assert_eq!(model.requests().len(), 1);
    assert_eq!(result.history.len(), 2);
    let ledger = result.ledger.unwrap();
    assert_eq!(ledger.receipts.len(), 1);
    assert_eq!(ledger.next_prompt_index, 1);
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_follow_up_during_a_tool_round_runs_after_the_answer_and_reject_if_busy_is_rejected()
 {
    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call(
            "blocking-call",
            "blocking_lookup",
            serde_json::json!({"key": "service"}),
        ),
        MockTurn::text("first answer"),
        MockTurn::text("second answer"),
        MockTurn::text("third answer"),
    ]);
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let agent = TemporalAgent::new(model.clone())
        .activity_max_attempts(1)
        .tool(BlockingLookup {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        });
    let result = with_session(agent, "rig-temporal-follow-up-test", |handle| async move {
        let first = submit(&handle, SubmitInput::new("req-1", "first"));
        tokio::pin!(first);
        tokio::select! {
            result = &mut first => panic!("first submission completed before tool release: {result:?}"),
            result = tokio::time::timeout(Duration::from_secs(20), started.notified()) => {
                result.unwrap();
            }
        }
        let busy = submit(
            &handle,
            SubmitInput::new("req-3", "third").mode(SubmissionMode::RejectIfBusy),
        )
        .await
        .unwrap_err();
        assert!(
            update_failure_message(&busy).starts_with("busy:"),
            "{}",
            update_failure_message(&busy)
        );
        let follow_up = submit(&handle, SubmitInput::new("req-2", "second"));
        tokio::pin!(follow_up);
        tokio::select! {
            result = &mut follow_up => panic!("follow-up completed before tool release: {result:?}"),
            () = tokio::time::sleep(Duration::from_millis(300)) => {}
        }
        let snapshot = handle
            .query(
                TemporalAgentSessionWorkflow::snapshot,
                (),
                WorkflowQueryOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(snapshot.queued_submissions, 1);
        assert_eq!(snapshot.ledger_receipts, 2);
        assert!(snapshot.busy);

        release.notify_one();
        let first = first.await.unwrap();
        assert_eq!(first.submission.prompt_index, 0);
        let first_response = first.response.unwrap();
        assert_eq!(first_response.output(), "first answer");
        assert_eq!(first_response.tool_outcomes.len(), 1);
        let follow_up = follow_up.await.unwrap();
        assert_eq!(follow_up.submission.prompt_index, 1);
        let second_response = follow_up.response.unwrap();
        assert_eq!(second_response.output(), "second answer");
        assert!(second_response.tool_outcomes.is_empty());

        // The rejected request is admitted once the session is idle.
        let third = submit(
            &handle,
            SubmitInput::new("req-3", "third").mode(SubmissionMode::RejectIfBusy),
        )
        .await
        .unwrap();
        assert_eq!(third.submission.prompt_index, 2);
        assert_eq!(third.response.unwrap().output(), "third answer");
        close(&handle).await
    })
    .await;

    let requests = model.requests();
    assert_eq!(requests.len(), 4);
    // The follow-up did not enter the first prompt's tool round.
    assert_eq!(user_texts(&requests[1].chat_history), ["first"]);
    assert_eq!(requests[1].chat_history.len(), 3);
    // It ran after the answer, on top of the whole first exchange.
    assert_eq!(user_texts(&requests[2].chat_history), ["first", "second"]);
    assert_eq!(requests[2].chat_history.len(), 5);
    assert_eq!(result.history.len(), 8);
    let ledger = result.ledger.unwrap();
    assert_eq!(
        ledger.receipts.values().map(|r| r.prompt_index).max(),
        Some(2)
    );
    assert!(
        ledger
            .receipts
            .values()
            .all(|r| r.state == SubmissionState::Answered)
    );
}

fn compaction(model: &MockCompletionModel, keep: usize) -> Compaction {
    Compaction::new(LastMessages(keep), ModelCompactor::new(model.clone()))
}

const SUMMARY_HEADER: &str = "Summary of the earlier conversation:\n";
const CARRY_OVER_HEADER: &str = "Summary of the conversation before this window:\n";

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_compaction_keeps_the_request_canonical_and_the_transcript_complete() {
    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call("call-1", "add", serde_json::json!({"x": 1, "y": 2})),
        MockTurn::text("answer one"),
        MockTurn::text("SUMMARY A"),
        MockTurn::text("answer two"),
        MockTurn::text("SUMMARY B"),
        MockTurn::text("answer three"),
        MockTurn::text("SUMMARY C"),
        MockTurn::text("answer four"),
        MockTurn::text("SUMMARY D"),
    ]);
    let agent = TemporalAgent::new(model.clone())
        .activity_max_attempts(1)
        .tool(MockAddTool)
        .compaction(compaction(&model, 2).version("test-1"));
    let result = with_session(agent, "rig-temporal-compaction-test", |handle| async move {
        assert_eq!(prompt(&handle, "one").await, "answer one");
        // Each prompt waits for the compaction round the previous one
        // scheduled.
        assert_eq!(prompt(&handle, "two").await, "answer two");
        assert_eq!(prompt(&handle, "three").await, "answer three");
        assert_eq!(prompt(&handle, "four").await, "answer four");
        close(&handle).await
    })
    .await;

    let requests: Vec<CompletionRequest> = model.requests();
    assert_eq!(requests.len(), 9);

    // Prompt one left 4 transcript messages. Keeping 2 would start the
    // window at the tool result, so the policy demoted the whole exchange.
    let summary_a = &requests[2];
    assert!(matches!(
        summary_a.chat_history.first().map(AsRef::as_ref),
        Some(Message::System { .. })
    ));
    assert_eq!(summary_a.chat_history.len(), 4);
    // Rig adapts unadvertised tool exchanges into text for summarization.
    assert!(
        serde_json::to_string(&summary_a.chat_history)
            .unwrap()
            .contains("called tool add")
    );
    assert_eq!(
        user_texts(&summary_a.chat_history[1..]),
        ["one", "[tool add result] 3", SUMMARY_PROMPT]
    );

    let second = &requests[3];
    rig::conversation::pairing::validate(&second.chat_history).unwrap();
    assert_eq!(second.chat_history.len(), 3);
    assert_eq!(
        user_texts(&second.chat_history),
        [format!("{SUMMARY_HEADER}SUMMARY A"), "two".into()]
    );
    assert!(!has_tool_call(&second.chat_history));

    let summary_b = &requests[4];
    assert_eq!(summary_b.chat_history.len(), 4);
    assert_eq!(
        user_texts(&summary_b.chat_history[1..]),
        [
            format!("{CARRY_OVER_HEADER}SUMMARY A"),
            SUMMARY_PROMPT.into()
        ]
    );

    let third = &requests[5];
    rig::conversation::pairing::validate(&third.chat_history).unwrap();
    assert_eq!(third.chat_history.len(), 3);
    assert_eq!(
        user_texts(&third.chat_history),
        [
            format!("{SUMMARY_HEADER}SUMMARY B"),
            "two".into(),
            "three".into()
        ]
    );

    let summary_c = &requests[6];
    assert_eq!(
        user_texts(&summary_c.chat_history[1..]),
        [
            format!("{CARRY_OVER_HEADER}SUMMARY B"),
            "two".into(),
            SUMMARY_PROMPT.into()
        ]
    );

    let fourth = &requests[7];
    rig::conversation::pairing::validate(&fourth.chat_history).unwrap();
    assert_eq!(
        user_texts(&fourth.chat_history),
        [
            format!("{SUMMARY_HEADER}SUMMARY C"),
            "three".into(),
            "four".into()
        ]
    );

    assert_eq!(result.history.len(), 10);
    assert_eq!(user_texts(&result.history), ["one", "two", "three", "four"]);
    assert!(has_tool_call(&result.history));
    let record = result.compaction.unwrap();
    assert_eq!(record.cutoff, 8);
    assert_eq!(record.input_messages, 2);
    assert_eq!(record.policy_version, "test-1");
    let summary: ModelSummary = serde_json::from_value(record.artifact.value).unwrap();
    assert_eq!(summary.text, "SUMMARY D");
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_failed_summary_leaves_the_context_in_place_and_the_session_open() {
    let model = MockCompletionModel::from_turns([
        MockTurn::text("answer one"),
        MockTurn::error("summarizer down"),
        MockTurn::text("answer two"),
        MockTurn::text("SUMMARY"),
        MockTurn::text("answer three"),
    ]);
    let agent = TemporalAgent::new(model.clone())
        .activity_max_attempts(1)
        .compaction(compaction(&model, 1));
    let result = with_session(
        agent,
        "rig-temporal-compaction-failure-test",
        |handle| async move {
            assert_eq!(prompt(&handle, "one").await, "answer one");
            assert_eq!(prompt(&handle, "two").await, "answer two");
            let snapshot = handle
                .query(
                    TemporalAgentSessionWorkflow::snapshot,
                    (),
                    WorkflowQueryOptions::default(),
                )
                .await
                .unwrap();
            assert!(!snapshot.closed);
            assert_eq!(prompt(&handle, "three").await, "answer three");
            close(&handle).await
        },
    )
    .await;

    let requests = model.requests();
    // The round after the third prompt had no scripted turn and failed too.
    assert_eq!(requests.len(), 6);
    assert_eq!(user_texts(&requests[2].chat_history), ["one", "two"]);
    assert_eq!(
        user_texts(&requests[3].chat_history[1..]),
        ["one", "two", SUMMARY_PROMPT]
    );
    assert_eq!(
        user_texts(&requests[4].chat_history),
        [format!("{SUMMARY_HEADER}SUMMARY"), "three".into()]
    );
    assert_eq!(result.history.len(), 6);
    assert_eq!(result.compaction.unwrap().cutoff, 3);
}

struct Disposition;

#[derive(Deserialize)]
struct DispositionArgs {
    mode: String,
}

impl Tool for Disposition {
    const NAME: &'static str = "disposition";
    type Args = DispositionArgs;
    type Output = String;
    type Error = ToolExecutionError;

    fn description(&self) -> String {
        "Return a chosen disposition".into()
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({"type":"object","properties":{"mode":{"type":"string"}}})
    }

    async fn call(
        &self,
        _: &mut ToolContext,
        args: DispositionArgs,
    ) -> Result<Self::Output, Self::Error> {
        let text = "same words";
        match args.mode.as_str() {
            "success" => Ok(text.into()),
            "error" => Err(ToolExecutionError::other(text)),
            "refused" => Err(ToolExecutionError::refused(text)),
            other => Err(ToolExecutionError::invalid_args(other)),
        }
    }
}

fn disposition_agent() -> TemporalAgent {
    TemporalAgent::new(MockCompletionModel::from_turns([
        MockTurn::tool_call(
            "c-success",
            "disposition",
            serde_json::json!({"mode": "success"}),
        ),
        MockTurn::tool_call(
            "c-error",
            "disposition",
            serde_json::json!({"mode": "error"}),
        ),
        MockTurn::tool_call(
            "c-refused",
            "disposition",
            serde_json::json!({"mode": "refused"}),
        ),
        MockTurn::text("done"),
    ]))
    .invocation_contract(InvocationContract::Logical)
    .activity_max_attempts(1)
    .max_turns(4)
    .tool_with_policy(Disposition, ToolPolicy::read_only())
}

fn assert_dispositions(outcomes: &[ToolOutcome]) {
    let summary: Vec<_> = outcomes
        .iter()
        .map(|outcome| {
            (
                outcome.turn,
                outcome.tool_call_id.as_str(),
                outcome.disposition,
                outcome.source,
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            (
                1,
                "c-success",
                ToolDisposition::Success,
                OutcomeSource::Retained
            ),
            (
                2,
                "c-error",
                ToolDisposition::Error,
                OutcomeSource::Retained
            ),
            (
                3,
                "c-refused",
                ToolDisposition::Refused,
                OutcomeSource::Retained
            ),
        ]
    );
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_single_run_exposes_ordered_dispositions() {
    let (runtime, client) = live_client().await;
    let agent = disposition_agent();
    let input = agent.input("go");
    let task_queue = format!("rig-temporal-disposition-test-{}", uuid::Uuid::new_v4());
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
                    format!("rig-temporal-disposition-test-{}", uuid::Uuid::new_v4()),
                )
                .build(),
            )
            .await
            .unwrap();
        // The workflow result stays Rig's PromptResponse.
        let response = handle
            .get_result(WorkflowGetResultOptions::default())
            .await
            .unwrap();
        let outcomes = handle
            .query(
                TemporalAgentWorkflow::tool_outcomes,
                (),
                WorkflowQueryOptions::default(),
            )
            .await
            .unwrap();
        shutdown();
        (response, outcomes)
    };
    let (worker_result, (response, outcomes)) = tokio::join!(worker.run(), run);
    worker_result.unwrap();
    assert_eq!(response.output(), "done");
    assert_dispositions(&outcomes);
    assert!(outcomes.iter().all(|o| o.prompt_index == 0));
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_session_results_expose_ordered_dispositions() {
    with_session(
        disposition_agent(),
        "rig-temporal-session-disposition-test",
        |handle| async move {
            let submitted = submit(&handle, SubmitInput::new("req-1", "go"))
                .await
                .unwrap();
            let response = submitted.response.unwrap();
            assert_eq!(response.output(), "done");
            assert_dispositions(&response.tool_outcomes);
            let retained = handle
                .query(
                    TemporalAgentSessionWorkflow::result,
                    submitted.submission.submission_id.clone(),
                    WorkflowQueryOptions::default(),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(retained.tool_outcomes.len(), 3);
            close(&handle).await
        },
    )
    .await;
}

#[tokio::test]
#[ignore = "requires a live Temporal server configured with TEMPORAL_* variables"]
async fn temporal_failed_submission_closes_the_session() {
    // The model has one answer; the second prompt finds no turn and fails.
    let agent = TemporalAgent::new(MockCompletionModel::from_turns([MockTurn::text("first")]))
        .activity_max_attempts(1);
    let (runtime, client) = live_client().await;
    let input = agent.session_input(Vec::new());
    let task_queue = format!("rig-temporal-failing-test-{}", uuid::Uuid::new_v4());
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
                    format!("rig-temporal-failing-test-{}", uuid::Uuid::new_v4()),
                )
                .build(),
            )
            .await
            .unwrap();
        assert_eq!(prompt(&handle, "one").await, "first");
        let failed = submit(&handle, SubmitInput::new("req-2", "two"))
            .await
            .unwrap();
        assert!(
            matches!(failed.submission.state, SubmissionState::Failed { .. }),
            "{:?}",
            failed.submission
        );
        assert!(failed.response.is_none());
        let result = handle.get_result(WorkflowGetResultOptions::default()).await;
        assert!(result.is_err());
        let closed = submit(&handle, SubmitInput::new("req-3", "three")).await;
        assert!(closed.is_err());
        shutdown();
    };
    let (worker_result, ()) = tokio::join!(worker.run(), run);
    worker_result.unwrap();
}
