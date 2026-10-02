//! Failure-boundary tests for tool replay safety on the Duroxide backend.
//!
//! A fake external service keeps its own effect ledger. The worker stops
//! after the service accepted an effect but before the activity recorded its
//! completion, so another worker receives the same attempt.

#![cfg(feature = "duroxide")]

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use duroxide::{RetryPolicy, providers::sqlite::SqliteProvider, runtime::RuntimeOptions};
use futures::future::BoxFuture;
use rig::{
    completion::Message,
    message::{ToolResultContent, UserContent},
    test_utils::{MockCompletionModel, MockTurn},
    tool::{Tool, ToolContext, ToolExecutionError, ToolSet},
};
use rig_durable::{
    AgentOrchestrator, AgentOrchestratorError, ApprovalRequest, CheckpointConfig, CheckpointPolicy,
    DurableAgent, DurableToolResult, InMemoryGuardStore, InterruptionReason, InvocationContract,
    InvocationGuardStore, LogicalCallKey, ToolActivityInput, ToolDisposition, ToolExecutor,
    ToolInvocation, ToolOptions, ToolPolicy,
    guard::{ClaimOutcome, ClaimRecord, ClaimRequest, GuardError, SettleOutcome},
};
use serde::Deserialize;
use tokio::sync::Notify;

const TOOL: &str = "effect";

#[derive(Deserialize)]
struct EffectArgs {
    #[serde(default)]
    value: String,
}

#[derive(Debug, thiserror::Error)]
enum ExternalError {
    #[error("connection reset")]
    Network,
    #[error("quota exhausted")]
    Quota,
}

/// Fake external service. Each accepted effect records the invocation that
/// produced it.
#[derive(Clone, Default)]
struct Ledger {
    effects: Arc<Mutex<Vec<ToolInvocation>>>,
}

impl Ledger {
    fn effects(&self) -> Vec<ToolInvocation> {
        self.effects.lock().unwrap().clone()
    }

    /// Record an effect. With `idempotent`, a repeated logical key is not a
    /// new effect.
    fn accept(&self, invocation: &ToolInvocation, idempotent: bool) -> bool {
        let mut effects = self.effects.lock().unwrap();
        if idempotent
            && effects
                .iter()
                .any(|effect| effect.logical_key == invocation.logical_key)
        {
            return false;
        }
        effects.push(invocation.clone());
        true
    }
}

/// Shared across the tool instances of every worker in one test.
#[derive(Clone, Default)]
struct Probe {
    calls: Arc<AtomicUsize>,
    invocations: Arc<Mutex<Vec<ToolInvocation>>>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl Probe {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn invocations(&self) -> Vec<ToolInvocation> {
        self.invocations.lock().unwrap().clone()
    }

    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(10), self.entered.notified())
            .await
            .expect("tool call started");
    }
}

#[derive(Clone)]
struct EffectTool {
    ledger: Ledger,
    probe: Probe,
    /// Calls that fail before any effect, starting at call `fail_from`.
    fail_from: usize,
    fail_calls: usize,
    fail_with: fn() -> ExternalError,
    /// Calls that record the effect and then wait for `probe.release`.
    hold_calls: usize,
    idempotent: bool,
}

impl EffectTool {
    fn new(ledger: &Ledger, probe: &Probe) -> Self {
        Self {
            ledger: ledger.clone(),
            probe: probe.clone(),
            fail_from: 1,
            fail_calls: 0,
            fail_with: || ExternalError::Network,
            hold_calls: 0,
            idempotent: false,
        }
    }

    fn hold_first(mut self) -> Self {
        self.hold_calls = 1;
        self
    }

    fn fail_first(mut self, error: fn() -> ExternalError) -> Self {
        self.fail_calls = 1;
        self.fail_with = error;
        self
    }

    fn idempotent(mut self) -> Self {
        self.idempotent = true;
        self
    }
}

impl Tool for EffectTool {
    const NAME: &'static str = TOOL;
    type Args = EffectArgs;
    type Output = String;
    type Error = ExternalError;

    fn description(&self) -> String {
        "Perform an effect on an external service".into()
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({"type":"object","properties":{"value":{"type":"string"}}})
    }

    fn map_error(&self, error: Self::Error) -> ToolExecutionError {
        match error {
            ExternalError::Network => ToolExecutionError::network(error.to_string()),
            ExternalError::Quota => {
                ToolExecutionError::provider(error.to_string()).with_retryable(false)
            }
        }
    }

    async fn call(
        &self,
        context: &mut ToolContext,
        args: EffectArgs,
    ) -> Result<Self::Output, Self::Error> {
        let invocation = context.require::<ToolInvocation>().unwrap().clone();
        self.probe
            .invocations
            .lock()
            .unwrap()
            .push(invocation.clone());
        let call = self.probe.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call >= self.fail_from && call < self.fail_from + self.fail_calls {
            return Err((self.fail_with)());
        }
        self.ledger.accept(&invocation, self.idempotent);
        self.probe.entered.notify_one();
        if call <= self.hold_calls {
            self.probe.release.notified().await;
        }
        Ok(format!("{}=applied", args.value))
    }
}

/// Guard store whose worker can die between the claim and the tool call.
struct DyingGuard {
    inner: Arc<InMemoryGuardStore>,
    die_after_claim: AtomicBool,
    claimed: Arc<Notify>,
}

impl InvocationGuardStore for DyingGuard {
    fn claim(&self, request: ClaimRequest) -> BoxFuture<'_, Result<ClaimOutcome, GuardError>> {
        Box::pin(async move {
            let outcome = self.inner.claim(request).await?;
            if matches!(outcome, ClaimOutcome::Created)
                && self.die_after_claim.load(Ordering::SeqCst)
            {
                self.claimed.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(outcome)
        })
    }

    fn settle(
        &self,
        key: &LogicalCallKey,
        claim_token: &str,
        result: DurableToolResult,
    ) -> BoxFuture<'_, Result<SettleOutcome, GuardError>> {
        let key = key.clone();
        let claim_token = claim_token.to_string();
        Box::pin(async move { self.inner.settle(&key, &claim_token, result).await })
    }

    fn get(&self, key: &LogicalCallKey) -> BoxFuture<'_, Result<Option<ClaimRecord>, GuardError>> {
        let key = key.clone();
        Box::pin(async move { self.inner.get(&key).await })
    }
}

fn tool_call(value: &str) -> MockTurn {
    MockTurn::tool_call(
        format!("call-{value}"),
        TOOL,
        serde_json::json!({"value": value}),
    )
}

/// A short worker lock so a stopped worker's activity is redelivered quickly.
fn runtime_options() -> RuntimeOptions {
    RuntimeOptions {
        worker_lock_timeout: Duration::from_secs(1),
        ..Default::default()
    }
}

/// One worker process. Its dispatchers run on a private tokio runtime, so
/// `die` stops every task at once, including tool calls in progress, and
/// nothing on the worker acknowledges or abandons its work items.
struct Worker {
    orchestrator: Option<AgentOrchestrator>,
    runtime: Option<tokio::runtime::Runtime>,
}

impl Worker {
    async fn start(store: Arc<SqliteProvider>, agent: rig_durable::AgentDefinition) -> Self {
        Self::start_with(store, agent, runtime_options()).await
    }

    async fn start_with(
        store: Arc<SqliteProvider>,
        agent: rig_durable::AgentDefinition,
        options: RuntimeOptions,
    ) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let orchestrator = runtime
            .spawn(async move {
                AgentOrchestrator::builder(store)
                    .register(agent)
                    .unwrap()
                    .runtime_options(options)
                    .start()
                    .await
                    .unwrap()
            })
            .await
            .unwrap();
        Self {
            orchestrator: Some(orchestrator),
            runtime: Some(runtime),
        }
    }

    fn agent(&self, name: &str) -> rig_durable::DurableAgent {
        self.orchestrator.as_ref().unwrap().agent(name).unwrap()
    }

    /// Stop the worker without any cleanup, like a crashed process.
    fn die(mut self) {
        self.runtime.take().unwrap().shutdown_background();
    }

    async fn stop(mut self) {
        self.orchestrator.take().unwrap().shutdown(None).await;
        self.runtime.take().unwrap().shutdown_background();
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

fn logical_agent(name: &str, model: MockCompletionModel) -> rig_durable::DurableAgentBuilder {
    DurableAgent::builder(name, model)
        .invocation_contract(InvocationContract::Logical)
        .max_turns(4)
}

/// Text the model received as tool results in `request`.
fn tool_result_text(model: &MockCompletionModel, request: usize) -> String {
    let requests = model.requests();
    let request = requests
        .get(request)
        .unwrap_or_else(|| panic!("model received request {request}"));
    request
        .chat_history
        .iter()
        .filter_map(|message| match message {
            Message::User { content } => Some(content.to_vec()),
            _ => None,
        })
        .flatten()
        .filter_map(|content| match content {
            UserContent::ToolResult(result) => Some(
                result
                    .content
                    .iter()
                    .filter_map(|content| match content {
                        ToolResultContent::Text(text) => Some(text.text.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Run one prompt whose tool call is interrupted after the external service
/// accepted the effect, then recover it on a second worker. Returns the
/// recovering worker's model so tests can inspect what it received.
async fn interrupt_and_recover(
    name: &str,
    first_tool: EffectTool,
    recovering_tool: EffectTool,
    options: ToolOptions,
    guard: Option<Arc<dyn InvocationGuardStore>>,
) -> (MockCompletionModel, String) {
    let probe = first_tool.probe.clone();
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let mut first = logical_agent(name, MockCompletionModel::from_turns([tool_call("order")]))
        .tool_with(first_tool, options.clone());
    if let Some(guard) = &guard {
        first = first.invocation_guard(Arc::clone(guard));
    }
    let first = Worker::start(store.clone(), first.build().unwrap()).await;
    first
        .agent(name)
        .start_with_id("run", "apply the order")
        .await
        .unwrap();
    probe.entered().await;
    first.die();

    let recovering_model = MockCompletionModel::from_turns([MockTurn::text("done")]);
    let mut second =
        logical_agent(name, recovering_model.clone()).tool_with(recovering_tool, options);
    if let Some(guard) = guard {
        second = second.invocation_guard(guard);
    }
    let second = Worker::start(store, second.build().unwrap()).await;
    let response = second
        .agent(name)
        .run("run")
        .wait_timeout(Duration::from_secs(15))
        .await
        .unwrap();
    second.stop().await;
    (recovering_model, response.output)
}

#[tokio::test]
async fn read_only_tool_retries_after_an_interrupted_attempt() {
    let ledger = Ledger::default();
    let probe = Probe::default();
    let (model, output) = interrupt_and_recover(
        "read-only",
        EffectTool::new(&ledger, &probe).hold_first(),
        EffectTool::new(&ledger, &probe),
        ToolOptions::default()
            .retry(RetryPolicy::new(1))
            .policy(ToolPolicy::read_only()),
        None,
    )
    .await;

    assert_eq!(output, "done");
    assert_eq!(probe.calls(), 2, "interrupted read runs again");
    assert_eq!(ledger.effects().len(), 2);
    let invocations = probe.invocations();
    assert_eq!(invocations[0].logical_key, invocations[1].logical_key);
    assert!(tool_result_text(&model, 0).contains("order=applied"));
}

#[tokio::test]
async fn idempotent_tool_records_one_effect_across_interrupted_attempts() {
    let ledger = Ledger::default();
    let probe = Probe::default();
    let (model, output) = interrupt_and_recover(
        "idempotent",
        EffectTool::new(&ledger, &probe).idempotent().hold_first(),
        EffectTool::new(&ledger, &probe).idempotent(),
        ToolOptions::default()
            .retry(RetryPolicy::new(1))
            .policy(ToolPolicy::idempotent()),
        None,
    )
    .await;

    assert_eq!(output, "done");
    assert_eq!(probe.calls(), 2, "both attempts reached the service");
    let effects = ledger.effects();
    assert_eq!(effects.len(), 1, "one logical key, one effect");
    let key = effects[0]
        .logical_key
        .clone()
        .expect("logical key supplied");
    assert_eq!(key.submission_id, "prompt-0");
    assert!(tool_result_text(&model, 0).contains("order=applied"));
}

#[tokio::test]
async fn guarded_tool_is_not_repeated_and_reports_uncertainty() {
    let ledger = Ledger::default();
    let probe = Probe::default();
    let guard = Arc::new(InMemoryGuardStore::new());
    let (model, output) = interrupt_and_recover(
        "guarded",
        EffectTool::new(&ledger, &probe).hold_first(),
        EffectTool::new(&ledger, &probe),
        ToolOptions::default()
            .retry(RetryPolicy::new(1))
            .policy(ToolPolicy::interrupt_on_uncertain()),
        Some(guard.clone()),
    )
    .await;

    assert_eq!(output, "done");
    assert_eq!(probe.calls(), 1, "no second effect attempt");
    assert_eq!(ledger.effects().len(), 1);
    let feedback = tool_result_text(&model, 0);
    assert!(feedback.contains("was interrupted"), "{feedback}");
    assert!(!feedback.contains("applied"), "{feedback}");
    assert!(!feedback.contains("failed"), "{feedback}");
    let key = probe.invocations()[0].logical_key.clone().unwrap();
    let record = guard.get(&key).await.unwrap().expect("claim retained");
    assert!(
        matches!(record.state, rig_durable::guard::ClaimState::Claimed),
        "the dead worker never settled its claim"
    );
}

#[tokio::test]
async fn claim_committed_then_worker_dies_before_io() {
    let ledger = Ledger::default();
    let probe = Probe::default();
    let inner = Arc::new(InMemoryGuardStore::new());
    let claimed = Arc::new(Notify::new());
    let dying: Arc<dyn InvocationGuardStore> = Arc::new(DyingGuard {
        inner: Arc::clone(&inner),
        die_after_claim: AtomicBool::new(true),
        claimed: Arc::clone(&claimed),
    });
    let options = ToolOptions::default()
        .retry(RetryPolicy::new(1))
        .policy(ToolPolicy::interrupt_on_uncertain());
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let first = Worker::start(
        store.clone(),
        logical_agent(
            "claim-then-die",
            MockCompletionModel::from_turns([tool_call("order")]),
        )
        .invocation_guard(dying)
        .tool_with(EffectTool::new(&ledger, &probe), options.clone())
        .build()
        .unwrap(),
    )
    .await;
    first
        .agent("claim-then-die")
        .start_with_id("run", "apply the order")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), claimed.notified())
        .await
        .unwrap();
    first.die();
    assert_eq!(probe.calls(), 0);

    let model = MockCompletionModel::from_turns([MockTurn::text("done")]);
    let recovering: Arc<dyn InvocationGuardStore> = Arc::new(DyingGuard {
        inner,
        die_after_claim: AtomicBool::new(false),
        claimed: Arc::new(Notify::new()),
    });
    let second = Worker::start(
        store,
        logical_agent("claim-then-die", model.clone())
            .invocation_guard(recovering)
            .tool_with(EffectTool::new(&ledger, &probe), options)
            .build()
            .unwrap(),
    )
    .await;
    let response = second
        .agent("claim-then-die")
        .run("run")
        .wait_timeout(Duration::from_secs(15))
        .await
        .unwrap();
    second.stop().await;

    assert_eq!(response.output, "done");
    assert_eq!(probe.calls(), 0, "recovery does not execute the tool");
    assert!(ledger.effects().is_empty());
    assert!(tool_result_text(&model, 0).contains("was interrupted"));
}

#[tokio::test]
async fn duplicate_workers_race_for_one_claim_and_no_takeover_repeats_the_write() {
    let ledger = Ledger::default();
    let probe = Probe::default();
    let guard: Arc<dyn InvocationGuardStore> = Arc::new(InMemoryGuardStore::new());
    let worker = || {
        Arc::new(
            ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![
                EffectTool::new(&ledger, &probe).hold_first(),
            ])))
            .with_guard(Some(Arc::clone(&guard))),
        )
    };
    let input = ToolActivityInput {
        name: TOOL.into(),
        arguments: r#"{"value":"order"}"#.into(),
        invocation: ToolInvocation {
            execution_id: "exec:1".into(),
            prompt_index: 0,
            turn: 1,
            call_index: 0,
            logical_key: Some(LogicalCallKey {
                logical_execution_id: "exec".into(),
                submission_id: "prompt-0".into(),
                model_turn: 1,
                call_index: 0,
            }),
            attempt: None,
        },
        policy: Some(ToolPolicy::interrupt_on_uncertain()),
    };

    let first_worker = worker();
    let first_input = input.clone();
    let first = tokio::spawn(async move { first_worker.execute(first_input).await });
    probe.entered().await;

    let second = worker().execute(input.clone()).await.unwrap();
    let second = second.result.unwrap();
    assert_eq!(second.disposition, ToolDisposition::Interrupted);
    assert_eq!(
        second.interruption.unwrap().reason,
        InterruptionReason::ClaimHeld
    );

    tokio::time::sleep(Duration::from_millis(200)).await;
    let late = worker().execute(input.clone()).await.unwrap();
    assert_eq!(
        late.result.unwrap().disposition,
        ToolDisposition::Interrupted,
        "waiting does not expire the claim"
    );

    probe.release.notify_one();
    let first = first.await.unwrap().unwrap().result.unwrap();
    assert_eq!(first.disposition, ToolDisposition::Success);

    let after_settle = worker().execute(input).await.unwrap().result.unwrap();
    assert_eq!(after_settle.disposition, ToolDisposition::Success);
    assert_eq!(probe.calls(), 1, "one worker ran the tool");
    assert_eq!(ledger.effects().len(), 1);
}

#[tokio::test]
async fn explicit_non_retryable_error_is_recorded_and_retryable_error_is_retried() {
    let quota: fn() -> ExternalError = || ExternalError::Quota;
    let network: fn() -> ExternalError = || ExternalError::Network;
    for (name, error, expected_calls, expected_text) in [
        ("quota", quota, 1, "quota exhausted"),
        ("network", network, 2, "order=applied"),
    ] {
        let ledger = Ledger::default();
        let probe = Probe::default();
        let model = MockCompletionModel::from_turns([tool_call("order"), MockTurn::text("done")]);
        let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
        let orchestrator = Worker::start(
            store,
            logical_agent(name, model.clone())
                .tool_with(
                    EffectTool::new(&ledger, &probe).fail_first(error),
                    ToolOptions::default()
                        .retry(RetryPolicy::new(3))
                        .policy(ToolPolicy::read_only()),
                )
                .build()
                .unwrap(),
        )
        .await;
        let output = orchestrator
            .agent(name)
            .prompt("apply the order")
            .await
            .unwrap();
        orchestrator.stop().await;

        assert_eq!(output, "done");
        assert_eq!(probe.calls(), expected_calls, "{name}");
        let text = tool_result_text(&model, 1);
        assert!(text.contains(expected_text), "{name}: {text}");
    }
}

#[tokio::test]
async fn two_submissions_use_distinct_logical_keys_and_retries_keep_theirs() {
    let ledger = Ledger::default();
    let probe = Probe::default();
    let model = MockCompletionModel::from_turns([
        tool_call("first"),
        MockTurn::text("first done"),
        tool_call("second"),
        MockTurn::text("second done"),
    ]);
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    // No worker stops here; a held call must not be redelivered.
    let orchestrator = Worker::start_with(
        store,
        logical_agent("submissions", model)
            .tool_with(
                EffectTool {
                    hold_calls: 1,
                    fail_from: 2,
                    fail_calls: 1,
                    ..EffectTool::new(&ledger, &probe)
                },
                ToolOptions::default()
                    .retry(RetryPolicy::new(2))
                    .policy(ToolPolicy::read_only()),
            )
            .build()
            .unwrap(),
        RuntimeOptions::default(),
    )
    .await;
    let agent = orchestrator.agent("submissions");
    let run = agent.start_with_id("run", "first prompt").await.unwrap();
    probe.entered().await;
    // Steering is accepted once the held call completes, so release the call
    // after the steering command is queued.
    let (steered, ()) = tokio::join!(run.steer("second prompt"), async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        probe.release.notify_one();
    });
    steered.unwrap();
    let response = run.wait_timeout(Duration::from_secs(15)).await.unwrap();
    orchestrator.stop().await;

    assert_eq!(response.output, "second done");
    let invocations = probe.invocations();
    assert_eq!(
        invocations.len(),
        3,
        "one call, then a failed and a retried call"
    );
    let keys: Vec<LogicalCallKey> = invocations
        .iter()
        .map(|invocation| invocation.logical_key.clone().unwrap())
        .collect();
    assert_eq!(keys[0].submission_id, "prompt-0");
    assert_eq!(keys[1].submission_id, "prompt-1");
    assert_ne!(keys[0], keys[1]);
    assert_eq!(keys[1], keys[2], "the retry keeps its submission's key");
    let attempts: Vec<Option<u32>> = invocations
        .iter()
        .map(|invocation| invocation.attempt.as_ref().unwrap().activity_attempt)
        .collect();
    assert_eq!(attempts, [Some(1), Some(1), Some(2)]);
}

#[tokio::test]
async fn continue_as_new_keeps_logical_identity_and_worker_upgrade_fails_closed() {
    let ledger = Ledger::default();
    let probe = Probe::default();
    let options = |version: &str| {
        ToolOptions::default()
            .retry(RetryPolicy::new(1))
            .policy(ToolPolicy::read_only().implementation_version(version))
    };
    let checkpoint = CheckpointConfig {
        policy: CheckpointPolicy::Every(1.try_into().unwrap()),
        target_version: None,
    };
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let first = Worker::start(
        store.clone(),
        logical_agent(
            "upgrade",
            MockCompletionModel::from_turns([tool_call("order")]),
        )
        .checkpoint(checkpoint.clone())
        .tool_with(EffectTool::new(&ledger, &probe).hold_first(), options("1"))
        .build()
        .unwrap(),
    )
    .await;
    let run = first
        .agent("upgrade")
        .start_with_id("run", "apply the order")
        .await
        .unwrap();
    probe.entered().await;
    let invocation = probe.invocations()[0].clone();
    assert!(
        invocation.execution_id.ends_with(":2"),
        "the tool runs in the continued execution: {}",
        invocation.execution_id
    );
    let key = invocation.logical_key.clone().unwrap();
    assert_eq!(key.logical_execution_id, run.instance_id());
    assert_eq!(key.submission_id, "prompt-0");
    first.die();

    let upgraded = Worker::start(
        store,
        logical_agent(
            "upgrade",
            MockCompletionModel::from_turns([MockTurn::text("done")]),
        )
        .checkpoint(checkpoint)
        .tool_with(EffectTool::new(&ledger, &probe), options("2"))
        .build()
        .unwrap(),
    )
    .await;
    let error = upgraded
        .agent("upgrade")
        .run("run")
        .wait_timeout(Duration::from_secs(15))
        .await
        .unwrap_err();
    upgraded.stop().await;

    let message = error.to_string();
    assert!(message.contains("implementation version"), "{message}");
    assert_eq!(
        probe.calls(),
        1,
        "the upgraded worker never ran the recorded call"
    );
}

#[tokio::test]
async fn approval_is_not_repeated_after_restart_and_binds_to_arguments() {
    let ledger = Ledger::default();
    let probe = Probe::default();
    let options = ToolOptions::default()
        .retry(RetryPolicy::new(1))
        .policy(ToolPolicy::read_only())
        .require_approval();
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let first = Worker::start(
        store.clone(),
        logical_agent(
            "approval",
            MockCompletionModel::from_turns([tool_call("order")]),
        )
        .tool_with(
            EffectTool::new(&ledger, &probe).hold_first(),
            options.clone(),
        )
        .build()
        .unwrap(),
    )
    .await;
    let run = first
        .agent("approval")
        .start_with_id("run", "apply the order")
        .await
        .unwrap();
    let request = run.next_approval().await.unwrap();
    assert!(request.approval_id.starts_with("approval-v2-"));
    assert_eq!(probe.calls(), 0);
    run.approve(&request).await.unwrap();
    probe.entered().await;
    first.die();

    let model = MockCompletionModel::from_turns([MockTurn::text("done")]);
    let second = Worker::start(
        store,
        logical_agent("approval", model.clone())
            .tool_with(EffectTool::new(&ledger, &probe), options.clone())
            .build()
            .unwrap(),
    )
    .await;
    let agent = second.agent("approval");
    let response = agent
        .run("run")
        .wait_timeout(Duration::from_secs(15))
        .await
        .expect("the recorded approval releases the call without a new request");
    assert_eq!(response.output, "done");
    assert_eq!(probe.calls(), 2);
    second.stop().await;

    // A decision recorded for other arguments does not authorize a rewritten call.
    let probe = Probe::default();
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    let orchestrator = Worker::start(
        store,
        logical_agent(
            "binding",
            MockCompletionModel::from_turns([tool_call("rewritten"), MockTurn::text("done")]),
        )
        .tool_with(EffectTool::new(&ledger, &probe), options)
        .build()
        .unwrap(),
    )
    .await;
    let run = orchestrator
        .agent("binding")
        .start_with_id("run", "apply the order")
        .await
        .unwrap();
    let rewritten = run.next_approval().await.unwrap();
    assert_ne!(rewritten.approval_id, request.approval_id);
    let stale = ApprovalRequest {
        approval_id: request.approval_id.clone(),
        ..rewritten.clone()
    };
    run.approve(&stale).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        probe.calls(),
        0,
        "the old decision does not authorize new arguments"
    );
    assert_eq!(
        run.next_approval().await.unwrap().approval_id,
        rewritten.approval_id
    );
    run.approve(&rewritten).await.unwrap();
    let response = run.wait_timeout(Duration::from_secs(15)).await.unwrap();
    orchestrator.stop().await;
    assert_eq!(response.output, "done");
    assert_eq!(probe.calls(), 1);
}

#[tokio::test]
async fn builder_rejects_policies_the_contract_or_worker_cannot_honor() {
    let ledger = Ledger::default();
    let probe = Probe::default();
    let legacy = DurableAgent::builder(
        "legacy",
        MockCompletionModel::from_turns([MockTurn::text("done")]),
    )
    .tool_with(
        EffectTool::new(&ledger, &probe),
        ToolOptions::default().policy(ToolPolicy::read_only()),
    )
    .build();
    assert!(matches!(
        legacy,
        Err(AgentOrchestratorError::InvocationContractRequired(name)) if name == TOOL
    ));

    let unguarded = logical_agent(
        "unguarded",
        MockCompletionModel::from_turns([MockTurn::text("done")]),
    )
    .tool_with(
        EffectTool::new(&ledger, &probe),
        ToolOptions::default().policy(ToolPolicy::interrupt_on_uncertain()),
    )
    .build();
    assert!(matches!(
        unguarded,
        Err(AgentOrchestratorError::GuardStoreRequired(name)) if name == TOOL
    ));
}
