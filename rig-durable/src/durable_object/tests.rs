use super::*;
use rig::test_utils::{MockAddTool, MockCompletionModel, MockTurn};
use serde_json::Value;
use std::{collections::BTreeMap, rc::Rc};

#[derive(Clone)]
struct Database(Rc<rusqlite::Connection>, Rc<Cell<bool>>);
impl Database {
    fn new() -> Result<Self, Error> {
        Ok(Self(
            Rc::new(rusqlite::Connection::open_in_memory().map_err(sql_error)?),
            Rc::new(Cell::new(false)),
        ))
    }
}
fn sql_error(error: rusqlite::Error) -> Error {
    Error::Storage(error.to_string())
}
impl SqlDatabase for Database {
    fn exec(&self, query: &str, params: &[String]) -> Result<Vec<BTreeMap<String, Value>>, Error> {
        let mut stmt = self.0.prepare(query).map_err(sql_error)?;
        let names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .map_err(sql_error)?;
        let mut result = Vec::new();
        while let Some(row) = rows.next().map_err(sql_error)? {
            let mut record = BTreeMap::new();
            for (index, name) in names.iter().enumerate() {
                let value = match row.get_ref(index).map_err(sql_error)? {
                    rusqlite::types::ValueRef::Text(bytes) => {
                        Value::String(String::from_utf8_lossy(bytes).into())
                    }
                    rusqlite::types::ValueRef::Integer(number) => Value::from(number),
                    _ => Value::Null,
                };
                record.insert(name.clone(), value);
            }
            result.push(record);
        }
        Ok(result)
    }
    fn transaction<T>(&self, f: impl FnOnce() -> Result<T, Error>) -> Result<T, Error> {
        self.0.execute_batch("BEGIN IMMEDIATE").map_err(sql_error)?;
        match f() {
            Ok(value) => {
                self.0.execute_batch("COMMIT").map_err(sql_error)?;
                if self.1.replace(false) {
                    return Err(Error::Storage("injected uncertain commit".into()));
                }
                Ok(value)
            }
            Err(error) => {
                self.0.execute_batch("ROLLBACK").map_err(sql_error)?;
                Err(error)
            }
        }
    }
}

#[derive(Clone, Default)]
struct Clock {
    now: Rc<Cell<u64>>,
    alarm: Rc<Cell<Option<u64>>>,
}
impl Wake for Clock {
    fn now_ms(&self) -> u64 {
        self.now.get()
    }
    async fn arm(&self, deadline: u64) -> Result<(), Error> {
        self.alarm.set(Some(deadline));
        Ok(())
    }
    async fn delay(&self, ms: u64) {
        self.now.set(self.now.get() + ms);
    }
}

#[tokio::test]
async fn tools_and_deduplication_survive_reopen() -> Result<(), Error> {
    let db = Database::new()?;
    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call("add-1", "add", serde_json::json!({"x": 7, "y": 12})),
        MockTurn::text("19"),
    ]);
    let engine = Builder::new(model.clone()).tool(MockAddTool).build(
        db.clone(),
        Clock::default(),
        "session",
    )?;
    let input = SubmitInput::new("r1", "add 7 and 12");
    let receipt = engine.submit(input.clone()).await?;
    assert_eq!(receipt.prompt_index, 0);
    let response = engine
        .wait("r1")
        .await?
        .ok_or_else(|| Error::Invalid("no response".into()))?;
    assert_eq!(response.output(), "19");
    assert_eq!(response.tool_outcomes.len(), 1);
    assert_eq!(
        response.tool_outcomes[0].disposition,
        crate::ToolDisposition::Success
    );
    assert_eq!(model.requests().len(), 2);
    assert_eq!(engine.transcript()?.len(), 4);
    drop(engine);
    let reopened =
        Builder::new(model.clone())
            .tool(MockAddTool)
            .build(db, Clock::default(), "session")?;
    assert_eq!(
        reopened.submit(input).await?.state,
        SubmissionState::Answered
    );
    assert_eq!(
        reopened.wait("r1").await?.map(|r| r.response.output),
        Some("19".into())
    );
    assert_eq!(model.requests().len(), 2);
    Ok(())
}

#[tokio::test]
async fn admission_and_rollback() -> Result<(), Error> {
    let engine = Builder::new(MockCompletionModel::from_turns([MockTurn::text("ok")])).build(
        Database::new()?,
        Clock::default(),
        "s",
    )?;
    let input = SubmitInput::new("one", "first");
    engine.submit(input.clone()).await?;
    assert!(matches!(
        engine.submit(SubmitInput::new("one", "changed")).await,
        Err(Error::Submission(crate::SubmissionError::Conflict { .. }))
    ));
    assert!(matches!(
        engine
            .submit(SubmitInput::new("two", "second").mode(crate::SubmissionMode::RejectIfBusy))
            .await,
        Err(Error::Submission(crate::SubmissionError::Busy))
    ));
    let before = engine.transcript()?;
    let result: Result<(), Error> = engine.update(|state| {
        state.closed = true;
        engine.append(&[Message::user("must roll back")])?;
        Err(Error::Storage("injected".into()))
    });
    assert!(result.is_err());
    assert_eq!(engine.transcript()?, before);
    assert!(!engine.status()?.closed);
    engine.close()?;
    assert_eq!(engine.submit(input).await?.state, SubmissionState::Queued);
    assert!(matches!(
        engine.submit(SubmitInput::new("new", "x")).await,
        Err(Error::Submission(crate::SubmissionError::Closed))
    ));
    engine.drive().await?;
    assert_eq!(
        engine.status()?.receipts[0].state,
        SubmissionState::Answered
    );
    Ok(())
}

#[test]
fn chunks_and_cap_are_atomic() -> Result<(), Error> {
    let db = Database::new()?;
    storage::initialize(&db)?;
    let value = "é".repeat(storage::CHUNK_BYTES + 7);
    db.transaction(|| storage::write(&db, "large", &value, 2_000_000))?;
    assert_eq!(storage::read::<String>(&db, "large")?, Some(value.clone()));
    let rows = db.exec("SELECT data FROM rig_runs WHERE key = 'large'", &[])?;
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|row| {
        row["data"]
            .as_str()
            .is_some_and(|s| s.len() <= storage::CHUNK_BYTES)
    }));
    assert!(matches!(
        db.transaction(|| storage::write(&db, "large", &value, 100)),
        Err(Error::Limit { .. })
    ));
    assert_eq!(storage::read::<String>(&db, "large")?, Some(value));
    Ok(())
}

#[derive(Clone)]
struct GateTool {
    calls: Arc<std::sync::atomic::AtomicUsize>,
    block: Arc<std::sync::atomic::AtomicBool>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl GateTool {
    fn new() -> Self {
        Self {
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            block: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        }
    }
    fn count(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Tool for GateTool {
    const NAME: &'static str = "gate";
    type Args = Value;
    type Output = String;
    type Error = std::convert::Infallible;
    fn description(&self) -> String {
        "Wait for the test gate".into()
    }
    fn parameters(&self) -> Value {
        serde_json::json!({"type":"object"})
    }
    async fn call(&self, _: &mut rig::tool::ToolContext, _: Value) -> Result<String, Self::Error> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.entered.notify_one();
        if self.block.load(std::sync::atomic::Ordering::SeqCst) {
            self.release.notified().await;
        }
        Ok("effect committed externally".into())
    }
}

#[tokio::test]
async fn uncertain_tools_obey_both_policies_after_restart() -> Result<(), Error> {
    use crate::{ReplaySafety as S, ToolDisposition as D, ToolPolicy};
    for (stored, current, expected_calls, disposition) in [
        (S::ApplicationManaged, S::ApplicationManaged, 2, D::Success),
        (S::ReadOnly, S::ReadOnly, 2, D::Success),
        (S::Idempotent, S::Idempotent, 2, D::Success),
        (
            S::InterruptOnUncertain,
            S::InterruptOnUncertain,
            1,
            D::Interrupted,
        ),
        (S::ReadOnly, S::InterruptOnUncertain, 1, D::Interrupted),
        (S::InterruptOnUncertain, S::ReadOnly, 1, D::Interrupted),
    ] {
        let db = Database::new()?;
        let gate = GateTool::new();
        let model = MockCompletionModel::from_turns([
            MockTurn::tool_call("gate-call", "gate", serde_json::json!({"amount": 13})),
            MockTurn::text("done"),
        ]);
        let engine = Builder::new(model.clone())
            .tool_with(
                gate.clone(),
                ToolOptions::default().policy(ToolPolicy::default().replay_safety(stored)),
            )
            .build(db.clone(), Clock::default(), "crash")?;
        engine.submit(SubmitInput::new("request", "go")).await?;
        {
            let driving = engine.drive();
            tokio::pin!(driving);
            tokio::select! {
                result = &mut driving => { result?; return Err(Error::Invalid("drive ended before tool blocked".into())); }
                () = gate.entered.notified() => {}
            }
        }
        assert_eq!(gate.count(), 1);
        assert!(engine.result("request")?.is_none());
        drop(engine);
        gate.block.store(false, std::sync::atomic::Ordering::SeqCst);
        let engine = Builder::new(model.clone())
            .tool_with(
                gate.clone(),
                ToolOptions::default().policy(ToolPolicy::default().replay_safety(current)),
            )
            .build(db, Clock::default(), "crash")?;
        let result = engine
            .wait("request")
            .await?
            .ok_or_else(|| Error::Invalid("missing recovered answer".into()))?;
        assert_eq!(gate.count(), expected_calls, "{stored:?} / {current:?}");
        assert_eq!(result.tool_outcomes[0].disposition, disposition);
        assert_eq!(
            model.requests().len(),
            2,
            "committed model turn must not repeat"
        );
    }
    Ok(())
}

#[tokio::test]
async fn submissions_and_alarm_interleave_without_another_driver() -> Result<(), Error> {
    let gate = GateTool::new();
    let clock = Clock::default();
    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call("gate", "gate", serde_json::json!({})),
        MockTurn::text("first answer"),
        MockTurn::text("second answer"),
    ]);
    let engine = Builder::new(model.clone()).tool(gate.clone()).build(
        Database::new()?,
        clock.clone(),
        "s",
    )?;
    engine.submit(SubmitInput::new("one", "first")).await?;
    let driving = engine.drive();
    tokio::pin!(driving);
    tokio::select! {
        result = &mut driving => { result?; return Err(Error::Invalid("gate did not block".into())); }
        () = gate.entered.notified() => {}
    }
    clock.now.set(37_000);
    clock.alarm.set(None);
    engine.alarm().await?;
    assert_eq!(clock.alarm.get(), Some(67_000));
    engine.drive().await?;
    assert_eq!(gate.count(), 1);
    assert!(engine.result("one")?.is_none());
    assert!(matches!(
        engine
            .submit(SubmitInput::new("reject", "urgent").mode(crate::SubmissionMode::RejectIfBusy))
            .await,
        Err(Error::Submission(crate::SubmissionError::Busy))
    ));
    engine.submit(SubmitInput::new("two", "second")).await?;
    gate.release.notify_one();
    driving.await?;
    assert_eq!(engine.status()?.receipts.len(), 2);
    assert!(
        engine
            .status()?
            .receipts
            .iter()
            .all(|r| r.state == SubmissionState::Answered)
    );
    assert_eq!(
        engine.result("two")?.map(|r| r.response.output),
        Some("second answer".into())
    );
    assert_eq!(model.requests().len(), 3);
    clock.alarm.set(None);
    engine.alarm().await?;
    assert_eq!(clock.alarm.get(), None, "idle alarms must not recur");
    Ok(())
}

#[tokio::test]
async fn approvals_survive_restart_and_denial_never_runs() -> Result<(), Error> {
    for approve in [false, true] {
        let db = Database::new()?;
        let gate = GateTool::new();
        gate.block.store(false, std::sync::atomic::Ordering::SeqCst);
        let model = MockCompletionModel::from_turns([
            MockTurn::tool_call("gate", "gate", serde_json::json!({"value": 8})),
            MockTurn::text("done"),
        ]);
        let builder = || {
            Builder::new(model.clone())
                .tool_with(gate.clone(), ToolOptions::default().require_approval())
        };
        let engine = builder().build(db.clone(), Clock::default(), "s")?;
        engine.submit(SubmitInput::new("r", "go")).await?;
        assert!(engine.wait("r").await?.is_none());
        let approval = engine.status()?.approvals.remove(0);
        assert_eq!(gate.count(), 0);
        assert!(engine.approve("wrong-id").await.is_err());
        if approve {
            engine.approve(&approval.approval_id).await?;
        } else {
            engine
                .deny(&approval.approval_id, Some("not allowed".into()))
                .await?;
        }
        drop(engine);
        let engine = builder().build(db, Clock::default(), "s")?;
        let result = engine
            .wait("r")
            .await?
            .ok_or_else(|| Error::Invalid("missing approval result".into()))?;
        assert_eq!(gate.count(), usize::from(approve));
        assert_eq!(
            result.tool_outcomes[0].disposition,
            if approve {
                crate::ToolDisposition::Success
            } else {
                crate::ToolDisposition::Refused
            }
        );
    }
    Ok(())
}

#[tokio::test]
async fn model_intent_reissues_and_retry_deadlines_are_durable() -> Result<(), Error> {
    let db = Database::new()?;
    let clock = Clock::default();
    let model =
        MockCompletionModel::from_turns([MockTurn::text("lost"), MockTurn::text("recovered")]);
    let engine = Builder::new(model.clone()).build(db.clone(), clock.clone(), "s")?;
    engine.submit(SubmitInput::new("r", "question")).await?;
    // A model intent with no committed result is the recovery boundary.
    engine.update(|state| {
        state.queued.clear();
        let mut ledger = engine.ledger()?;
        ledger.set_state("r", SubmissionState::Running);
        engine.save_ledger(&ledger)?;
        state.active = Some(Run {
            request_id: "r".into(),
            prompt_index: 0,
            agent: AgentRun::new("question"),
            recorded: 0,
            turn: 1,
            phase: Phase::Model,
            outcomes: Vec::new(),
            attempt: 1,
            deadline_ms: Some(5000),
        });
        Ok(())
    })?;
    let mut agent = AgentRun::new("question");
    let Effect::Model { request, .. } =
        driver::next_effect(&mut agent, (&engine.config).into()).map_err(Error::Invalid)?
    else {
        return Err(Error::Invalid("expected model".into()));
    };
    completion::complete(&engine.model, request)
        .await
        .map_err(Error::Invalid)?;
    drop(engine);
    let engine = Builder::new(model.clone()).build(db, clock.clone(), "s")?;
    assert_eq!(engine.alarm_deadline()?, Some(5000));
    engine.alarm().await?;
    assert_eq!(model.requests().len(), 1);
    assert_eq!(clock.alarm.get(), Some(5000));
    clock.now.set(5000);
    let result = engine
        .wait("r")
        .await?
        .ok_or_else(|| Error::Invalid("missing model recovery result".into()))?;
    assert_eq!(result.output(), "recovered");
    assert_eq!(
        serde_json::to_value(&model.requests()[0])?,
        serde_json::to_value(&model.requests()[1])?
    );
    assert_eq!(engine.transcript()?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn compaction_precedes_queued_prompt() -> Result<(), Error> {
    let model = MockCompletionModel::from_turns([
        MockTurn::text("a1"),
        MockTurn::text("a2"),
        MockTurn::text("a3"),
    ]);
    let summary = MockCompletionModel::from_turns([
        MockTurn::text("summary one"),
        MockTurn::text("summary two"),
    ]);
    let compaction = Compaction::new(
        rig_memory::SlidingWindowMemory::last_messages(2),
        crate::ModelCompactor::new(summary.clone()),
    );
    let engine = Builder::new(model.clone()).compaction(compaction).build(
        Database::new()?,
        Clock::default(),
        "s",
    )?;
    for index in 0..3 {
        engine
            .submit(SubmitInput::new(index.to_string(), format!("q{index}")))
            .await?;
    }
    engine.drive().await?;
    assert_eq!(engine.transcript()?.len(), 6);
    assert_eq!(engine.status()?.compaction.map(|c| c.cutoff), Some(4));
    assert_eq!(model.requests()[2].chat_history.len(), 4);
    assert!(serde_json::to_string(&model.requests()[2])?.contains("summary one"));
    assert_eq!(summary.requests().len(), 2);
    Ok(())
}

#[tokio::test]
async fn uncertain_commit_reloads_and_alarm_rearms_on_error() -> Result<(), Error> {
    let db = Database::new()?;
    let clock = Clock::default();
    let model = MockCompletionModel::from_turns([MockTurn::text("answer")]);
    let engine = Builder::new(model.clone()).build(db.clone(), clock.clone(), "s")?;
    let input = SubmitInput::new("r", "question");
    db.1.set(true);
    assert!(matches!(
        engine.submit(input.clone()).await,
        Err(Error::Storage(_))
    ));
    assert_eq!(engine.submit(input).await?.prompt_index, 0);
    assert_eq!(engine.status()?.receipts.len(), 1);
    db.1.set(true);
    clock.now.set(71);
    engine.alarm().await?;
    assert_eq!(clock.alarm.get(), Some(2071));
    assert_eq!(engine.status()?.receipts[0].state, SubmissionState::Running);
    assert!(model.requests().is_empty());
    engine.alarm().await?;
    assert_eq!(
        engine.result("r")?.map(|r| r.response.output),
        Some("answer".into())
    );
    assert_eq!(engine.transcript()?.len(), 2);
    assert_eq!(model.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn ledger_full_does_not_evict_receipts() -> Result<(), Error> {
    let engine = Builder::new(MockCompletionModel::from_turns([])).build(
        Database::new()?,
        Clock::default(),
        "s",
    )?;
    let mut full = false;
    for index in 0..1000 {
        match engine
            .submit(SubmitInput::new(index.to_string(), "q"))
            .await
        {
            Ok(_) => {}
            Err(Error::Submission(crate::SubmissionError::LedgerFull { .. })) => {
                full = true;
                break;
            }
            Err(error) => return Err(error),
        }
    }
    assert!(full);
    assert_eq!(
        engine
            .submit(SubmitInput::new("0", "q"))
            .await?
            .prompt_index,
        0
    );
    assert_eq!(engine.load()?.queued.len(), engine.status()?.receipts.len());
    Ok(())
}

#[tokio::test]
async fn implementation_version_change_fails_closed() -> Result<(), Error> {
    let db = Database::new()?;
    let gate = GateTool::new();
    let model = MockCompletionModel::from_turns([MockTurn::tool_call(
        "gate",
        "gate",
        serde_json::json!({}),
    )]);
    let engine = Builder::new(model.clone())
        .tool_with(
            gate.clone(),
            ToolOptions::default()
                .require_approval()
                .policy(crate::ToolPolicy::read_only().implementation_version("v1")),
        )
        .build(db.clone(), Clock::default(), "s")?;
    engine.submit(SubmitInput::new("r", "question")).await?;
    engine.drive().await?;
    engine
        .submit(SubmitInput::new("queued", "follow-up"))
        .await?;
    let approval = engine.status()?.approvals[0].approval_id.clone();
    engine.approve(approval).await?;
    drop(engine);
    let engine = Builder::new(model.clone())
        .tool_with(
            gate.clone(),
            ToolOptions::default()
                .require_approval()
                .policy(crate::ToolPolicy::read_only().implementation_version("v2")),
        )
        .build(db, Clock::default(), "s")?;
    assert!(matches!(engine.drive().await, Err(Error::Invalid(_))));
    assert!(engine.status()?.closed);
    assert_eq!(gate.count(), 0);
    assert!(matches!(
        engine.ledger()?.receipts["r"].state,
        SubmissionState::Failed { .. }
    ));
    assert_eq!(
        engine.ledger()?.receipts["queued"].state,
        SubmissionState::Cancelled
    );
    assert_eq!(model.requests().len(), 1);
    Ok(())
}

#[derive(Clone, Default)]
struct FailingTool(Arc<std::sync::atomic::AtomicUsize>);
impl Tool for FailingTool {
    const NAME: &'static str = "fail";
    type Args = Value;
    type Output = String;
    type Error = rig::tool::ToolExecutionError;
    fn description(&self) -> String {
        "Return a provider failure".into()
    }
    fn parameters(&self) -> Value {
        serde_json::json!({"type":"object"})
    }
    async fn call(&self, _: &mut rig::tool::ToolContext, _: Value) -> Result<String, Self::Error> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(rig::tool::ToolExecutionError::provider("retryable failure"))
    }
}

#[tokio::test]
async fn returned_tool_failures_keep_deadline_and_attempt_count() -> Result<(), Error> {
    let db = Database::new()?;
    let clock = Clock::default();
    let tool = FailingTool::default();
    let model = MockCompletionModel::from_turns([MockTurn::tool_call(
        "failure",
        "fail",
        serde_json::json!({}),
    )]);
    let options = ToolOptions::default()
        .policy(crate::ToolPolicy::read_only())
        .retry(
            RetryPolicy::new(2).with_backoff(crate::retry::BackoffStrategy::Fixed {
                delay: std::time::Duration::from_secs(5),
            }),
        );
    let engine = Builder::new(model.clone())
        .tool_with(tool.clone(), options.clone())
        .build(db.clone(), clock.clone(), "s")?;
    engine.submit(SubmitInput::new("r", "question")).await?;
    engine.drive().await?;
    assert_eq!(tool.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(engine.status()?.next_deadline_ms, Some(5000));
    assert_eq!(clock.alarm.get(), Some(5000));
    drop(engine);
    let engine = Builder::new(model.clone())
        .tool_with(tool.clone(), options)
        .build(db, clock.clone(), "s")?;
    clock.now.set(4999);
    // A short wait uses the local clock, but the stored deadline determines it.
    engine.drive().await?;
    assert_eq!(clock.now.get(), 5000);
    assert_eq!(tool.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(engine.status()?.closed);
    assert!(matches!(
        engine.ledger()?.receipts["r"].state,
        SubmissionState::Failed { .. }
    ));
    engine.alarm().await?;
    assert_eq!(tool.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(model.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn streaming_parallel_results_commit_separately_and_keep_emission_order() -> Result<(), Error>
{
    use rig::test_utils::MockStreamEvent as E;
    let db = Database::new()?;
    let gate = GateTool::new();
    let model = MockCompletionModel::from_stream_turns([
        vec![
            E::tool_call("slow", "gate", serde_json::json!({})),
            E::tool_call("fast", "add", serde_json::json!({"x":7,"y":12})),
            E::final_response_with_default_usage(),
        ],
        vec![E::text("done"), E::final_response_with_default_usage()],
    ]);
    let options = ToolOptions::default().policy(crate::ToolPolicy::read_only());
    let engine = Builder::new(model.clone())
        .completion_mode(CompletionMode::Streaming)
        .tool_with(gate.clone(), options.clone())
        .tool(MockAddTool)
        .build(db.clone(), Clock::default(), "s")?;
    engine.submit(SubmitInput::new("r", "question")).await?;
    {
        let driving = engine.drive();
        tokio::pin!(driving);
        assert!(futures::poll!(&mut driving).is_pending());
        let state = engine.load()?;
        let Some(Run {
            phase: Phase::Tools { keys },
            ..
        }) = state.active
        else {
            return Err(Error::Invalid("tool intents not committed".into()));
        };
        assert!(engine.call(&keys[0])?.output.is_none());
        let fast = engine.call(&keys[1])?;
        assert_eq!(fast.input.name, "add");
        assert!(
            fast.output.is_some(),
            "fast result must commit while slow call waits"
        );
        assert!(engine.result("r")?.is_none());
    }
    drop(engine);
    gate.block.store(false, std::sync::atomic::Ordering::SeqCst);
    let engine = Builder::new(model.clone())
        .completion_mode(CompletionMode::Streaming)
        .tool_with(gate.clone(), options)
        .tool(MockAddTool)
        .build(db, Clock::default(), "s")?;
    let result = engine
        .wait("r")
        .await?
        .ok_or_else(|| Error::Invalid("missing streamed result".into()))?;
    assert_eq!(result.output(), "done");
    assert_eq!(
        result
            .tool_outcomes
            .iter()
            .map(|o| o.tool_call_id.as_str())
            .collect::<Vec<_>>(),
        ["slow", "fast"]
    );
    assert_eq!(gate.count(), 2);
    assert_eq!(model.requests().len(), 2);
    assert_eq!(engine.transcript()?.len(), 4);
    Ok(())
}

#[tokio::test]
async fn unauthorized_model_calls_fail_without_retrying_the_provider() -> Result<(), Error> {
    use rig::test_utils::MockStreamEvent as E;
    for mode in [CompletionMode::Blocking, CompletionMode::Streaming] {
        let model = match mode {
            CompletionMode::Blocking => MockCompletionModel::from_turns([
                MockTurn::tool_call("unknown", "missing", serde_json::json!({})),
                MockTurn::text("must not run"),
            ]),
            CompletionMode::Streaming => MockCompletionModel::from_stream_turns([
                vec![
                    E::tool_call("unknown", "missing", serde_json::json!({})),
                    E::final_response_with_default_usage(),
                ],
                vec![
                    E::text("must not run"),
                    E::final_response_with_default_usage(),
                ],
            ]),
        };
        let engine = Builder::new(model.clone()).completion_mode(mode).build(
            Database::new()?,
            Clock::default(),
            "s",
        )?;
        engine.submit(SubmitInput::new("r", "question")).await?;
        assert!(matches!(engine.drive().await, Err(Error::Invalid(_))));
        assert!(engine.status()?.closed);
        assert!(engine.result("r")?.is_none());
        assert_eq!(model.requests().len(), 1);
    }
    Ok(())
}

struct SlowAlarmAck {
    clock: Clock,
    first: Cell<bool>,
    release: tokio::sync::Notify,
}
impl Wake for SlowAlarmAck {
    fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }
    async fn arm(&self, deadline: u64) -> Result<(), Error> {
        self.clock.arm(deadline).await?;
        if self.first.replace(false) {
            self.release.notified().await;
        }
        Ok(())
    }
    async fn delay(&self, ms: u64) {
        self.clock.delay(ms).await;
    }
}

#[tokio::test]
async fn alarm_consumed_before_admission_ack_is_replaced() -> Result<(), Error> {
    let clock = Clock::default();
    let wake = SlowAlarmAck {
        clock: clock.clone(),
        first: Cell::new(true),
        release: tokio::sync::Notify::new(),
    };
    let engine = Builder::new(MockCompletionModel::from_turns([MockTurn::text("answer")])).build(
        Database::new()?,
        wake,
        "s",
    )?;
    let admission = engine.submit(SubmitInput::new("r", "question"));
    tokio::pin!(admission);
    assert!(futures::poll!(&mut admission).is_pending());
    assert_eq!(clock.alarm.get(), Some(1000));
    clock.now.set(1000);
    clock.alarm.set(None);
    engine.alarm().await?;
    assert!(!engine.status()?.busy);
    engine.wake.release.notify_one();
    admission.await?;
    assert!(engine.status()?.busy);
    assert_eq!(clock.alarm.get(), Some(2000));
    clock.now.set(2000);
    engine.alarm().await?;
    assert_eq!(
        engine.result("r")?.map(|r| r.response.output),
        Some("answer".into())
    );
    Ok(())
}
