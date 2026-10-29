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
async fn old_schema_is_rejected_before_loading_typed_state() -> Result<(), Error> {
    let db = Database::new()?;
    db.exec(
        "CREATE TABLE rig_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        &[],
    )?;
    db.exec("INSERT INTO rig_meta VALUES ('schema', '1')", &[])?;
    // Deliberately no typed-state tables: opening must stop at the schema gate.
    let result = Builder::new(MockCompletionModel::from_turns([MockTurn::text("unused")])).build(
        db,
        Clock::default(),
        "old-session",
    );
    assert!(
        matches!(result, Err(Error::Invalid(message)) if message == "unsupported Durable Object schema version")
    );
    Ok(())
}

#[tokio::test]
async fn tools_and_deduplication_survive_reopen() -> Result<(), Error> {
    let db = Database::new()?;
    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call("add-1", "add", serde_json::json!({"x": 7, "y": 12}))
            .with_response_id("tool-response")
            .with_finish_reason(rig::completion::FinishReason::ToolCalls),
        MockTurn::text("19")
            .with_response_id("answer-response")
            .with_finish_reason(rig::completion::FinishReason::Stop),
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
    let transcript = engine.transcript()?;
    for (index, response_id, stop) in [
        (1, "tool-response", rig::message::StopReason::ToolUse),
        (3, "answer-response", rig::message::StopReason::Stop),
    ] {
        let Message::Assistant(assistant) = &transcript[index] else {
            panic!("expected persisted assistant turn");
        };
        assert_eq!(
            assistant.origin.as_ref().unwrap().response_id.as_deref(),
            Some(response_id)
        );
        assert_eq!(assistant.stop.as_ref(), Some(&stop));
    }
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
        reopened.wait("r1").await?.map(|r| r.response.output()),
        Some("19".into())
    );
    assert_eq!(model.requests().len(), 2);
    assert_eq!(reopened.transcript()?, transcript);
    assert_eq!(
        reopened.result("r1")?.unwrap().response.messages,
        response.response.messages
    );
    Ok(())
}

#[tokio::test]
async fn malformed_calls_keep_outcomes_and_valid_sibling_positions() -> Result<(), Error> {
    for valid_sibling in [false, true] {
        let mut calls = vec![rig::message::AssistantContent::ToolCall(
            rig::message::ToolCall::from_wire(
                "bad",
                rig::message::ToolFunction::new(
                    rig::message::ToolName::new("add").unwrap(),
                    serde_json::json!("not an object"),
                ),
            ),
        )];
        if valid_sibling {
            calls.push(rig::message::AssistantContent::ToolCall(
                rig::message::ToolCall::from_wire(
                    "good",
                    rig::message::ToolFunction::new(
                        rig::message::ToolName::new("add").unwrap(),
                        serde_json::json!({"x":7,"y":2}),
                    ),
                ),
            ));
        }
        let model = MockCompletionModel::from_turns([
            MockTurn::from_contents(calls),
            MockTurn::text("handled"),
        ]);
        let engine = Builder::new(model.clone()).tool(MockAddTool).build(
            Database::new()?,
            Clock::default(),
            "malformed",
        )?;
        engine.submit(SubmitInput::new("r", "add")).await?;
        let result = engine.wait("r").await?.unwrap();
        assert_eq!(result.output(), "handled");
        assert_eq!(
            result.tool_outcomes.len(),
            if valid_sibling { 2 } else { 1 }
        );
        assert_eq!(result.tool_outcomes[0].call_index, 0);
        assert_eq!(
            result.tool_outcomes[0].source,
            crate::OutcomeSource::Preresolved
        );
        if valid_sibling {
            assert_eq!(result.tool_outcomes[1].call_index, 1);
            assert_eq!(
                result.tool_outcomes[1].disposition,
                crate::ToolDisposition::Success
            );
        }
        rig::conversation::pairing::validate(&model.requests()[1].chat_history).unwrap();
    }
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
        engine.result("two")?.map(|r| r.response.output()),
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
        rig_memory::recent::LastMessages(2),
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
    assert_eq!(model.requests()[2].chat_history.len(), 3);
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
        engine.result("r")?.map(|r| r.response.output()),
        Some("answer".into())
    );
    assert_eq!(engine.transcript()?.len(), 2);
    assert_eq!(model.requests().len(), 1);
    Ok(())
}

#[cfg(feature = "codemode")]
mod scripts {
    use super::*;
    use crate::codemode::SchedulerAction;
    use rig_codemode::{
        Limits, ScriptDelivery,
        replay::{NativeReplayPlanner, ReplayError, ReplayFuture, ReplayInput, ReplayPlanner},
    };
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Clone)]
    struct Namespace {
        answer: &'static str,
        definition: ChildDefinition,
        #[allow(clippy::type_complexity)]
        engines: Rc<std::cell::RefCell<BTreeMap<String, Rc<Engine<Database, Clock>>>>>,
        requests: Rc<std::cell::RefCell<Vec<String>>>,
        lost_ack: Rc<Cell<bool>>,
    }
    impl Namespace {
        fn new(answer: &'static str) -> Result<Self, Error> {
            Ok(Self {
                answer,
                definition: Builder::new(MockCompletionModel::from_turns([]))
                    .implementation_version(answer)
                    .child_definition()?,
                engines: Rc::default(),
                requests: Rc::default(),
                lost_ack: Rc::new(Cell::new(false)),
            })
        }
        fn handle(&self) -> ChildHandle {
            ChildHandle::new(self.definition.clone(), self.clone())
        }
        async fn finish(&self) -> Result<(), Error> {
            let engines = self.engines.borrow().values().cloned().collect::<Vec<_>>();
            for engine in engines {
                engine.drive().await?;
            }
            Ok(())
        }
    }
    impl ChildTransport for Namespace {
        fn request<'a>(
            &'a self,
            request: &'a ChildRequest,
        ) -> futures::future::LocalBoxFuture<'a, Result<ChildReply, Error>> {
            Box::pin(async move {
                self.requests
                    .borrow_mut()
                    .push(serde_json::to_string(request)?);
                let id = request.child_id().to_owned();
                if !self.engines.borrow().contains_key(&id) {
                    let engine = Builder::new(MockCompletionModel::from_turns([MockTurn::text(
                        self.answer,
                    )]))
                    .implementation_version(self.answer)
                    .build(Database::new()?, Clock::default(), &id)?;
                    self.engines
                        .borrow_mut()
                        .insert(id.clone(), Rc::new(engine));
                }
                let engine = self.engines.borrow().get(&id).unwrap().clone();
                let reply = engine.handle_child_request(request.clone()).await?;
                if self.lost_ack.replace(false) {
                    return Err(Error::Storage("injected acknowledgement loss".into()));
                }
                Ok(reply)
            })
        }
    }

    fn definition(name: &str) -> rig::completion::ToolDefinition {
        let mut definition = child_tool();
        definition.name = rig::message::ToolName::new(name).unwrap();
        definition
    }
    fn parent_key(call_index: usize) -> LogicalCallKey {
        LogicalCallKey {
            logical_execution_id: "parent".into(),
            submission_id: "prompt-0".into(),
            model_turn: 1,
            call_index,
        }
    }
    fn model(code: &str) -> MockCompletionModel {
        MockCompletionModel::from_turns([
            MockTurn::tool_call("script", "codemode", serde_json::json!({"code":code})),
            MockTurn::text("final answer"),
        ])
    }
    fn builder(
        model: MockCompletionModel,
        slow: &Namespace,
        fast: &Namespace,
        limits: Limits,
        planner: impl ReplayPlanner + 'static,
    ) -> Result<Builder, Error> {
        Ok(Builder::new(model)
            .sub_agent(definition("slow"), slow.handle())?
            .sub_agent(definition("fast"), fast.handle())?
            .code_mode(limits, planner))
    }

    #[tokio::test]
    async fn deep_script_payloads_drain_and_reopen() -> Result<(), Error> {
        use rig_codemode::ExecutionStatus::{Completed, ScriptError};
        for (depth, expression, status) in [
            (124, "return deep;", None),
            (124, "await tools.fast({deep}); return 1;", None),
            (124, "await tools.deep(124); return 1;", Some(ScriptError)),
            (64, "return deep;", None),
            (63, "return deep;", Some(Completed)),
            (64, "await tools.mark({deep}); return 1;", None),
            (63, "await tools.mark({deep}); return 1;", Some(Completed)),
            (50, "await tools.deep(50); return 1;", Some(Completed)),
        ] {
            let slow = Namespace::new("slow answer")?;
            let fast = Namespace::new("fast answer")?;
            let db = Database::new()?;
            let clock = Clock::default();
            let model = model(&format!(
                "const slow=tools.slow({{}}); await tools.mark({{}}); let deep=0; for(let i=0;i<{depth};i++) deep=[deep]; {expression}"
            ));
            let parent = builder(
                model.clone(),
                &slow,
                &fast,
                Limits::default(),
                NativeReplayPlanner,
            )?
            .tool(Mark(Arc::new(AtomicBool::new(false))))
            .tool(DeepOutput)
            .build(db.clone(), clock.clone(), "parent")?;
            parent.submit(SubmitInput::new("r", "deep payload")).await?;
            parent.drive().await?;
            assert!(parent.result("r")?.is_none());
            assert!(parent.script_state(&parent_key(0))?.unwrap().is_draining());
            assert!(fast.requests.borrow().is_empty());
            assert_eq!(slow.engines.borrow().len(), 1);
            drop(parent);
            slow.finish().await?;
            let parent = builder(
                model.clone(),
                &slow,
                &fast,
                Limits::default(),
                NativeReplayPlanner,
            )?
            .tool(Mark(Arc::new(AtomicBool::new(false))))
            .tool(DeepOutput)
            .build(db, clock.clone(), "parent")?;
            clock.now.set(2000);
            parent.drive().await?;
            assert!(parent.result("r")?.is_some());
            let state = parent.script_state(&parent_key(0))?.unwrap();
            if let Some(status) = status {
                let SchedulerAction::Complete(Ok(report)) = state.action() else {
                    panic!("missing report: {:?}", state.action())
                };
                assert_eq!(report.status, status);
                if expression == "return deep;" {
                    let mut expected = serde_json::json!(0);
                    for _ in 0..depth {
                        expected = Value::Array(vec![expected]);
                    }
                    assert_eq!(report.returned, Some(expected));
                }
                assert_eq!(
                    parent.result("r")?.unwrap().tool_outcomes[0].disposition,
                    crate::ToolDisposition::Success
                );
            } else {
                assert!(
                    matches!(state.action(), SchedulerAction::Complete(Err(message)) if message.contains("storage limit"))
                );
                assert_eq!(
                    parent.result("r")?.unwrap().tool_outcomes[0].disposition,
                    crate::ToolDisposition::Error
                );
            }
            assert_eq!(slow.engines.borrow().len(), 1);
            assert!(fast.requests.borrow().is_empty());
        }
        Ok(())
    }

    #[tokio::test]
    async fn retained_script_without_depth_headroom_blocks_before_effects() -> Result<(), Error> {
        let slow = Namespace::new("slow")?;
        let fast = Namespace::new("fast")?;
        let model = model("return await tools.slow({});");
        let db = Database::new()?;
        let clock = Clock::default();
        let build = || {
            builder(
                model.clone(),
                &slow,
                &fast,
                Limits::default(),
                NativeReplayPlanner,
            )
        };
        let parent = build()?.build(db.clone(), clock.clone(), "parent")?;
        parent.submit(SubmitInput::new("r", "legacy depth")).await?;
        parent.drive().await?;
        let key = format!("script:{}", parent_key(0).canonical());
        let mut record = storage::read::<Value>(&db, &key)?.unwrap();
        let mut deep = Value::Null;
        for _ in 0..70 {
            deep = Value::Array(vec![deep]);
        }
        record["scheduler"]["active"][0]["arguments"] = deep.clone();
        record["scheduler"]["input"]["admitted"][0]["arguments"] = deep;
        storage::write(&db, &key, &record, 8 * 1024 * 1024)?;
        let requests = slow.requests.borrow().len();
        drop(parent);
        let parent = build()?.build(db.clone(), clock.clone(), "parent")?;
        clock.now.set(2000);
        assert!(matches!(parent.drive().await, Err(Error::Storage(_))));
        assert!(parent.status()?.busy);
        assert!(!parent.status()?.closed);
        assert!(parent.result("r")?.is_none());
        assert_eq!(slow.requests.borrow().len(), requests);
        assert!(fast.requests.borrow().is_empty());
        assert_eq!(storage::read::<Value>(&db, &key)?.unwrap(), record);
        Ok(())
    }

    #[tokio::test]
    async fn later_child_wins_and_loser_drains_after_restart_and_close() -> Result<(), Error> {
        let slow = Namespace::new("slow answer")?;
        let fast = Namespace::new("fast answer")?;
        fast.lost_ack.set(true);
        let db = Database::new()?;
        let clock = Clock::default();
        let model = model("return await Promise.race([tools.slow({}), tools.fast({})]);");
        let parent = builder(
            model.clone(),
            &slow,
            &fast,
            Limits::default(),
            NativeReplayPlanner,
        )?
        .build(db.clone(), clock.clone(), "parent")?;
        parent.submit(SubmitInput::new("r", "race")).await?;
        parent.drive().await?;
        assert_eq!(
            parent.script_state(&parent_key(0))?.unwrap().active().len(),
            2
        );
        assert_eq!(parent.status()?.next_deadline_ms, Some(2000));
        fast.finish().await?;
        clock.now.set(2000);
        parent.drive().await?;
        assert_eq!(fast.requests.borrow()[0], fast.requests.borrow()[1]);
        let state = parent.script_state(&parent_key(0))?.unwrap();
        assert_eq!(state.input().history[0].call.ordinal, 1);
        assert!(state.is_draining());
        assert!(parent.result("r")?.is_none());
        parent.close()?;
        drop(parent);
        slow.finish().await?;
        let parent = builder(
            model.clone(),
            &slow,
            &fast,
            Limits::default(),
            NativeReplayPlanner,
        )?
        .build(db, clock.clone(), "parent")?;
        clock.now.set(4000);
        parent.drive().await?;
        assert_eq!(parent.result("r")?.unwrap().output(), "final answer");
        let state = parent.script_state(&parent_key(0))?.unwrap();
        assert_eq!(
            state
                .input()
                .history
                .iter()
                .map(|item| item.call.ordinal)
                .collect::<Vec<_>>(),
            [1, 0]
        );
        let SchedulerAction::Complete(Ok(report)) = state.action() else {
            panic!("missing terminal report")
        };
        assert_eq!(report.returned, Some(serde_json::json!("fast answer")));
        assert_eq!(report.calls[0].delivery, ScriptDelivery::Discarded);
        assert_eq!(report.calls[1].delivery, ScriptDelivery::Delivered);
        assert_eq!(slow.engines.borrow().len(), 1);
        assert_eq!(fast.engines.borrow().len(), 1);
        assert_eq!(model.requests().len(), 2);
        Ok(())
    }

    struct PlannerFailure(Option<String>);
    impl ReplayPlanner for PlannerFailure {
        fn runtime_identity(&self) -> &str {
            NativeReplayPlanner.runtime_identity()
        }
        fn plan<'a>(&'a self, input: ReplayInput<'a>) -> ReplayFuture<'a> {
            if input.admitted.len() == 2 && input.history.is_empty() {
                Box::pin(async {
                    Err(match &self.0 {
                        Some(message) => ReplayError::Runner(message.clone()),
                        None => ReplayError::WorkerStopped,
                    })
                })
            } else {
                NativeReplayPlanner.plan(input)
            }
        }
    }

    #[tokio::test]
    async fn planner_failure_drains_queued_children_and_checkpoints_after_restart()
    -> Result<(), Error> {
        for message in [None, Some("\0".repeat(4096))] {
            let slow = Namespace::new("slow answer")?;
            let fast = Namespace::new("fast answer")?;
            let db = Database::new()?;
            let clock = Clock::default();
            let model = model("return await Promise.all([tools.slow({}),tools.fast({})]);");
            let limits = Limits {
                max_in_flight: 1,
                ..Limits::default()
            };
            let cap = if message.is_some() {
                48000
            } else {
                8 * 1024 * 1024
            };
            let parent = builder(
                model.clone(),
                &slow,
                &fast,
                limits,
                PlannerFailure(message.clone()),
            )?
            .session_history_max_bytes(cap)
            .build(db.clone(), clock.clone(), "parent")?;
            parent.submit(SubmitInput::new("r", "drain")).await?;
            parent.drive().await?;
            let state = parent.script_state(&parent_key(0))?.unwrap();
            assert_eq!(state.active().len(), 1);
            assert_eq!(state.queued().len(), 1);
            assert_eq!(fast.engines.borrow().len(), 0);
            assert!(state.failure().is_some());
            drop(parent);
            slow.finish().await?;
            let parent = builder(
                model.clone(),
                &slow,
                &fast,
                limits,
                PlannerFailure(message.clone()),
            )?
            .session_history_max_bytes(cap)
            .build(db.clone(), clock.clone(), "parent")?;
            clock.now.set(2000);
            parent.drive().await?;
            assert_eq!(fast.engines.borrow().len(), 1);
            assert!(parent.result("r")?.is_none());
            drop(parent);
            fast.finish().await?;
            let parent = builder(
                model.clone(),
                &slow,
                &fast,
                limits,
                PlannerFailure(message.clone()),
            )?
            .session_history_max_bytes(cap)
            .build(db, clock.clone(), "parent")?;
            clock.now.set(4000);
            parent.drive().await?;
            let state = parent.script_state(&parent_key(0))?.unwrap();
            assert_eq!(state.input().history.len(), 2);
            let SchedulerAction::Complete(Err(error)) = state.action() else {
                panic!("failure did not settle")
            };
            if message.is_some() {
                assert!(error.contains("storage limit"));
            } else {
                assert_eq!(error, "script worker stopped without an outcome");
            }
            assert_eq!(
                parent.result("r")?.unwrap().tool_outcomes[0].disposition,
                crate::ToolDisposition::Error
            );
            assert_eq!(model.requests().len(), 2);
        }
        Ok(())
    }

    struct DriftPlanner;
    impl ReplayPlanner for DriftPlanner {
        fn runtime_identity(&self) -> &str {
            "other-runtime"
        }
        fn plan<'a>(&'a self, _: ReplayInput<'a>) -> ReplayFuture<'a> {
            Box::pin(async { panic!("drift must fail before replay") })
        }
    }

    #[tokio::test]
    async fn runtime_catalog_policy_and_limit_drift_preserve_and_resume_drain() -> Result<(), Error>
    {
        for drift in 0..4 {
            let slow = Namespace::new("slow answer")?;
            let fast = Namespace::new("fast answer")?;
            let db = Database::new()?;
            let clock = Clock::default();
            let model = model("return await Promise.race([tools.slow({}), tools.fast({})]);");
            let parent = builder(
                model.clone(),
                &slow,
                &fast,
                Limits::default(),
                NativeReplayPlanner,
            )?
            .build(db.clone(), clock.clone(), "parent")?;
            parent.submit(SubmitInput::new("r", "drift")).await?;
            parent.drive().await?;
            fast.finish().await?;
            clock.now.set(2000);
            parent.drive().await?;
            assert!(parent.script_state(&parent_key(0))?.unwrap().is_draining());
            let status = serde_json::to_value(parent.status()?)?;
            let state = serde_json::to_value(parent.script_state(&parent_key(0))?)?;
            let before = fast.requests.borrow().len();
            let slow_before = slow.requests.borrow().len();
            drop(parent);
            let mut next = builder(
                model.clone(),
                &slow,
                &fast,
                Limits::default(),
                NativeReplayPlanner,
            )?;
            match drift {
                0 => next = next.code_mode(Limits::default(), DriftPlanner),
                1 => next
                    .config
                    .tools
                    .0
                    .get_mut("fast")
                    .unwrap()
                    .definition
                    .description
                    .push_str(" changed"),
                2 => {
                    next.config.tools.0.get_mut("fast").unwrap().policy =
                        crate::ToolPolicy::read_only().implementation_version("upgraded")
                }
                _ => {
                    next = next.code_mode(
                        Limits {
                            max_in_flight: 1,
                            ..Limits::default()
                        },
                        NativeReplayPlanner,
                    )
                }
            }
            let parent = next.build(db.clone(), clock.clone(), "parent")?;
            clock.now.set(4000);
            assert!(matches!(parent.alarm().await, Err(Error::Incompatible(_))));
            assert_eq!(serde_json::to_value(parent.status()?)?, status);
            assert_eq!(
                serde_json::to_value(parent.script_state(&parent_key(0))?)?,
                state
            );
            assert!(parent.result("r")?.is_none());
            assert_eq!(fast.requests.borrow().len(), before);
            assert_eq!(slow.requests.borrow().len(), slow_before);
            assert_eq!(model.requests().len(), 1);
            drop(parent);
            let parent = builder(
                model.clone(),
                &slow,
                &fast,
                Limits::default(),
                NativeReplayPlanner,
            )?
            .build(db, clock.clone(), "parent")?;
            parent.drive().await?;
            assert!(parent.status()?.busy);
            assert!(parent.result("r")?.is_none());
            assert_eq!(slow.engines.borrow().len(), 1);
            slow.finish().await?;
            clock.now.set(6000);
            parent.drive().await?;
            assert_eq!(parent.result("r")?.unwrap().output(), "final answer");
            let state = parent.script_state(&parent_key(0))?.unwrap();
            let SchedulerAction::Complete(Ok(report)) = state.action() else {
                panic!("drain did not settle")
            };
            assert_eq!(report.returned, Some(serde_json::json!("fast answer")));
            assert_eq!(
                state
                    .input()
                    .history
                    .iter()
                    .map(|item| item.call.ordinal)
                    .collect::<Vec<_>>(),
                [1, 0]
            );
            assert_eq!(slow.engines.borrow().len(), 1);
            assert_eq!(fast.engines.borrow().len(), 1);
            assert_eq!(model.requests().len(), 2);
        }
        Ok(())
    }

    #[tokio::test]
    async fn identical_ordinals_in_two_outer_scripts_admit_distinct_child_objects()
    -> Result<(), Error> {
        let slow = Namespace::new("slow answer")?;
        let fast = Namespace::new("fast answer")?;
        let script = serde_json::json!({"code":"return await tools.fast({});"});
        let model = MockCompletionModel::from_turns([
            MockTurn::from_contents(["script-a", "script-b"].map(|id| {
                rig::message::AssistantContent::ToolCall(rig::message::ToolCall::from_wire(
                    id,
                    rig::message::ToolFunction::new(
                        rig::message::ToolName::new("codemode").unwrap(),
                        script.clone(),
                    ),
                ))
            })),
            MockTurn::text("two answers"),
        ]);
        let clock = ManualWake::default();
        let parent = builder(
            model.clone(),
            &slow,
            &fast,
            Limits::default(),
            NativeReplayPlanner,
        )?
        .build(Database::new()?, clock.clone(), "parent")?;
        parent.submit(SubmitInput::new("r", "two scripts")).await?;
        parent.drive().await?;
        assert_eq!(fast.engines.borrow().len(), 2);
        assert_ne!(fast.requests.borrow()[0], fast.requests.borrow()[1]);
        fast.finish().await?;
        clock.advance(2000);
        assert_eq!(parent.wait("r").await?.unwrap().output(), "two answers");
        for index in 0..2 {
            assert_eq!(
                parent
                    .script_state(&parent_key(index))?
                    .unwrap()
                    .input()
                    .history[0]
                    .call
                    .ordinal,
                0
            );
        }
        Ok(())
    }

    #[derive(Clone, Default)]
    struct ManualWake {
        clock: Clock,
        changed: Rc<tokio::sync::Notify>,
    }
    impl ManualWake {
        fn advance(&self, ms: u64) {
            self.clock.now.set(ms);
            self.changed.notify_one();
        }
    }
    impl Wake for ManualWake {
        fn now_ms(&self) -> u64 {
            self.clock.now_ms()
        }
        async fn arm(&self, deadline: u64) -> Result<(), Error> {
            self.clock.arm(deadline).await
        }
        async fn delay(&self, ms: u64) {
            let deadline = self.now_ms().saturating_add(ms);
            while self.now_ms() < deadline {
                self.changed.notified().await;
            }
        }
    }

    #[derive(Clone)]
    struct Mark(Arc<AtomicBool>);
    impl Tool for Mark {
        const NAME: &'static str = "mark";
        type Args = Value;
        type Output = String;
        type Error = std::convert::Infallible;
        fn description(&self) -> String {
            "Record branch progress".into()
        }
        fn parameters(&self) -> Value {
            serde_json::json!({"type":"object"})
        }
        async fn call(
            &self,
            _: &mut rig::tool::ToolContext,
            _: Value,
        ) -> Result<String, Self::Error> {
            self.0.store(true, Ordering::SeqCst);
            Ok("branch advanced".into())
        }
    }

    #[derive(Clone)]
    struct DeepOutput;
    impl Tool for DeepOutput {
        const NAME: &'static str = "deep";
        type Args = usize;
        type Output = rig::tool::ToolOutput;
        type Error = std::convert::Infallible;
        fn description(&self) -> String {
            "Deep JSON".into()
        }
        fn parameters(&self) -> Value {
            serde_json::json!({"type":"integer"})
        }
        async fn call(
            &self,
            _: &mut rig::tool::ToolContext,
            depth: usize,
        ) -> Result<Self::Output, Self::Error> {
            let mut value = Value::Null;
            for _ in 0..depth {
                value = Value::Array(vec![value]);
            }
            Ok(rig::tool::ToolOutput::json(value))
        }
    }

    #[derive(Clone)]
    struct SizedOutput(Arc<std::sync::atomic::AtomicUsize>);
    impl Tool for SizedOutput {
        const NAME: &'static str = "sized";
        type Args = usize;
        type Output = String;
        type Error = std::convert::Infallible;
        fn description(&self) -> String {
            "Produce the requested number of bytes".into()
        }
        fn parameters(&self) -> Value {
            serde_json::json!({"type":"integer"})
        }
        async fn call(
            &self,
            _: &mut rig::tool::ToolContext,
            bytes: usize,
        ) -> Result<String, Self::Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok("x".repeat(bytes))
        }
    }

    #[tokio::test]
    async fn oversized_nested_result_commits_failure_and_retains_sibling_drain() -> Result<(), Error>
    {
        for (history_limit, caught) in [(true, true), (false, true), (true, false), (false, false)]
        {
            let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let gate = GateTool::new();
            let expression = if history_limit {
                "await Promise.all(Array.from({length:10},()=>tools.sized(15000)))"
            } else {
                "await tools.sized(1200000)"
            };
            let code = if caught {
                format!(
                    "const slow=tools.gate({{}}); try {{return {expression};}} catch(error) {{return error.message;}}"
                )
            } else {
                format!("const slow=tools.gate({{}}); return {expression};")
            };
            let expected_count = if history_limit { 10 } else { 1 };
            let model = model(&code);
            let db = Database::new()?;
            let parent = Builder::new(model.clone())
                .tool_with(
                    gate.clone(),
                    ToolOptions::default().policy(crate::ToolPolicy::read_only()),
                )
                .tool_with(
                    SizedOutput(count.clone()),
                    ToolOptions::default().policy(crate::ToolPolicy::read_only()),
                )
                .session_history_max_bytes(if history_limit {
                    32768
                } else {
                    8 * 1024 * 1024
                })
                .code_mode(
                    Limits {
                        max_in_flight: 3,
                        ..Limits::default()
                    },
                    NativeReplayPlanner,
                )
                .build(db.clone(), ManualWake::default(), "parent")?;
            parent
                .submit(SubmitInput::new("r", "large nested result"))
                .await?;
            let mut drive = Box::pin(parent.drive());
            tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                tokio::select! {
                    result = &mut drive => panic!("drive abandoned sibling before drain: {result:?}; status={:?}; effects={}, sibling={}", parent.status(), count.load(Ordering::SeqCst), gate.count()),
                    _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {},
                }
                if parent.script_state(&parent_key(0))?.is_some_and(|s| s.is_draining()) { break; }
            }
            Ok::<_, Error>(())
        }).await.unwrap()?;
            assert!(parent.result("r")?.is_none());
            assert_eq!(count.load(Ordering::SeqCst), expected_count);
            assert_eq!(gate.count(), 1);
            gate.release.notify_one();
            tokio::time::timeout(std::time::Duration::from_secs(3), drive)
                .await
                .unwrap()?;
            assert_eq!(count.load(Ordering::SeqCst), expected_count);
            assert_eq!(gate.count(), 1);
            let state = parent.script_state(&parent_key(0))?.unwrap();
            let SchedulerAction::Complete(Ok(report)) = state.action() else {
                panic!("missing terminal report")
            };
            assert!(
                report
                    .calls
                    .iter()
                    .any(|call| call.status == rig_codemode::CallStatus::Failed)
            );
            if caught {
                assert_eq!(report.status, rig_codemode::ExecutionStatus::Completed);
                assert!(
                    report
                        .returned
                        .as_ref()
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .contains("storage limit")
                );
            } else {
                assert_eq!(report.status, rig_codemode::ExecutionStatus::ScriptError);
                assert!(
                    report
                        .diagnostic
                        .as_ref()
                        .unwrap()
                        .message
                        .contains("storage limit")
                );
            }
            assert_eq!(
                parent.result("r")?.unwrap().tool_outcomes[0].disposition,
                crate::ToolDisposition::Success
            );
            let chunks = db.exec(
                "SELECT data FROM rig_runs WHERE key = ?",
                &[format!("script:{}", parent_key(0).canonical())],
            )?;
            assert!(
                chunks
                    .iter()
                    .map(|row| storage::text(row).unwrap().len())
                    .sum::<usize>()
                    <= if history_limit {
                        32768
                    } else {
                        8 * 1024 * 1024
                    }
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_boundary_keeps_live_effects_across_result_delivery() -> Result<(), Error> {
        for bytes in [29184usize, 29185, 30000] {
            let gate = GateTool::new();
            let code = format!(
                "await tools.mark({{}}); const slow=tools.gate({{}}); await tools.sized({bytes}); return await slow;"
            );
            let parent = Builder::new(model(&code))
                .tool_with(
                    gate.clone(),
                    ToolOptions::default().policy(crate::ToolPolicy::read_only()),
                )
                .tool(Mark(Arc::new(AtomicBool::new(false))))
                .tool_with(
                    SizedOutput(Arc::new(std::sync::atomic::AtomicUsize::new(0))),
                    ToolOptions::default().policy(crate::ToolPolicy::read_only()),
                )
                .session_history_max_bytes(32768)
                .code_mode(Limits::default(), NativeReplayPlanner)
                .build(Database::new()?, ManualWake::default(), "parent")?;
            parent.submit(SubmitInput::new("r", "boundary")).await?;
            let mut drive = Box::pin(parent.drive());
            let observed: Result<bool, Error> = tokio::time::timeout(std::time::Duration::from_secs(3), async {
                loop {
                    tokio::select! {
                        result = &mut drive => panic!("drive lost the boundary sibling: {result:?}"),
                        _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {},
                    }
                    if let Some(state) = parent.script_state(&parent_key(0))?
                        && let Some(value) = state.input().history.iter().find(|h| h.call.ordinal == 2) { return Ok(value.result.is_success()); }
                }
            }).await.unwrap();
            assert_eq!(observed?, bytes < 30000);
            assert!(parent.result("r")?.is_none());
            assert_eq!(gate.count(), 1);
            gate.release.notify_one();
            drive.await?;
            assert_eq!(gate.count(), 1);
            assert!(parent.result("r")?.is_some());
            assert!(!parent.status()?.busy);
        }
        Ok(())
    }

    #[tokio::test]
    async fn unpersistable_plans_drain_live_effects_and_bound_terminal_reports() -> Result<(), Error>
    {
        for new_call in [true, false] {
            let gate = GateTool::new();
            let mark = Mark(Arc::new(AtomicBool::new(false)));
            let code = if new_call {
                "const slow=tools.gate({}); await tools.mark({}); await tools.gate({pad:'x'.repeat(600000)}); return await slow;"
            } else {
                "const slow=tools.gate({}); await tools.mark({}); await slow; return 'x'.repeat(600000);"
            };
            let model = model(code);
            let parent = Builder::new(model.clone())
                .tool(gate.clone())
                .tool(mark.clone())
                .session_history_max_bytes(32768)
                .code_mode(Limits::default(), NativeReplayPlanner)
                .build(Database::new()?, ManualWake::default(), "parent")?;
            parent
                .submit(SubmitInput::new("r", "storage admission"))
                .await?;
            let mut drive = Box::pin(parent.drive());
            tokio::time::timeout(std::time::Duration::from_secs(3), async {
                loop {
                    tokio::select! {
                        result = &mut drive => panic!("effect dropped before drain: {result:?}"),
                        _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {},
                    }
                    if parent.script_state(&parent_key(0))?.is_some_and(|s| {
                        if new_call {
                            s.failure().is_some()
                        } else {
                            !s.input().history.is_empty()
                        }
                    }) {
                        break;
                    }
                }
                Ok::<_, Error>(())
            })
            .await
            .unwrap()?;
            assert!(parent.result("r")?.is_none());
            assert!(mark.0.load(Ordering::SeqCst));
            assert_eq!(gate.count(), 1);
            gate.release.notify_one();
            tokio::time::timeout(std::time::Duration::from_secs(3), drive)
                .await
                .unwrap()?;
            let state = parent.script_state(&parent_key(0))?.unwrap();
            let SchedulerAction::Complete(Err(error)) = state.action() else {
                panic!("missing bounded planning failure")
            };
            assert!(error.contains("storage limit"));
            assert_eq!(state.input().admitted.len(), 2);
            assert_eq!(state.input().history.len(), 2);
            assert_eq!(gate.count(), 1);
            assert_eq!(
                parent.result("r")?.unwrap().tool_outcomes[0].disposition,
                crate::ToolDisposition::Error
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn near_full_outer_script_intent_does_not_abandon_sibling() -> Result<(), Error> {
        for (padding, retained) in [
            (523268, false),
            (523397, false),
            (523818, false),
            (523818, true),
        ] {
            let slow = Namespace::new("slow answer")?;
            let fast = Namespace::new("fast answer")?;
            let returned = if padding == 523268 {
                "'x'.repeat(10000)"
            } else if padding == 523397 {
                "await tools.fast({})"
            } else {
                "'normal'"
            };
            let padded = format!("/*{}*/ return {returned};", " ".repeat(padding));
            let model = MockCompletionModel::from_turns([
                MockTurn::from_contents(
                    [
                        ("slow", "return await tools.slow({});"),
                        ("large", padded.as_str()),
                    ]
                    .map(|(id, code)| {
                        rig::message::AssistantContent::ToolCall(rig::message::ToolCall::from_wire(
                            id,
                            rig::message::ToolFunction::new(
                                rig::message::ToolName::new("codemode").unwrap(),
                                serde_json::json!({"code":code}),
                            ),
                        ))
                    }),
                ),
                MockTurn::text("both settled"),
            ]);
            let limits = Limits {
                source_bytes: 1024 * 1024,
                ..Limits::default()
            };
            let db = Database::new()?;
            let clock = Clock::default();
            if padding == 523397 {
                clock.now.set(1_700_000_000_000);
            }
            let mut parent = builder(model.clone(), &slow, &fast, limits, NativeReplayPlanner)?
                .session_history_max_bytes(32 * 1024 * 1024)
                .build(db.clone(), clock.clone(), "parent")?;
            parent
                .submit(SubmitInput::new("r", "near-full outer"))
                .await?;
            let result = parent.drive().await;
            if padding == 523397 {
                result?;
                assert!(parent.status()?.busy);
                assert_eq!(fast.engines.borrow().len(), 1);
                fast.finish().await?;
                slow.finish().await?;
                drop(parent);
                let parent = builder(model.clone(), &slow, &fast, limits, NativeReplayPlanner)?
                    .session_history_max_bytes(32 * 1024 * 1024)
                    .build(db, clock.clone(), "parent")?;
                clock.now.set(clock.now.get() + 2000);
                parent.drive().await?;
                assert_eq!(parent.result("r")?.unwrap().output(), "both settled");
                assert_eq!(fast.engines.borrow().len(), 1);
                assert_eq!(slow.engines.borrow().len(), 1);
                assert_eq!(model.requests().len(), 2);
                continue;
            }
            if padding == 523268 {
                result?;
                assert!(parent.status()?.busy);
                assert!(parent.result("r")?.is_none());
                assert_eq!(slow.engines.borrow().len(), 1);
                slow.finish().await?;
                clock.now.set(2000);
                parent.drive().await?;
                let result = parent.result("r")?.unwrap();
                assert_eq!(result.output(), "both settled");
                assert_eq!(
                    result.tool_outcomes[0].disposition,
                    crate::ToolDisposition::Success
                );
                assert_eq!(
                    result.tool_outcomes[1].disposition,
                    crate::ToolDisposition::Error
                );
                let rows = db.exec(
                    "SELECT length(CAST(data AS BLOB)) AS bytes FROM rig_tool_calls",
                    &[],
                )?;
                assert!(
                    rows.iter()
                        .all(|row| row["bytes"].as_u64().unwrap() <= storage::ROW_BYTES as u64)
                );
                assert_eq!(model.requests().len(), 2);
                continue;
            }
            assert!(
                matches!(result, Err(Error::Storage(message)) if message.contains("fixed 1048576-byte tool-call row limit"))
            );
            assert!(parent.status()?.busy);
            assert!(!parent.status()?.closed);
            assert!(parent.result("r")?.is_none());
            assert!(slow.engines.borrow().is_empty());
            assert!(db.exec("SELECT key FROM rig_tool_calls", &[])?.is_empty());
            if retained {
                // Seed legacy admission without terminal-output reservation.
                let runtime = parent.script.take();
                parent.advance(&parent.load()?, &parent.config)?;
                parent.script = runtime;
                let call = parent.call(&parent_key(0).canonical())?;
                parent.script(&call).await?;
                let large = parent.call(&parent_key(1).canonical())?;
                let bytes = serde_json::to_vec(&large)?.len();
                assert!(bytes <= storage::ROW_BYTES && bytes > storage::ROW_BYTES - 100);
                assert_eq!(slow.engines.borrow().len(), 1);
            }
            let status = serde_json::to_value(parent.status()?)?;
            let state = serde_json::to_value(parent.script_state(&parent_key(0))?)?;
            let requests = slow.requests.borrow().clone();
            drop(parent);
            let parent = builder(model.clone(), &slow, &fast, limits, NativeReplayPlanner)?
                .session_history_max_bytes(64 * 1024 * 1024)
                .build(db, clock, "parent")?;
            assert!(matches!(parent.drive().await, Err(Error::Storage(_))));
            assert!(parent.status()?.busy);
            assert!(!parent.status()?.closed);
            assert_eq!(serde_json::to_value(parent.status()?)?, status);
            assert_eq!(
                serde_json::to_value(parent.script_state(&parent_key(0))?)?,
                state
            );
            assert_eq!(*slow.requests.borrow(), requests);
            assert_eq!(slow.engines.borrow().len(), usize::from(retained));
            assert_eq!(model.requests().len(), 1);
        }
        Ok(())
    }

    #[tokio::test]
    async fn child_poll_deadline_advances_branch_while_local_tool_is_blocked() -> Result<(), Error>
    {
        for separate_scripts in [false, true] {
            let fast = Namespace::new("fast answer")?;
            let gate = GateTool::new();
            let mark = Mark(Arc::new(AtomicBool::new(false)));
            let model = if separate_scripts {
                MockCompletionModel::from_turns([
                    MockTurn::from_contents(
                        [
                            ("fast", "await tools.fast({}); return await tools.mark({});"),
                            ("slow", "return await tools.gate({});"),
                        ]
                        .map(|(id, code)| {
                            rig::message::AssistantContent::ToolCall(
                                rig::message::ToolCall::from_wire(
                                    id,
                                    rig::message::ToolFunction::new(
                                        rig::message::ToolName::new("codemode").unwrap(),
                                        serde_json::json!({"code":code}),
                                    ),
                                ),
                            )
                        }),
                    ),
                    MockTurn::text("final answer"),
                ])
            } else {
                model(
                    "const slow=tools.gate({}); await tools.fast({}); await tools.mark({}); return await slow;",
                )
            };
            let wake = ManualWake::default();
            let parent = Builder::new(model.clone())
                .tool_with(
                    gate.clone(),
                    ToolOptions::default().policy(crate::ToolPolicy::read_only()),
                )
                .tool(mark.clone())
                .sub_agent(definition("fast"), fast.handle())?
                .code_mode(Limits::default(), NativeReplayPlanner)
                .build(Database::new()?, wake.clone(), "parent")?;
            parent.submit(SubmitInput::new("r", "progress")).await?;
            let mut drive = Box::pin(parent.drive());
            tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                tokio::select! {
                    result = &mut drive => panic!("drive ended before fast child polling: {result:?}"),
                    _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {},
                }
                if parent.status()?.next_deadline_ms == Some(2000) && gate.count() == 1 { break; }
            }
            Ok::<_, Error>(())
        }).await.unwrap()?;
            assert_eq!(gate.count(), 1);
            fast.finish().await?;
            wake.advance(2000);
            tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while !mark.0.load(Ordering::SeqCst) {
                tokio::select! {
                    result = &mut drive => panic!("drive ended before slow release: {result:?}"),
                    _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {},
                }
            }
        })
        .await
        .unwrap();
            assert!(parent.result("r")?.is_none());
            assert_eq!(
                parent
                    .script_state(&parent_key(0))?
                    .unwrap()
                    .input()
                    .history[0]
                    .call
                    .ordinal,
                u32::from(!separate_scripts)
            );
            gate.release.notify_one();
            tokio::time::timeout(std::time::Duration::from_secs(3), drive)
                .await
                .unwrap()?;
            assert_eq!(parent.result("r")?.unwrap().output(), "final answer");
        }
        Ok(())
    }

    #[derive(Clone)]
    struct RetryGate(GateTool);
    impl Tool for RetryGate {
        const NAME: &'static str = "retry";
        type Args = Value;
        type Output = String;
        type Error = rig::tool::ToolExecutionError;
        fn description(&self) -> String {
            "Retry then wait for a simultaneous release".into()
        }
        fn parameters(&self) -> Value {
            serde_json::json!({"type":"object"})
        }
        async fn call(
            &self,
            _: &mut rig::tool::ToolContext,
            _: Value,
        ) -> Result<String, Self::Error> {
            if self.0.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(rig::tool::ToolExecutionError::network("retry once"));
            }
            self.0.entered.notify_one();
            self.0.release.notified().await;
            Ok("retried old call".into())
        }
    }

    #[tokio::test]
    async fn old_retry_ties_follow_admission_order_not_future_insertion_order() -> Result<(), Error>
    {
        let old = RetryGate(GateTool::new());
        let newer = GateTool::new();
        let wake = ManualWake::default();
        let model = model("return await Promise.race([tools.retry({}),tools.gate({})]);");
        let parent = Builder::new(model)
            .tool_with(
                old.clone(),
                ToolOptions::default()
                    .policy(crate::ToolPolicy::read_only())
                    .retry(RetryPolicy::new(2).with_backoff(
                        crate::retry::BackoffStrategy::Fixed {
                            delay: std::time::Duration::from_millis(2000),
                        },
                    )),
            )
            .tool_with(
                newer.clone(),
                ToolOptions::default().policy(crate::ToolPolicy::read_only()),
            )
            .code_mode(Limits::default(), NativeReplayPlanner)
            .build(Database::new()?, wake.clone(), "parent")?;
        parent.submit(SubmitInput::new("r", "tie")).await?;
        let mut drive = Box::pin(parent.drive());
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                tokio::select! { result = &mut drive => panic!("early completion {result:?}"), _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {} }
                if parent.status()?.next_deadline_ms == Some(2000) { break; }
            }
            Ok::<_, Error>(())
        }).await.unwrap()?;
        wake.advance(2000);
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::select! { result = &mut drive => panic!("early completion {result:?}"), _ = old.0.entered.notified() => {} }
        }).await.unwrap();
        old.0.release.notify_one();
        newer.release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(3), drive)
            .await
            .unwrap()?;
        let state = parent.script_state(&parent_key(0))?.unwrap();
        assert_eq!(
            state
                .input()
                .history
                .iter()
                .map(|item| item.call.ordinal)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        let SchedulerAction::Complete(Ok(report)) = state.action() else {
            panic!("missing report")
        };
        assert_eq!(report.returned, Some(serde_json::json!("retried old call")));
        assert_eq!(old.0.count(), 2);
        assert_eq!(newer.count(), 1);
        Ok(())
    }

    struct PausePlanner {
        history_len: usize,
        paused: Arc<AtomicBool>,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }
    impl ReplayPlanner for PausePlanner {
        fn runtime_identity(&self) -> &str {
            NativeReplayPlanner.runtime_identity()
        }
        fn plan<'a>(&'a self, input: ReplayInput<'a>) -> ReplayFuture<'a> {
            Box::pin(async move {
                if input.history.len() == self.history_len
                    && !self.paused.swap(true, Ordering::SeqCst)
                {
                    self.entered.notify_one();
                    self.release.notified().await;
                }
                NativeReplayPlanner.plan(input).await
            })
        }
    }

    #[tokio::test]
    async fn free_slot_is_reserved_before_second_retained_ready_delivery() -> Result<(), Error> {
        let gate = GateTool::new();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let model = model(
            "return await Promise.all([tools.add({x:1,y:2}),tools.add({x:7,y:11}),tools.gate({}),tools.add({x:13,y:19})]);",
        );
        let parent = Builder::new(model)
            .tool(MockAddTool)
            .tool(gate.clone())
            .code_mode(
                Limits {
                    max_in_flight: 3,
                    ..Limits::default()
                },
                PausePlanner {
                    history_len: 2,
                    paused: Arc::new(AtomicBool::new(false)),
                    entered: entered.clone(),
                    release: release.clone(),
                },
            )
            .build(Database::new()?, Clock::default(), "parent")?;
        parent.submit(SubmitInput::new("r", "queued")).await?;
        let mut drive = Box::pin(parent.drive());
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::select! { result = &mut drive => panic!("early completion {result:?}"), _ = entered.notified() => {} }
        }).await.unwrap();
        let state = parent.script_state(&parent_key(0))?.unwrap();
        assert!(state.queued().is_empty());
        assert_eq!(
            state
                .active()
                .iter()
                .map(|call| call.ordinal)
                .collect::<Vec<_>>(),
            [2, 3]
        );
        release.notify_one();
        gate.release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(3), drive)
            .await
            .unwrap()?;
        let state = parent.script_state(&parent_key(0))?.unwrap();
        let SchedulerAction::Complete(Ok(report)) = state.action() else {
            panic!("missing report")
        };
        assert_eq!(
            report.returned,
            Some(serde_json::json!([
                3,
                18,
                "effect committed externally",
                32
            ]))
        );
        Ok(())
    }

    #[tokio::test]
    async fn schema_rejection_chain_races_valid_child_and_drains_across_reopen() -> Result<(), Error>
    {
        let fast = Namespace::new("child answer")?;
        let gate = GateTool::new();
        let code = "const bad=tools.gate('invalid').catch(()=>Promise.resolve().then(()=>0).then(()=>1).then(()=>2).then(()=>3).then(()=>4).then(()=>'schema branch')); return await Promise.race([bad,tools.fast({})]);";
        let model = model(code);
        let db = Database::new()?;
        let clock = Clock::default();
        let create = || {
            Builder::new(model.clone())
                .tool(gate.clone())
                .sub_agent(definition("fast"), fast.handle())
                .map(|builder| builder.code_mode(Limits::default(), NativeReplayPlanner))
        };
        let parent = create()?.build(db.clone(), clock.clone(), "parent")?;
        parent.submit(SubmitInput::new("r", "schema race")).await?;
        parent.drive().await?;
        assert_eq!(gate.count(), 0);
        assert_eq!(fast.engines.borrow().len(), 1);
        assert!(parent.script_state(&parent_key(0))?.unwrap().is_draining());
        drop(parent);
        fast.finish().await?;
        clock.now.set(2000);
        let parent = create()?.build(db, clock, "parent")?;
        parent.drive().await?;
        let state = parent.script_state(&parent_key(0))?.unwrap();
        let SchedulerAction::Complete(Ok(report)) = state.action() else {
            panic!("missing schema report")
        };
        assert_eq!(report.returned, Some(serde_json::json!("schema branch")));
        assert_eq!(report.calls[1].delivery, ScriptDelivery::Discarded);
        assert_eq!(gate.count(), 0);
        assert!(parent.result("r")?.is_some());
        Ok(())
    }

    #[derive(Clone, Copy)]
    enum Window {
        Admission,
        EffectResult,
        Delivery,
    }
    #[derive(Clone)]
    struct CrashSql {
        db: Database,
        window: Window,
        armed: Rc<Cell<bool>>,
    }
    impl SqlDatabase for CrashSql {
        fn exec(
            &self,
            query: &str,
            params: &[String],
        ) -> Result<Vec<BTreeMap<String, Value>>, Error> {
            if self.armed.get() && query.starts_with("INSERT INTO rig_script_effects") {
                let effect: Value = serde_json::from_str(&params[1])?;
                if matches!(self.window, Window::Admission) && effect["started"] == false {
                    self.armed.set(false);
                    self.db.1.set(true);
                } else if matches!(self.window, Window::EffectResult) && !effect["output"].is_null()
                {
                    self.armed.set(false);
                    return Err(Error::Storage(
                        "injected crash after effect before result commit".into(),
                    ));
                }
            }
            if self.armed.get()
                && matches!(self.window, Window::Delivery)
                && query.starts_with("INSERT INTO rig_runs")
                && params[0].starts_with("script:")
            {
                let record: Value = serde_json::from_str(&params[2])?;
                if record["scheduler"]["input"]["history"]
                    .as_array()
                    .is_some_and(|items| !items.is_empty())
                {
                    self.armed.set(false);
                    self.db.1.set(true);
                }
            }
            self.db.exec(query, params)
        }
        fn transaction<T>(&self, f: impl FnOnce() -> Result<T, Error>) -> Result<T, Error> {
            self.db.transaction(f)
        }
    }

    #[tokio::test]
    async fn committed_admission_and_delivery_recover_and_uncertain_effect_never_repeats()
    -> Result<(), Error> {
        for (window, repeatable, expected_count) in [
            (Window::Admission, false, 1),
            (Window::EffectResult, false, 1),
            (Window::EffectResult, true, 2),
            (Window::Delivery, false, 1),
        ] {
            let gate = GateTool::new();
            gate.block.store(false, Ordering::SeqCst);
            let model = model(
                "try {return await tools.gate({pad:'x'.repeat(1000)});} catch(error) {return error.message;}",
            );
            let db = CrashSql {
                db: Database::new()?,
                window,
                armed: Rc::new(Cell::new(true)),
            };
            let policy = if repeatable {
                crate::ToolPolicy::read_only()
            } else {
                crate::ToolPolicy::interrupt_on_uncertain()
            };
            let create = || {
                Builder::new(model.clone())
                    .tool_with(gate.clone(), ToolOptions::default().policy(policy.clone()))
                    .session_history_max_bytes(16384)
                    .code_mode(Limits::default(), NativeReplayPlanner)
            };
            let parent = create().build(db.clone(), Clock::default(), "parent")?;
            parent.submit(SubmitInput::new("r", "crash")).await?;
            assert!(matches!(parent.drive().await, Err(Error::Storage(_))));
            assert!(!db.armed.get());
            assert!(parent.result("r")?.is_none());
            if matches!(window, Window::Admission) {
                assert_eq!(gate.count(), 0);
            }
            drop(parent);
            if matches!(window, Window::Admission) {
                let blocked = create().session_history_max_bytes(4096).build(
                    db.clone(),
                    Clock::default(),
                    "parent",
                )?;
                assert!(matches!(blocked.drive().await, Err(Error::Storage(_))));
                assert_eq!(gate.count(), 0);
                assert!(blocked.status()?.busy);
                assert!(!blocked.status()?.closed);
            }
            let parent = create().build(db.clone(), Clock::default(), "parent")?;
            parent.drive().await?;
            assert_eq!(gate.count(), expected_count);
            let state = parent.script_state(&parent_key(0))?.unwrap();
            assert_eq!(state.input().admitted.len(), 1);
            assert_eq!(state.input().history.len(), 1);
            let SchedulerAction::Complete(Ok(report)) = state.action() else {
                panic!("missing recovered report")
            };
            if matches!(window, Window::EffectResult) && !repeatable {
                assert_eq!(report.calls[0].status, rig_codemode::CallStatus::Failed);
                let rows = db.exec("SELECT data FROM rig_script_effects", &[])?;
                let effect: Value = serde_json::from_str(storage::text(&rows[0])?)?;
                let durable: DurableToolResult =
                    serde_json::from_value(effect["output"]["result"].clone())?;
                assert_eq!(durable.disposition, crate::ToolDisposition::Interrupted);
                assert_eq!(
                    durable.interruption.unwrap().reason,
                    InterruptionReason::ClaimHeld
                );
                assert!(durable.result.is_none());
                assert!(
                    report
                        .returned
                        .as_ref()
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .contains("may have happened")
                );
            } else {
                assert_eq!(
                    report.returned,
                    Some(serde_json::json!("effect committed externally"))
                );
            }
            assert!(parent.result("r")?.is_some());
        }
        Ok(())
    }

    #[tokio::test]
    async fn malformed_outer_arguments_commit_error_without_abandoning_sibling_script()
    -> Result<(), Error> {
        for invalid in [serde_json::json!({}), serde_json::json!({"code":42})] {
            let slow = Namespace::new("slow answer")?;
            let fast = Namespace::new("fast answer")?;
            let valid = serde_json::json!({"code":"return await tools.fast({});"});
            let model = MockCompletionModel::from_turns([
                MockTurn::from_contents([("invalid", invalid), ("valid", valid)].map(
                    |(id, args)| {
                        rig::message::AssistantContent::ToolCall(rig::message::ToolCall::from_wire(
                            id,
                            rig::message::ToolFunction::new(
                                rig::message::ToolName::new("codemode").unwrap(),
                                args,
                            ),
                        ))
                    },
                )),
                MockTurn::text("both settled"),
            ]);
            let db = Database::new()?;
            let clock = Clock::default();
            let parent = builder(
                model.clone(),
                &slow,
                &fast,
                Limits::default(),
                NativeReplayPlanner,
            )?
            .build(db.clone(), clock.clone(), "parent")?;
            parent
                .submit(SubmitInput::new("r", "malformed sibling"))
                .await?;
            parent.drive().await?;
            assert!(!parent.status()?.closed);
            assert!(parent.script_state(&parent_key(0))?.is_none());
            assert_eq!(fast.engines.borrow().len(), 1);
            drop(parent);
            fast.finish().await?;
            clock.now.set(2000);
            let parent = builder(
                model.clone(),
                &slow,
                &fast,
                Limits::default(),
                NativeReplayPlanner,
            )?
            .build(db, clock, "parent")?;
            parent.drive().await?;
            let result = parent.result("r")?.unwrap();
            assert_eq!(result.output(), "both settled");
            assert_eq!(
                result.tool_outcomes[0].disposition,
                crate::ToolDisposition::Error
            );
            assert_eq!(
                result.tool_outcomes[1].disposition,
                crate::ToolDisposition::Success
            );
            assert_eq!(model.requests().len(), 2);
            assert!(!parent.status()?.busy);
        }
        Ok(())
    }
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
async fn implementation_version_change_preserves_approved_and_queued_work() -> Result<(), Error> {
    let db = Database::new()?;
    let gate = GateTool::new();
    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call("gate", "gate", serde_json::json!({})),
        MockTurn::text("answer"),
        MockTurn::text("queued answer"),
    ]);
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
    let status = serde_json::to_value(engine.status()?)?;
    drop(engine);
    let engine = Builder::new(model.clone())
        .tool_with(
            gate.clone(),
            ToolOptions::default()
                .require_approval()
                .policy(crate::ToolPolicy::read_only().implementation_version("v2")),
        )
        .build(db.clone(), Clock::default(), "s")?;
    assert!(matches!(engine.alarm().await, Err(Error::Incompatible(_))));
    assert_eq!(serde_json::to_value(engine.status()?)?, status);
    assert!(!engine.status()?.closed);
    assert_eq!(gate.count(), 0);
    assert_eq!(model.requests().len(), 1);
    drop(engine);
    let engine = Builder::new(model.clone())
        .tool_with(
            gate.clone(),
            ToolOptions::default()
                .require_approval()
                .policy(crate::ToolPolicy::read_only().implementation_version("v1")),
        )
        .build(db, Clock::default(), "s")?;
    gate.block.store(false, std::sync::atomic::Ordering::SeqCst);
    engine.drive().await?;
    assert_eq!(gate.count(), 1);
    assert_eq!(engine.result("r")?.unwrap().output(), "answer");
    assert_eq!(engine.result("queued")?.unwrap().output(), "queued answer");
    assert_eq!(model.requests().len(), 3);
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
        engine.result("r")?.map(|r| r.response.output()),
        Some("answer".into())
    );
    Ok(())
}

fn child_tool() -> rig::completion::ToolDefinition {
    rig::completion::ToolDefinition {
        name: rig::message::ToolName::new("helper").unwrap(),
        description: "Run the helper agent".into(),
        parameters: serde_json::json!({"type":"object"}),
        annotations: Default::default(),
    }
}

#[derive(Clone)]
struct LocalChild {
    engine: Rc<Engine<Database, Clock>>,
    lost_ack: Rc<Cell<bool>>,
    requests: Rc<std::cell::RefCell<Vec<String>>>,
}
impl ChildTransport for LocalChild {
    fn request<'a>(
        &'a self,
        request: &'a ChildRequest,
    ) -> futures::future::LocalBoxFuture<'a, Result<ChildReply, Error>> {
        Box::pin(async move {
            self.requests
                .borrow_mut()
                .push(serde_json::to_string(request)?);
            let reply = self.engine.handle_child_request(request.clone()).await?;
            if self.lost_ack.replace(false) {
                return Err(Error::Storage("injected child ack loss".into()));
            }
            Ok(reply)
        })
    }
}

#[tokio::test]
async fn ordinary_child_full_call_capacity_drains_and_reopens() -> Result<(), Error> {
    for (padding, rejected, oversized, retained) in [
        (300000, false, false, false),
        (348000, false, true, false),
        (350000, true, false, false),
        (348000, false, false, true),
    ] {
        let child_model = MockCompletionModel::from_turns([MockTurn::text("x".repeat(10000))]);
        let child_builder = Builder::new(child_model.clone());
        let definition = child_builder.child_definition()?;
        let fast = LocalChild {
            engine: Rc::new(child_builder.build(Database::new()?, Clock::default(), "fast")?),
            lost_ack: Rc::new(Cell::new(false)),
            requests: Rc::default(),
        };
        let slow_builder = Builder::new(MockCompletionModel::from_turns([MockTurn::text("slow")]));
        let slow_definition = slow_builder.child_definition()?;
        let slow = LocalChild {
            engine: Rc::new(slow_builder.build(Database::new()?, Clock::default(), "slow")?),
            lost_ack: Rc::new(Cell::new(false)),
            requests: Rc::default(),
        };
        let mut slow_tool = child_tool();
        slow_tool.name = rig::message::ToolName::new("slow").unwrap();
        let model = MockCompletionModel::from_turns([
            MockTurn::from_contents(
                [
                    ("slow", "slow", serde_json::json!({})),
                    (
                        "large",
                        "helper",
                        serde_json::json!({"padding":"x".repeat(padding)}),
                    ),
                ]
                .map(|(id, name, args)| {
                    rig::message::AssistantContent::ToolCall(rig::message::ToolCall::from_wire(
                        id,
                        rig::message::ToolFunction::new(
                            rig::message::ToolName::new(name).unwrap(),
                            args,
                        ),
                    ))
                }),
            ),
            MockTurn::text("settled"),
        ]);
        let build = || {
            Builder::new(model.clone())
                .sub_agent(
                    child_tool(),
                    ChildHandle::new(definition.clone(), fast.clone()),
                )
                .unwrap()
                .sub_agent(
                    slow_tool.clone(),
                    ChildHandle::new(slow_definition.clone(), slow.clone()),
                )
                .unwrap()
                .session_history_max_bytes(32 * 1024 * 1024)
        };
        let db = Database::new()?;
        let clock = Clock::default();
        let parent = build().build(db.clone(), clock.clone(), "parent")?;
        parent.submit(SubmitInput::new("r", "capacity")).await?;
        let result = parent.drive().await;
        if rejected {
            assert!(matches!(result, Err(Error::Storage(_))));
            assert!(parent.status()?.busy);
            assert!(!parent.status()?.closed);
            assert!(slow.requests.borrow().is_empty());
            assert!(fast.requests.borrow().is_empty());
            assert!(db.exec("SELECT key FROM rig_tool_calls", &[])?.is_empty());
            continue;
        }
        result?;
        if retained {
            // Legacy intent: the row itself fits, but no terminal settlement
            // was reserved. Fill its remaining capacity without new effects.
            let rows = db.exec("SELECT key, data FROM rig_tool_calls", &[])?;
            let row = rows
                .iter()
                .find(|row| storage::text(row).unwrap().contains("\"name\":\"helper\""))
                .unwrap();
            let data = storage::text(row)?;
            let original = "x".repeat(padding);
            let copies = data.matches(&original).count();
            let expanded = "x".repeat(padding + (storage::ROW_BYTES - data.len()) / copies);
            let mut old: Call = serde_json::from_str(&data.replace(&original, &expanded))?;
            old.digest =
                crate::identity::arguments_digest(&serde_json::from_str(&old.input.arguments)?)?;
            let data = storage::encode(&old, storage::ROW_BYTES)?;
            assert!(data.len() > storage::ROW_BYTES - 4);
            let actual_key = row["key"].as_str().unwrap();
            db.exec(
                "UPDATE rig_tool_calls SET data = ? WHERE key = ?",
                &[data.clone(), actual_key.into()],
            )?;
            drop(parent);
            let parent = build().build(db.clone(), clock.clone(), "parent")?;
            clock.now.set(2000);
            assert!(matches!(parent.drive().await, Err(Error::Storage(_))));
            assert!(parent.status()?.busy);
            assert!(!parent.status()?.closed);
            assert!(parent.result("r")?.is_none());
            assert_eq!(slow.requests.borrow().len(), 1);
            assert_eq!(fast.requests.borrow().len(), 1);
            assert!(child_model.requests().is_empty());
            assert_eq!(
                storage::text(
                    &db.exec(
                        "SELECT data FROM rig_tool_calls WHERE key = ?",
                        &[actual_key.into()]
                    )?[0]
                )?,
                data
            );
            continue;
        }
        fast.engine.drive().await?;
        clock.now.set(2000);
        parent.drive().await?;
        assert!(parent.result("r")?.is_none());
        let keys = db.exec("SELECT key, data FROM rig_tool_calls", &[])?;
        let finished: Call = keys
            .iter()
            .map(|row| serde_json::from_str::<Call>(storage::text(row).unwrap()).unwrap())
            .find(|call| call.input.name == "helper")
            .unwrap();
        assert!(finished.output.is_some());
        let durable = finished.output.as_ref().unwrap().result.as_ref().unwrap();
        assert_eq!(durable.result.as_ref().unwrap().is_success(), !oversized);
        if oversized {
            let error = durable.result.as_ref().unwrap().error().unwrap();
            assert_eq!(error.kind(), rig::tool::ToolErrorKind::Other);
            assert_eq!(serde_json::to_value(error)?["retryable"], false);
        }
        drop(parent);
        slow.engine.drive().await?;
        let parent = build().build(db, clock.clone(), "parent")?;
        clock.now.set(4000);
        parent.drive().await?;
        assert_eq!(parent.result("r")?.unwrap().output(), "settled");
        assert_eq!(child_model.requests().len(), 1);
        assert_eq!(fast.requests.borrow().len(), 2);
        assert_eq!(slow.engine.status()?.receipts.len(), 1);
        let requests = slow.requests.borrow();
        assert_eq!(
            serde_json::from_str::<Value>(&requests[0])?["child_id"],
            serde_json::from_str::<Value>(&requests[1])?["child_id"]
        );
        assert_eq!(requests[1], requests[2]);
    }
    Ok(())
}

#[tokio::test]
async fn ordinary_child_ack_loss_and_completed_reuse_survive_parent_reopen() -> Result<(), Error> {
    let child_model = MockCompletionModel::from_turns([MockTurn::text("child answer")]);
    let child_builder = Builder::new(child_model.clone()).implementation_version("helper-v3");
    let descriptor = child_builder.child_definition()?;
    let transport = LocalChild {
        engine: Rc::new(child_builder.build(Database::new()?, Clock::default(), "child-object")?),
        lost_ack: Rc::new(Cell::new(true)),
        requests: Rc::new(std::cell::RefCell::new(Vec::new())),
    };
    let db = Database::new()?;
    let clock = Clock::default();
    let parent_model = MockCompletionModel::from_turns([
        MockTurn::tool_call(
            "child-call",
            "helper",
            serde_json::json!({"question":"asymmetric"}),
        ),
        MockTurn::text("parent answer"),
    ]);
    let parent = Builder::new(parent_model.clone())
        .sub_agent(
            child_tool(),
            ChildHandle::new(descriptor.clone(), transport.clone()),
        )?
        .build(db.clone(), clock.clone(), "parent")?;
    parent.submit(SubmitInput::new("r", "question")).await?;
    parent.drive().await?;
    assert_eq!(transport.engine.status()?.receipts.len(), 1);
    assert_eq!(child_model.requests().len(), 0);
    assert_eq!(parent.status()?.next_deadline_ms, Some(2000));
    drop(parent);
    transport.engine.drive().await?;
    assert_eq!(child_model.requests().len(), 1);
    let parent = Builder::new(parent_model.clone())
        .sub_agent(
            child_tool(),
            ChildHandle::new(descriptor, transport.clone()),
        )?
        .build(db, clock.clone(), "parent")?;
    clock.now.set(2000);
    parent.drive().await?;
    assert_eq!(parent.result("r")?.unwrap().output(), "parent answer");
    assert_eq!(
        transport.requests.borrow()[0],
        transport.requests.borrow()[1]
    );
    assert_eq!(transport.engine.status()?.receipts.len(), 1);
    assert_eq!(child_model.requests().len(), 1);
    assert_eq!(parent_model.requests().len(), 2);
    let repeated: ChildRequest = serde_json::from_str(&transport.requests.borrow()[0])?;
    transport.engine.close()?;
    assert!(matches!(
        transport.engine.handle_child_request(repeated).await?,
        ChildReply::Completed(_)
    ));
    assert_eq!(child_model.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn child_protocol_rejects_drift_conflicts_and_reports_terminal_failures() -> Result<(), Error>
{
    let model = MockCompletionModel::from_turns([MockTurn::tool_call(
        "bad",
        "missing",
        serde_json::json!({}),
    )]);
    let builder = Builder::new(model.clone()).implementation_version("v2");
    let descriptor = builder.child_definition()?;
    let engine = builder.build(Database::new()?, Clock::default(), "object")?;
    let request = ChildRequest::Submit {
        child_id: "child-a".into(),
        expected: descriptor.clone(),
        input: SubmitInput::new("child-a", "prompt"),
    };
    let mut drifted = descriptor.clone();
    drifted.config.max_turns += 1;
    assert!(matches!(
        engine
            .handle_child_request(ChildRequest::Submit {
                child_id: "child-a".into(),
                expected: drifted,
                input: SubmitInput::new("child-a", "prompt"),
            })
            .await?,
        ChildReply::Rejected(_)
    ));
    assert!(engine.status()?.receipts.is_empty());
    assert_eq!(model.requests().len(), 0);
    assert!(matches!(
        engine.handle_child_request(request.clone()).await?,
        ChildReply::Pending
    ));
    assert!(matches!(
        engine
            .handle_child_request(ChildRequest::Submit {
                child_id: "child-a".into(),
                expected: descriptor.clone(),
                input: SubmitInput::new("child-a", "changed"),
            })
            .await?,
        ChildReply::Rejected(_)
    ));
    assert_eq!(engine.status()?.receipts.len(), 1);
    assert!(engine.drive().await.is_err());
    assert!(matches!(
        engine
            .handle_child_request(ChildRequest::Poll {
                child_id: "child-a".into(),
                expected: descriptor,
                request_id: "child-a".into()
            })
            .await?,
        ChildReply::Failed(_)
    ));
    assert_eq!(model.requests().len(), 1);
    Ok(())
}

#[derive(Clone)]
struct CorruptChild {
    child: LocalChild,
    oversized: bool,
    poll: bool,
    corrupt: Rc<Cell<bool>>,
}
impl ChildTransport for CorruptChild {
    fn request<'a>(
        &'a self,
        request: &'a ChildRequest,
    ) -> futures::future::LocalBoxFuture<'a, Result<ChildReply, Error>> {
        Box::pin(async move {
            let reply = self.child.request(request).await?;
            if matches!(request, ChildRequest::Poll { .. }) == self.poll
                && self.corrupt.replace(false)
            {
                let bytes = if self.oversized {
                    vec![b' '; CHILD_PROTOCOL_MAX_BYTES + 1]
                } else {
                    b"not JSON".to_vec()
                };
                return ChildReply::decode(&bytes);
            }
            Ok(reply)
        })
    }
}

#[tokio::test]
async fn corrupt_child_replies_preserve_admission_and_acknowledgement_across_reopen()
-> Result<(), Error> {
    for oversized in [false, true] {
        for poll in [false, true] {
            let child_model = MockCompletionModel::from_turns([MockTurn::text("child answer")]);
            let child_builder = Builder::new(child_model.clone());
            let descriptor = child_builder.child_definition()?;
            let transport = CorruptChild {
                child: LocalChild {
                    engine: Rc::new(child_builder.build(
                        Database::new()?,
                        Clock::default(),
                        "child",
                    )?),
                    lost_ack: Rc::new(Cell::new(false)),
                    requests: Rc::default(),
                },
                oversized,
                poll,
                corrupt: Rc::new(Cell::new(true)),
            };
            let db = Database::new()?;
            let clock = Clock::default();
            let model = MockCompletionModel::from_turns([
                MockTurn::tool_call("helper", "helper", serde_json::json!({})),
                MockTurn::text("parent answer"),
            ]);
            let parent = Builder::new(model.clone())
                .sub_agent(
                    child_tool(),
                    ChildHandle::new(descriptor.clone(), transport.clone()),
                )?
                .build(db.clone(), clock.clone(), "parent")?;
            parent.submit(SubmitInput::new("r", "question")).await?;
            parent.drive().await?;
            if poll {
                clock.now.set(2000);
                parent.drive().await?;
            }
            assert!(!transport.corrupt.get());
            assert!(parent.status()?.busy);
            assert!(parent.result("r")?.is_none());
            assert_eq!(transport.child.engine.status()?.receipts.len(), 1);
            assert!(child_model.requests().is_empty());
            drop(parent);
            let parent = Builder::new(model.clone())
                .sub_agent(
                    child_tool(),
                    ChildHandle::new(descriptor, transport.clone()),
                )?
                .build(db, clock.clone(), "parent")?;
            clock.now.set(if poll { 4000 } else { 2000 });
            parent.drive().await?;
            {
                let requests = transport.child.requests.borrow();
                assert_eq!(
                    requests[if poll { 1 } else { 0 }],
                    *requests.last().unwrap()
                );
            }
            assert!(parent.status()?.busy);
            transport.child.engine.drive().await?;
            clock.now.set(if poll { 6000 } else { 4000 });
            parent.drive().await?;
            assert_eq!(parent.result("r")?.unwrap().output(), "parent answer");
            assert_eq!(transport.child.engine.status()?.receipts.len(), 1);
            assert_eq!(child_model.requests().len(), 1);
            assert_eq!(model.requests().len(), 2);
        }
    }
    Ok(())
}

#[tokio::test]
async fn admitted_child_registration_drift_blocks_without_terminal_rejection() -> Result<(), Error>
{
    let db = Database::new()?;
    let clock = Clock::default();
    let model = MockCompletionModel::from_turns([MockTurn::text("child answer")]);
    let builder = Builder::new(model.clone()).implementation_version("v1");
    let expected = builder.child_definition()?;
    let request = ChildRequest::Submit {
        child_id: "stable-child".into(),
        expected: expected.clone(),
        input: SubmitInput::new("stable-child", "original"),
    };
    let engine = builder.build(db.clone(), clock.clone(), "child")?;
    assert!(matches!(
        engine.handle_child_request(request.clone()).await?,
        ChildReply::Pending
    ));
    let status = serde_json::to_value(engine.status()?)?;
    drop(engine);
    let engine = Builder::new(model.clone())
        .implementation_version("v2")
        .build(db.clone(), clock.clone(), "child")?;
    assert!(matches!(
        engine.handle_child_request(request.clone()).await,
        Err(Error::Incompatible(_))
    ));
    let poll = ChildRequest::Poll {
        child_id: "stable-child".into(),
        expected: expected.clone(),
        request_id: "stable-child".into(),
    };
    assert!(matches!(
        engine.handle_child_request(poll.clone()).await,
        Err(Error::Incompatible(_))
    ));
    assert!(matches!(engine.alarm().await, Err(Error::Incompatible(_))));
    assert_eq!(serde_json::to_value(engine.status()?)?, status);
    assert!(model.requests().is_empty());
    assert!(matches!(
        engine
            .handle_child_request(ChildRequest::Submit {
                child_id: "stable-child".into(),
                expected: expected.clone(),
                input: SubmitInput::new("stable-child", "conflict"),
            })
            .await?,
        ChildReply::Rejected(_)
    ));
    assert!(matches!(
        engine
            .handle_child_request(ChildRequest::Poll {
                child_id: "different".into(),
                expected,
                request_id: "different".into(),
            })
            .await?,
        ChildReply::Rejected(_)
    ));
    drop(engine);
    let engine = Builder::new(model.clone())
        .implementation_version("v1")
        .build(db.clone(), clock.clone(), "child")?;
    assert!(matches!(
        engine.handle_child_request(request.clone()).await?,
        ChildReply::Pending
    ));
    engine.drive().await?;
    assert!(matches!(
        engine.handle_child_request(poll.clone()).await?,
        ChildReply::Completed(_)
    ));
    drop(engine);
    let engine = Builder::new(model.clone())
        .implementation_version("v2")
        .build(db, clock, "child")?;
    assert!(matches!(
        engine.handle_child_request(request).await?,
        ChildReply::Completed(_)
    ));
    assert!(matches!(
        engine.handle_child_request(poll).await?,
        ChildReply::Completed(_)
    ));
    assert_eq!(model.requests().len(), 1);
    assert_eq!(engine.status()?.receipts.len(), 1);
    Ok(())
}

#[test]
fn full_parent_call_identity_separates_identical_nested_ordinals() {
    let a = LogicalCallKey {
        logical_execution_id: "parent".into(),
        submission_id: "prompt-0".into(),
        model_turn: 1,
        call_index: 0,
    };
    let b = LogicalCallKey {
        call_index: 1,
        ..a.clone()
    };
    assert_ne!(child_id(&a, Some(0)), child_id(&b, Some(0)));
    assert_ne!(child_id(&a, Some(0)), child_id(&a, None));
    assert_eq!(child_id(&a, Some(0)), child_id(&a, Some(0)));
}

struct MigrationFailure(Database);
impl SqlDatabase for MigrationFailure {
    fn exec(&self, query: &str, params: &[String]) -> Result<Vec<BTreeMap<String, Value>>, Error> {
        if query.contains("CREATE TABLE IF NOT EXISTS rig_script_effects") {
            return Err(Error::Storage("injected migration failure".into()));
        }
        self.0.exec(query, params)
    }
    fn transaction<T>(&self, f: impl FnOnce() -> Result<T, Error>) -> Result<T, Error> {
        self.0.transaction(f)
    }
}

#[tokio::test]
async fn additive_schema_upgrade_rolls_back_and_reopens_old_session() -> Result<(), Error> {
    let db = Database::new()?;
    let model = MockCompletionModel::from_turns([MockTurn::text("old answer")]);
    let engine = Builder::new(model.clone()).build(db.clone(), Clock::default(), "s")?;
    engine.submit(SubmitInput::new("r", "old prompt")).await?;
    drop(engine);
    // Construct the version-2 layout, including its unextended JSON checkpoint.
    db.transaction(|| {
        let mut session = storage::read::<Value>(&db, "session")?.unwrap();
        session.as_object_mut().unwrap().remove("child_id");
        session
            .as_object_mut()
            .unwrap()
            .remove("implementation_version");
        storage::write(&db, "session", &session, 8 * 1024 * 1024)?;
        db.exec("DROP TABLE rig_script_effects", &[])?;
        db.exec("UPDATE rig_meta SET value = '2' WHERE key = 'schema'", &[])?;
        Ok(())
    })?;
    assert!(
        Builder::new(model.clone())
            .build(MigrationFailure(db.clone()), Clock::default(), "s")
            .is_err()
    );
    assert_eq!(
        db.exec("SELECT value FROM rig_meta WHERE key = 'schema'", &[])?[0]["value"],
        "2"
    );
    assert!(
        db.exec(
            "SELECT name FROM sqlite_master WHERE name = 'rig_script_effects'",
            &[]
        )?
        .is_empty()
    );
    let engine = Builder::new(model.clone()).build(db.clone(), Clock::default(), "s")?;
    assert_eq!(engine.wait("r").await?.unwrap().output(), "old answer");
    drop(engine);
    let engine = Builder::new(model.clone()).build(db.clone(), Clock::default(), "s")?;
    assert_eq!(engine.result("r")?.unwrap().output(), "old answer");
    assert_eq!(
        db.exec("SELECT value FROM rig_meta WHERE key = 'schema'", &[])?[0]["value"],
        "3"
    );
    assert_eq!(model.requests().len(), 1);
    Ok(())
}

#[derive(Clone)]
struct LargeOutput(Arc<std::sync::atomic::AtomicUsize>);
impl Tool for LargeOutput {
    const NAME: &'static str = "large";
    type Args = Value;
    type Output = String;
    type Error = std::convert::Infallible;
    fn description(&self) -> String {
        "Produce a bounded result".into()
    }
    fn parameters(&self) -> Value {
        serde_json::json!({"type":"object"})
    }
    async fn call(&self, _: &mut rig::tool::ToolContext, _: Value) -> Result<String, Self::Error> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok("é".repeat(150_000))
    }
}

#[tokio::test]
async fn runner_window_reserves_settlement_space_and_bounds_large_tool_results() -> Result<(), Error>
{
    let effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call("large", "large", serde_json::json!({})),
        MockTurn::text("bounded answer"),
    ]);
    let db = Database::new()?;
    let build = || {
        Builder::new(model.clone())
            .tool(LargeOutput(effects.clone()))
            .session_history_max_bytes(16 * 1024)
    };
    let engine = build().build(db.clone(), Clock::default(), "bounded")?;
    engine.submit(SubmitInput::new("r", "large result")).await?;
    engine.drive().await?;
    assert_eq!(effects.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        engine.result("r")?.unwrap().tool_outcomes[0].disposition,
        crate::ToolDisposition::Error
    );
    assert!(serde_json::to_string(&model.requests()[1].chat_history)?.contains(STORAGE_FAILURE));
    drop(engine);
    let engine = build().build(db, Clock::default(), "bounded")?;
    assert_eq!(engine.wait("r").await?.unwrap().output(), "bounded answer");
    assert_eq!(effects.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(model.requests().len(), 2);
    Ok(())
}

#[tokio::test]
async fn turn_limit_does_not_cancel_admitted_tools_during_capacity_planning() -> Result<(), Error> {
    let gate = GateTool::new();
    gate.block.store(false, std::sync::atomic::Ordering::SeqCst);
    let model = MockCompletionModel::from_turns([MockTurn::tool_call(
        "gate",
        "gate",
        serde_json::json!({"value": 8}),
    )]);
    let db = Database::new()?;
    let build = || Builder::new(model.clone()).tool(gate.clone()).max_turns(1);
    let engine = build().build(db.clone(), Clock::default(), "limited")?;
    engine.submit(SubmitInput::new("r", "run once")).await?;
    assert!(matches!(engine.drive().await, Err(Error::Invalid(_))));
    assert_eq!(gate.count(), 1);
    assert!(matches!(
        engine.status()?.receipts[0].state,
        SubmissionState::Failed { .. }
    ));
    drop(engine);
    let engine = build().build(db, Clock::default(), "limited")?;
    engine.drive().await?;
    assert_eq!(gate.count(), 1);
    assert_eq!(model.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn oversized_completed_child_reply_is_terminal_and_full_result_is_retained()
-> Result<(), Error> {
    let effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let child_model = MockCompletionModel::from_turns(
        (0..4)
            .map(|index| {
                MockTurn::tool_call(format!("large-{index}"), "large", serde_json::json!({}))
            })
            .chain([MockTurn::text("short answer")]),
    );
    let child_builder = Builder::new(child_model.clone()).tool(LargeOutput(effects.clone()));
    let definition = child_builder.child_definition()?;
    let transport = LocalChild {
        engine: Rc::new(child_builder.build(Database::new()?, Clock::default(), "child")?),
        lost_ack: Rc::new(Cell::new(false)),
        requests: Rc::default(),
    };
    let clock = Clock::default();
    let parent_model = MockCompletionModel::from_turns([
        MockTurn::tool_call("child", "helper", serde_json::json!({})),
        MockTurn::text("handled large result"),
    ]);
    let parent = Builder::new(parent_model.clone())
        .sub_agent(
            child_tool(),
            ChildHandle::new(definition, transport.clone()),
        )?
        .build(Database::new()?, clock.clone(), "parent")?;
    parent.submit(SubmitInput::new("r", "large child")).await?;
    parent.drive().await?;
    transport.engine.drive().await?;
    let receipt = transport.engine.status()?.receipts[0].clone();
    let result = transport.engine.result(&receipt.request_id)?.unwrap();
    assert_eq!(result.output(), "short answer");
    assert!(serde_json::to_vec(&result)?.len() > CHILD_PROTOCOL_MAX_BYTES);
    assert_eq!(receipt.state, SubmissionState::Answered);
    clock.now.set(2000);
    parent.drive().await?;
    assert_eq!(
        parent.result("r")?.unwrap().tool_outcomes[0].disposition,
        crate::ToolDisposition::Error
    );
    assert!(!parent.status()?.busy);
    assert_eq!(effects.load(std::sync::atomic::Ordering::SeqCst), 4);
    assert_eq!(child_model.requests().len(), 5);
    assert_eq!(transport.requests.borrow().len(), 2);
    let request: ChildRequest = serde_json::from_str(&transport.requests.borrow()[1])?;
    let reply = transport.engine.handle_child_request(request).await?;
    assert!(
        matches!(&reply, ChildReply::Failed(message) if message.contains("full result remains stored"))
    );
    assert!(serde_json::to_vec(&reply)?.len() < CHILD_PROTOCOL_MAX_BYTES);
    assert_eq!(effects.load(std::sync::atomic::Ordering::SeqCst), 4);
    Ok(())
}

#[tokio::test]
async fn child_checkpoint_limit_rejects_without_admission_or_endless_parent_retry()
-> Result<(), Error> {
    let child_model = MockCompletionModel::from_turns([MockTurn::text("unused")]);
    let child_builder = Builder::new(child_model.clone()).session_history_max_bytes(4096);
    let definition = child_builder.child_definition()?;
    let child_db = Database::new()?;
    let transport = LocalChild {
        engine: Rc::new(child_builder.build(child_db.clone(), Clock::default(), "child")?),
        lost_ack: Rc::new(Cell::new(false)),
        requests: Rc::default(),
    };
    let parent_model = MockCompletionModel::from_turns([
        MockTurn::tool_call(
            "child",
            "helper",
            serde_json::json!({"message":"x".repeat(8192)}),
        ),
        MockTurn::text("rejected child handled"),
    ]);
    let parent = Builder::new(parent_model.clone())
        .sub_agent(
            child_tool(),
            ChildHandle::new(definition, transport.clone()),
        )?
        .build(Database::new()?, Clock::default(), "parent")?;
    parent
        .submit(SubmitInput::new("r", "oversized child prompt"))
        .await?;
    parent.drive().await?;
    let result = parent.result("r")?.unwrap();
    assert_eq!(
        result.tool_outcomes[0].disposition,
        crate::ToolDisposition::Error
    );
    assert!(!parent.status()?.busy);
    assert_eq!(transport.requests.borrow().len(), 1);
    let request: ChildRequest = serde_json::from_str(&transport.requests.borrow()[0])?;
    assert!(serde_json::to_vec(&request)?.len() < CHILD_PROTOCOL_MAX_BYTES);
    assert!(matches!(
        transport.engine.handle_child_request(request).await?,
        ChildReply::Rejected(_)
    ));
    assert!(transport.engine.status()?.receipts.is_empty());
    assert!(child_model.requests().is_empty());
    let session = storage::read::<Value>(&child_db, "session")?.unwrap();
    assert!(session["child_id"].is_null());
    assert!(session["queued"].as_array().unwrap().is_empty());
    Ok(())
}
