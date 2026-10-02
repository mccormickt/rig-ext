#![cfg(feature = "duroxide")]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use duroxide::{RetryPolicy, providers::sqlite::SqliteProvider};
use rig::{
    completion::{CompletionRequest, Message},
    message::{AssistantContent, UserContent},
    test_utils::{MockAddTool, MockCompletionModel, MockTurn},
    tool::{Tool, ToolContext, ToolExecutionError},
};
use rig_durable::{
    AgentOrchestrator, AgentOrchestratorError, CompactionPolicy, DurableAgent, InvocationContract,
    OutcomeSource, SubmissionError, SubmissionMode, SubmissionState, SubmitInput, ToolDisposition,
    ToolOptions, ToolPolicy,
};
use serde::Deserialize;

const WAIT: Duration = Duration::from_secs(15);

async fn orchestrator(definition: rig_durable::AgentDefinition) -> AgentOrchestrator {
    let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
    AgentOrchestrator::builder(store)
        .register(definition)
        .unwrap()
        .start()
        .await
        .unwrap()
}

/// The first `expected` requests the model received. Duroxide delivers
/// activities at least once, so a request after `expected` is accepted only
/// when it repeats an earlier request.
fn scripted_requests(model: &MockCompletionModel, expected: usize) -> Vec<CompletionRequest> {
    let requests = model.requests();
    assert!(
        requests.len() >= expected,
        "expected {expected} requests, got {}",
        requests.len()
    );
    for (index, extra) in requests.iter().enumerate().skip(expected) {
        assert!(
            requests[..expected]
                .iter()
                .any(|earlier| earlier.chat_history == extra.chat_history),
            "request {index} is not a redelivery: {:?}",
            user_texts(&extra.chat_history)
        );
    }
    requests[..expected].to_vec()
}

fn user_texts(messages: &[Message]) -> Vec<String> {
    messages
        .iter()
        .filter_map(|message| match message {
            Message::User { content } => content.iter().find_map(|item| match item {
                UserContent::Text(text) => Some(text.text.clone()),
                _ => None,
            }),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn duplicate_submission_returns_receipt_and_altered_duplicate_is_rejected() {
    let model = MockCompletionModel::from_turns([MockTurn::text("only answer")]);
    let orchestrator = orchestrator(
        DurableAgent::builder("dedup", model.clone())
            .build()
            .unwrap(),
    )
    .await;
    let session = orchestrator
        .agent("dedup")
        .unwrap()
        .open_session("s1")
        .await
        .unwrap();

    let first = session
        .submit(SubmitInput::new("req-1", "hello"))
        .await
        .unwrap();
    let response = session.wait_timeout("req-1", WAIT).await.unwrap();
    assert_eq!(response.output(), "only answer");

    let retried = session
        .submit(SubmitInput::new("req-1", "hello"))
        .await
        .unwrap();
    assert_eq!(retried.submission_id, first.submission_id);
    assert_eq!(retried.prompt_index, first.prompt_index);
    assert_eq!(retried.payload_digest, first.payload_digest);
    assert_eq!(retried.state, SubmissionState::Answered);

    let altered = session
        .submit(SubmitInput::new("req-1", "something else"))
        .await;
    assert!(
        matches!(
            altered,
            Err(AgentOrchestratorError::SubmissionRejected(
                SubmissionError::Conflict { ref request_id }
            )) if request_id == "req-1"
        ),
        "{altered:?}"
    );
    let altered_mode = session
        .submit(SubmitInput::new("req-1", "hello").mode(SubmissionMode::RejectIfBusy))
        .await;
    assert!(matches!(
        altered_mode,
        Err(AgentOrchestratorError::SubmissionRejected(
            SubmissionError::Conflict { .. }
        ))
    ));

    assert_eq!(
        scripted_requests(&model, 1).len(),
        1,
        "one admitted prompt ran"
    );
    let ledger = session.ledger().await.unwrap().unwrap();
    assert_eq!(ledger.receipts.len(), 1);

    session.close().await.unwrap();
    let result = session.result_timeout(WAIT).await.unwrap();
    assert_eq!(result.ledger.receipts.len(), 1);
    assert_eq!(result.context.transcript.len(), 2);
    orchestrator.shutdown(None).await;
}

#[derive(Clone)]
struct HeldTool {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    calls: Arc<AtomicUsize>,
}

#[derive(Deserialize)]
struct NoArgs {}

impl Tool for HeldTool {
    const NAME: &'static str = "held";
    type Args = NoArgs;
    type Output = &'static str;
    type Error = ToolExecutionError;

    fn description(&self) -> String {
        "Blocks until the test releases it".into()
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({"type":"object"})
    }

    async fn call(&self, _: &mut ToolContext, _: NoArgs) -> Result<Self::Output, Self::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_waiters();
        self.release.notified().await;
        Ok("held result")
    }
}

#[tokio::test]
async fn follow_up_during_a_tool_round_runs_after_the_answer_and_reject_if_busy_is_rejected() {
    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call("call-1", "held", serde_json::json!({})),
        MockTurn::text("first answer"),
        MockTurn::text("second answer"),
        MockTurn::text("third answer"),
    ]);
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let orchestrator = orchestrator(
        DurableAgent::builder("follow-up", model.clone())
            .tool_with(
                HeldTool {
                    entered: entered.clone(),
                    release: release.clone(),
                    calls: calls.clone(),
                },
                ToolOptions::default().retry(RetryPolicy::new(1)),
            )
            .build()
            .unwrap(),
    )
    .await;
    let session = orchestrator
        .agent("follow-up")
        .unwrap()
        .open_session("s2")
        .await
        .unwrap();

    let first = session
        .submit(SubmitInput::new("req-1", "first"))
        .await
        .unwrap();
    assert_eq!(first.prompt_index, 0);
    tokio::time::timeout(WAIT, entered.notified())
        .await
        .unwrap();

    // Both arrive while the tool round is in progress. The session admits
    // them at the next operation boundary, after the tool result returns.
    let follow_up = session.submit(SubmitInput::new("req-2", "second"));
    let busy =
        session.submit(SubmitInput::new("req-3", "third").mode(SubmissionMode::RejectIfBusy));
    let (follow_up, busy, ()) = tokio::join!(follow_up, busy, async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        release.notify_one();
    });
    let follow_up = follow_up.unwrap();
    assert_eq!(follow_up.prompt_index, 1);
    assert!(
        matches!(
            busy,
            Err(AgentOrchestratorError::SubmissionRejected(
                SubmissionError::Busy
            ))
        ),
        "{busy:?}"
    );

    let first_response = session.wait_timeout("req-1", WAIT).await.unwrap();
    assert_eq!(first_response.output(), "first answer");
    assert_eq!(first_response.tool_outcomes.len(), 1);
    let second_response = session.wait_timeout("req-2", WAIT).await.unwrap();
    assert_eq!(second_response.output(), "second answer");
    assert!(second_response.tool_outcomes.is_empty());

    let requests = scripted_requests(&model, 3);
    // The follow-up did not enter the first prompt's tool round: the second
    // model call of the first prompt saw only the first prompt.
    assert_eq!(user_texts(&requests[1].chat_history), ["first"]);
    assert_eq!(requests[1].chat_history.len(), 3);
    // It ran after the answer, on top of the whole first exchange.
    assert_eq!(user_texts(&requests[2].chat_history), ["first", "second"]);
    assert_eq!(requests[2].chat_history.len(), 5);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // The same rejected request is admitted once the session is idle.
    let third = session
        .submit(SubmitInput::new("req-3", "third").mode(SubmissionMode::RejectIfBusy))
        .await
        .unwrap();
    assert_eq!(third.prompt_index, 2);
    assert_eq!(
        session.wait_timeout("req-3", WAIT).await.unwrap().output(),
        "third answer"
    );

    session.close().await.unwrap();
    let result = session.result_timeout(WAIT).await.unwrap();
    assert_eq!(result.context.transcript.len(), 8);
    assert_eq!(
        result
            .ledger
            .receipts
            .values()
            .map(|r| r.prompt_index)
            .max(),
        Some(2)
    );
    orchestrator.shutdown(None).await;
}

const SUMMARY_PROMPT: &str = "Summarize the conversation above for an assistant that will \
continue it. Reply with the summary only.";

fn has_tool_call(messages: &[Message]) -> bool {
    messages.iter().any(|message| {
        matches!(
            message,
            Message::Assistant { content, .. }
                if content.iter().any(|item| matches!(item, AssistantContent::ToolCall(_)))
        )
    })
}

#[tokio::test]
async fn compaction_keeps_the_request_canonical_and_the_transcript_complete() {
    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call("call-1", "add", serde_json::json!({"x": 1, "y": 2})),
        MockTurn::text("answer one"),
        MockTurn::text("answer two"),
        // Summarizer call, scheduled after the second prompt answers.
        MockTurn::text("SUMMARY ONE"),
        MockTurn::text("answer three"),
        // Second summary, built on the first.
        MockTurn::text("SUMMARY TWO"),
        MockTurn::text("answer four"),
        // Summaries run after the answer is published, so the session is
        // closed before the requests are counted.
        MockTurn::text("SUMMARY THREE"),
    ]);
    let orchestrator = orchestrator(
        DurableAgent::builder("compacting", model.clone())
            .tool(MockAddTool)
            .compaction(CompactionPolicy::new(4, 2).version("test-1"))
            .build()
            .unwrap(),
    )
    .await;
    let session = orchestrator
        .agent("compacting")
        .unwrap()
        .open_session("s3")
        .await
        .unwrap();

    // Prompt one: 4 transcript messages, within budget.
    assert_eq!(session.prompt("one").await.unwrap().output(), "answer one");
    assert_eq!(scripted_requests(&model, 2).len(), 2);
    // Prompt two: 6 messages. Cutoff 4 is the prompt boundary that keeps
    // two; it does not split the tool exchange.
    assert_eq!(session.prompt("two").await.unwrap().output(), "answer two");
    assert_eq!(
        session.prompt("three").await.unwrap().output(),
        "answer three"
    );
    assert_eq!(
        session.prompt("four").await.unwrap().output(),
        "answer four"
    );
    session.close().await.unwrap();
    let result = session.result_timeout(WAIT).await.unwrap();

    let requests = scripted_requests(&model, 8);

    let summary_one = &requests[3];
    assert!(matches!(
        summary_one.chat_history.first(),
        Some(Message::System { .. })
    ));
    assert_eq!(summary_one.chat_history.len(), 6);
    assert!(has_tool_call(&summary_one.chat_history));
    assert_eq!(
        user_texts(&summary_one.chat_history[1..]),
        ["one", SUMMARY_PROMPT]
    );

    let third = &requests[4];
    rig::transcript::validate_canonical(&third.chat_history).unwrap();
    assert_eq!(third.chat_history.len(), 4);
    let texts = user_texts(&third.chat_history);
    assert_eq!(
        texts[0],
        "Summary of the earlier conversation (compacted, policy version test-1):\nSUMMARY ONE"
    );
    assert_eq!(&texts[1..], ["two", "three"]);
    assert!(
        !has_tool_call(&third.chat_history),
        "the tool exchange left the active context"
    );

    let summary_two = &requests[5];
    assert_eq!(
        user_texts(&summary_two.chat_history[1..]),
        [
            "Summary of the conversation before this window:\nSUMMARY ONE",
            "two",
            SUMMARY_PROMPT
        ]
    );
    assert_eq!(summary_two.chat_history.len(), 5);

    let fourth = &requests[6];
    rig::transcript::validate_canonical(&fourth.chat_history).unwrap();
    assert_eq!(
        user_texts(&fourth.chat_history),
        [
            "Summary of the earlier conversation (compacted, policy version test-1):\nSUMMARY TWO",
            "three",
            "four"
        ]
    );
    assert_eq!(fourth.chat_history.len(), 4);
    assert_eq!(
        user_texts(&requests[7].chat_history[1..]),
        [
            "Summary of the conversation before this window:\nSUMMARY TWO",
            "three",
            SUMMARY_PROMPT
        ]
    );

    assert_eq!(result.context.transcript.len(), 10);
    assert_eq!(
        user_texts(&result.context.transcript),
        ["one", "two", "three", "four"]
    );
    assert!(
        has_tool_call(&result.context.transcript),
        "the audit transcript keeps the tool exchange"
    );
    let record = result.context.compaction.unwrap();
    assert_eq!(record.cutoff, 8);
    assert_eq!(record.input_messages, 2);
    assert_eq!(record.policy_version, "test-1");
    assert_eq!(record.summary, "SUMMARY THREE");
    orchestrator.shutdown(None).await;
}

#[tokio::test]
async fn failed_summary_leaves_the_context_in_place_and_the_session_open() {
    let model = MockCompletionModel::from_turns([
        MockTurn::text("answer one"),
        MockTurn::error("summarizer down"),
        MockTurn::text("answer two"),
        MockTurn::text("SUMMARY"),
        MockTurn::text("answer three"),
    ]);
    let orchestrator = orchestrator(
        DurableAgent::builder("compaction-failure", model.clone())
            .completion_retry(RetryPolicy::new(1))
            .compaction(CompactionPolicy::new(1, 0))
            .build()
            .unwrap(),
    )
    .await;
    let session = orchestrator
        .agent("compaction-failure")
        .unwrap()
        .open_session("s6")
        .await
        .unwrap();

    assert_eq!(session.prompt("one").await.unwrap().output(), "answer one");
    assert_eq!(session.prompt("two").await.unwrap().output(), "answer two");
    assert_eq!(
        session.prompt("three").await.unwrap().output(),
        "answer three"
    );
    session.close().await.unwrap();
    let result = session.result_timeout(WAIT).await.unwrap();

    // The summary after the third prompt had no scripted turn and failed too.
    let requests = scripted_requests(&model, 6);
    // The failed summary left the full context for the second prompt.
    assert_eq!(user_texts(&requests[2].chat_history), ["one", "two"]);
    // The next summary covered everything up to the newest boundary.
    assert_eq!(
        user_texts(&requests[3].chat_history[1..]),
        ["one", "two", SUMMARY_PROMPT]
    );
    assert_eq!(
        user_texts(&requests[4].chat_history),
        [
            "Summary of the earlier conversation (compacted, policy version 1):\nSUMMARY",
            "three"
        ]
    );
    assert_eq!(
        user_texts(&requests[5].chat_history[1..]),
        [
            "Summary of the conversation before this window:\nSUMMARY",
            "three",
            SUMMARY_PROMPT
        ]
    );

    assert_eq!(result.context.transcript.len(), 6);
    assert_eq!(result.context.compaction.unwrap().cutoff, 4);
    orchestrator.shutdown(None).await;
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

fn disposition_model() -> MockCompletionModel {
    MockCompletionModel::from_turns([
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
    ])
}

fn disposition_agent(model: MockCompletionModel) -> rig_durable::AgentDefinition {
    DurableAgent::builder("dispositions", model)
        .invocation_contract(InvocationContract::Logical)
        .max_turns(4)
        .tool_with(
            Disposition,
            ToolOptions::default()
                .retry(RetryPolicy::new(1))
                .policy(ToolPolicy::read_only()),
        )
        .build()
        .unwrap()
}

fn assert_dispositions(outcomes: &[rig_durable::ToolOutcome]) {
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
async fn single_run_exposes_ordered_dispositions_with_equal_content() {
    let model = disposition_model();
    let orchestrator = orchestrator(disposition_agent(model.clone())).await;
    let agent = orchestrator.agent("dispositions").unwrap();
    let run = agent.start_with_id("run-1", "go").await.unwrap();

    // The plain Rig response is unchanged.
    let plain = run.wait_timeout(WAIT).await.unwrap();
    assert_eq!(plain.output, "done");

    let detailed = run.wait_detailed_timeout(WAIT).await.unwrap();
    assert_eq!(detailed.output(), "done");
    assert_eq!(detailed.response.output, plain.output);
    assert_dispositions(&detailed.tool_outcomes);
    assert_eq!(run.tool_outcomes().await.unwrap().tool_outcomes.len(), 3);

    // Every tool result the model saw carried the same words.
    let last = model.requests().last().unwrap().chat_history.clone();
    let results: Vec<String> = last
        .iter()
        .filter_map(|message| match message {
            Message::User { content } => content.iter().find_map(|item| match item {
                UserContent::ToolResult(result) => Some(
                    result
                        .content
                        .iter()
                        .filter_map(|c| match c {
                            rig::message::ToolResultContent::Text(t) => Some(t.text.clone()),
                            _ => None,
                        })
                        .collect::<String>(),
                ),
                _ => None,
            }),
            _ => None,
        })
        .collect();
    assert_eq!(results, ["same words", "same words", "same words"]);
    orchestrator.shutdown(None).await;
}

#[tokio::test]
async fn session_results_expose_ordered_dispositions() {
    let orchestrator = orchestrator(disposition_agent(disposition_model())).await;
    let session = orchestrator
        .agent("dispositions")
        .unwrap()
        .open_session("s4")
        .await
        .unwrap();
    let response = session.prompt("go").await.unwrap();
    assert_eq!(response.output(), "done");
    assert_dispositions(&response.tool_outcomes);
    assert!(response.tool_outcomes.iter().all(|o| o.prompt_index == 0));
    orchestrator.shutdown(None).await;
}

#[tokio::test]
async fn failed_prompt_closes_the_session_and_cancels_queued_submissions() {
    // The model has one answer; the second prompt finds no turn and fails.
    let model = MockCompletionModel::from_turns([MockTurn::text("first")]);
    let orchestrator = orchestrator(
        DurableAgent::builder("failing", model)
            .completion_retry(RetryPolicy::new(1))
            .build()
            .unwrap(),
    )
    .await;
    let session = orchestrator
        .agent("failing")
        .unwrap()
        .open_session("s5")
        .await
        .unwrap();
    assert_eq!(session.prompt("one").await.unwrap().output(), "first");
    session
        .submit(SubmitInput::new("req-2", "two"))
        .await
        .unwrap();
    let failed = session.wait_timeout("req-2", WAIT).await;
    assert!(
        matches!(failed, Err(AgentOrchestratorError::Run(_))),
        "{failed:?}"
    );
    let closed = session.submit(SubmitInput::new("req-3", "three")).await;
    assert!(
        matches!(
            closed,
            Err(AgentOrchestratorError::SubmissionRejected(
                SubmissionError::Closed
            ))
        ),
        "{closed:?}"
    );
    orchestrator.shutdown(None).await;
}
