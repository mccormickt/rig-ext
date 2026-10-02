//! The host side of the guest bridge: bounded request/reply messages, result
//! projection into script values, call admission, concurrency, and the
//! cancellation protocol. Engine-independent.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use rig_core::completion::message::ToolResultContent;
use rig_core::tool::{ToolErrorKind, ToolExecutionError, ToolOutput, ToolResult};
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use crate::catalog::Catalog;
use crate::dispatch::{DispatchOutcome, HostDispatcher, Invocation};
use crate::limits::Limits;
use crate::policy::ScriptGrant;
use crate::report::{
    CallRecord, CallStatus, ExecutionReport, ExecutionStatus, ScriptDelivery, ScriptDiagnostic,
    ScriptOutput,
};

/// Guest-to-host message.
#[derive(Debug)]
pub(crate) enum Request {
    /// The script called `tools[name](args)` or `.raw(args)`.
    Call {
        ordinal: u32,
        name: String,
        /// Serialized JSON arguments, at most `message_bytes`.
        arguments: String,
        raw: bool,
    },
    /// The guest rejected the call before sending it: unknown name or
    /// oversized arguments. Recorded, never dispatched.
    Rejected {
        ordinal: u32,
        name: String,
        kind: ToolErrorKind,
    },
}

/// Host-to-guest message.
#[derive(Debug)]
pub(crate) struct Reply {
    pub ordinal: u32,
    pub payload: Payload,
}

#[derive(Debug)]
pub(crate) enum Payload {
    /// Resolve the promise with this JSON value.
    Resolve(String),
    /// Reject the promise with a typed script error.
    Reject(ScriptError),
}

/// A safe, typed error the script can observe. Contains only model-visible
/// text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScriptError {
    pub message: String,
    pub kind: ToolErrorKind,
    /// `error`, `denied`, `skipped`, `rejected`, or `size`.
    pub status: &'static str,
    pub tool: Option<String>,
}

impl ScriptError {
    pub(crate) fn size(message: impl Into<String>, tool: Option<String>) -> Self {
        Self {
            message: message.into(),
            kind: ToolErrorKind::InvalidArgs,
            status: "size",
            tool,
        }
    }

    pub(crate) fn rejected(kind: ToolErrorKind, message: impl Into<String>, tool: String) -> Self {
        Self {
            message: message.into(),
            kind,
            status: "rejected",
            tool: Some(tool),
        }
    }
}

/// What the worker reports when the guest is done.
#[derive(Debug)]
pub(crate) struct WorkerOutcome {
    pub status: ExecutionStatus,
    pub output: ScriptOutput,
    pub returned: Option<serde_json::Value>,
    pub diagnostic: Option<ScriptDiagnostic>,
    /// Calls the guest rejected because the call budget was exhausted. They
    /// have no records; the budget bounds the record list.
    pub over_budget_calls: u32,
}

/// The worker's ends of the bridge.
pub(crate) struct WorkerChannels {
    pub requests: mpsc::Sender<Request>,
    pub replies: std::sync::mpsc::Receiver<Reply>,
    pub done: oneshot::Sender<WorkerOutcome>,
    pub cancel: Arc<AtomicBool>,
}

/// Everything a worker needs to run one script.
pub(crate) struct WorkerConfig {
    pub code: String,
    pub catalog: Arc<Catalog>,
    pub limits: Limits,
    pub deadline: Instant,
}

/// The host's ends of the bridge.
pub(crate) struct HostChannels {
    pub requests: mpsc::Receiver<Request>,
    pub replies: std::sync::mpsc::Sender<Reply>,
    pub done: oneshot::Receiver<WorkerOutcome>,
    pub cancel: Arc<AtomicBool>,
}

/// Build both ends. The request channel holds at most `max_calls` messages;
/// the guest never sends more than that many admitted calls.
pub(crate) fn channels(limits: &Limits) -> (WorkerChannels, HostChannels) {
    let (request_tx, request_rx) = mpsc::channel(limits.max_calls as usize);
    let (reply_tx, reply_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = oneshot::channel();
    let cancel = Arc::new(AtomicBool::new(false));
    (
        WorkerChannels {
            requests: request_tx,
            replies: reply_rx,
            done: done_tx,
            cancel: cancel.clone(),
        },
        HostChannels {
            requests: request_rx,
            replies: reply_tx,
            done: done_rx,
            cancel,
        },
    )
}

#[derive(Serialize)]
struct RawEnvelope<'a> {
    status: &'static str,
    name: &'a str,
    content: &'a [ToolResultContent],
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RawError<'a>>,
}

#[derive(Serialize)]
struct RawError<'a> {
    kind: ToolErrorKind,
    message: String,
    retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<&'a str>,
}

/// JSON writer with byte and nesting ceilings. It fails before copying a
/// write that exceeds either ceiling; serde then stops visiting the value.
pub(crate) fn bounded_json(
    value: &(impl Serialize + ?Sized),
    limit: usize,
) -> Result<String, serde_json::Error> {
    struct Writer {
        bytes: Vec<u8>,
        limit: usize,
        depth: usize,
        quoted: bool,
        escaped: bool,
    }
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
                return Err(std::io::Error::other("JSON byte limit exceeded"));
            }
            for &byte in bytes {
                if self.quoted {
                    if self.escaped {
                        self.escaped = false;
                    } else if byte == b'\\' {
                        self.escaped = true;
                    } else if byte == b'"' {
                        self.quoted = false;
                    }
                } else {
                    match byte {
                        b'"' => self.quoted = true,
                        b'[' | b'{' => {
                            self.depth += 1;
                            if self.depth > 64 {
                                return Err(std::io::Error::other("JSON nesting limit exceeded"));
                            }
                        }
                        b']' | b'}' => self.depth = self.depth.saturating_sub(1),
                        _ => {}
                    }
                }
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer {
        bytes: Vec::new(),
        limit,
        depth: 0,
        quoted: false,
        escaped: false,
    };
    serde_json::to_writer(&mut writer, value)?;
    // serde_json emits UTF-8, including when it escapes a string.
    String::from_utf8(writer.bytes)
        .map_err(|error| serde_json::Error::io(std::io::Error::other(error)))
}

fn bounded_render(output: &ToolOutput, limit: usize) -> Result<String, ()> {
    if let Some(text) = output.as_text() {
        return (text.len() <= limit).then(|| text.to_owned()).ok_or(());
    }
    if let Some(value) = output.as_json() {
        bounded_json(value, limit).map_err(|_| ())
    } else {
        bounded_json(output.as_content(), limit).map_err(|_| ())
    }
}

/// Project a policy-filtered result into what the script receives.
///
/// Normal mode: one JSON block is that value; one plain text block is the
/// literal string; mixed content is `{ "content": [...] }`; error, refusal,
/// and skip reject with a typed error carrying only model-visible text.
/// Raw mode: a status envelope for every disposition.
pub(crate) fn project(name: &str, result: &ToolResult, raw: bool, limit: usize) -> Payload {
    let unserializable = || {
        Payload::Reject(ScriptError::size(
            "tool result exceeds the byte or nesting limit",
            Some(name.into()),
        ))
    };
    if raw {
        let error = result.error().or_else(|| result.refusal());
        let message = match error
            .map(|error| bounded_render(error.model_output(), limit))
            .transpose()
        {
            Ok(message) => message,
            Err(()) => return unserializable(),
        };
        let envelope = RawEnvelope {
            status: result.status_name(),
            name,
            content: result.output().as_content(),
            error: error.map(|error| RawError {
                kind: error.kind(),
                message: message.unwrap_or_default(),
                retryable: error
                    .retryable()
                    .or_else(|| error.kind().default_retryable())
                    .unwrap_or(false),
                code: error.code(),
            }),
        };
        return bounded_json(&envelope, limit)
            .map(Payload::Resolve)
            .unwrap_or_else(|_| unserializable());
    }
    if result.is_success() {
        let output = result.output();
        let json = if let Some(value) = output.as_json() {
            bounded_json(value, limit)
        } else if let Some(text) = output.as_text() {
            bounded_json(text, limit)
        } else {
            #[derive(Serialize)]
            struct Content<'a> {
                content: &'a [ToolResultContent],
            }
            bounded_json(
                &Content {
                    content: output.as_content(),
                },
                limit,
            )
        };
        return json
            .map(Payload::Resolve)
            .unwrap_or_else(|_| unserializable());
    }
    let (status, kind) = if let Some(error) = result.refusal() {
        ("denied", error.kind())
    } else if let Some(error) = result.error() {
        ("error", error.kind())
    } else {
        ("skipped", ToolErrorKind::Cancelled)
    };
    Payload::Reject(ScriptError {
        message: match bounded_render(result.output(), limit) {
            Ok(message) => message,
            Err(()) => return unserializable(),
        },
        kind,
        status,
        tool: Some(name.to_string()),
    })
}

/// Enforce the message limit on a projected payload. Oversized results become
/// a size error so the guest never parses or copies them.
pub(crate) fn bound_payload(
    name: &str,
    payload: Payload,
    message_bytes: usize,
) -> (Payload, ScriptDelivery) {
    if matches!(&payload, Payload::Reject(error) if error.status == "size") {
        return (payload, ScriptDelivery::Oversized);
    }
    let size = match &payload {
        Payload::Resolve(json) => json.len(),
        Payload::Reject(error) => error.message.len(),
    };
    if size > message_bytes {
        let error = ScriptError::size(
            format!("tool result is {size} bytes; the limit is {message_bytes} bytes"),
            Some(name.to_string()),
        );
        (Payload::Reject(error), ScriptDelivery::Oversized)
    } else {
        (payload, ScriptDelivery::Delivered)
    }
}

/// Interrupts the guest when the host future is dropped before the worker
/// reports.
struct CancelOnDrop(Arc<AtomicBool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

struct Admitted {
    ordinal: u32,
    name: String,
    arguments: serde_json::Value,
    raw: bool,
}

struct Started {
    name: String,
    raw: bool,
}

struct HostState {
    dispatcher: Arc<dyn HostDispatcher>,
    limits: Limits,
    deadline: Instant,
    parent_call_id: String,
    grant: ScriptGrant,
    replies: std::sync::mpsc::Sender<Reply>,
    records: BTreeMap<u32, CallRecord>,
    queued: VecDeque<Admitted>,
    in_flight: JoinSet<(u32, DispatchOutcome)>,
    /// Ordinal and identity of each in-flight task, keyed by task id so a
    /// panicked task can still be attributed.
    started: HashMap<tokio::task::Id, (u32, Started)>,
}

impl HostState {
    fn record(&mut self, record: CallRecord) {
        self.records.insert(record.ordinal, record);
    }

    fn reply(&self, ordinal: u32, payload: Payload, delivery: ScriptDelivery) -> ScriptDelivery {
        if self.replies.send(Reply { ordinal, payload }).is_ok() {
            delivery
        } else {
            ScriptDelivery::Discarded
        }
    }

    fn reject(&mut self, ordinal: u32, name: String, error: ScriptError) {
        let kind = error.kind;
        let delivery = self.reply(ordinal, Payload::Reject(error), ScriptDelivery::Delivered);
        self.record(CallRecord {
            ordinal,
            name,
            status: CallStatus::Rejected,
            error_kind: Some(kind),
            delivery,
        });
    }

    /// Refuse a call the script grant does not cover, without dispatching.
    /// The script observes exactly what a dispatcher refusal produces.
    fn refuse_not_granted(&mut self, ordinal: u32, name: String, raw: bool) {
        let result = ToolResult::failed(ToolExecutionError::refused(format!(
            "tool {name:?} is not granted to this script"
        )));
        self.finish(
            ordinal,
            Started { name, raw },
            DispatchOutcome::from(result),
        );
    }

    fn start_queued(&mut self) {
        while Instant::now() < self.deadline
            && self.in_flight.len() < self.limits.max_in_flight as usize
        {
            let Some(admitted) = self.queued.pop_front() else {
                break;
            };
            let invocation = Invocation::new(
                self.parent_call_id.clone(),
                admitted.ordinal,
                admitted.name.clone(),
                admitted.arguments,
                self.grant.context.for_dispatch(),
            );
            let dispatcher = self.dispatcher.clone();
            let ordinal = admitted.ordinal;
            let handle = self
                .in_flight
                .spawn(async move { (ordinal, dispatcher.dispatch(invocation).await) });
            self.started.insert(
                handle.id(),
                (
                    ordinal,
                    Started {
                        name: admitted.name,
                        raw: admitted.raw,
                    },
                ),
            );
        }
    }

    fn finish(&mut self, ordinal: u32, started: Started, outcome: DispatchOutcome) {
        let (status, error_kind) = status_of(&outcome.result);
        let payload = project(
            &started.name,
            &outcome.result,
            started.raw,
            self.limits.message_bytes,
        );
        let (payload, delivery) = bound_payload(&started.name, payload, self.limits.message_bytes);
        let delivery = self.reply(ordinal, payload, delivery);
        self.record(CallRecord {
            ordinal,
            name: started.name,
            status,
            error_kind,
            delivery,
        });
    }

    async fn stop_dispatches(&mut self) {
        self.in_flight.abort_all();
        while let Some(joined) = self.in_flight.join_next_with_id().await {
            let (id, status, error_kind, delivery) = match joined {
                Ok((id, (_, outcome))) => {
                    let (status, kind) = status_of(&outcome.result);
                    (id, status, kind, ScriptDelivery::Discarded)
                }
                Err(error) if error.is_cancelled() => (
                    error.id(),
                    CallStatus::CancellationRequested,
                    None,
                    ScriptDelivery::None,
                ),
                Err(error) => (
                    error.id(),
                    CallStatus::Failed,
                    Some(ToolErrorKind::Other),
                    ScriptDelivery::Discarded,
                ),
            };
            if let Some((ordinal, started)) = self.started.remove(&id) {
                self.record(CallRecord {
                    ordinal,
                    name: started.name,
                    status,
                    error_kind,
                    delivery,
                });
            }
        }
    }
}

/// Stop admission at the deadline, then wait for the interrupted worker.
pub(crate) async fn run_host(
    dispatcher: Arc<dyn HostDispatcher>,
    limits: Limits,
    deadline: Instant,
    parent_call_id: String,
    grant: ScriptGrant,
    channels: HostChannels,
) -> ExecutionReport {
    let started_at = Instant::now();
    let HostChannels {
        mut requests,
        replies,
        mut done,
        cancel,
    } = channels;
    let _cancel_guard = CancelOnDrop(cancel.clone());
    let mut state = HostState {
        dispatcher,
        limits,
        deadline,
        parent_call_id,
        grant,
        replies,
        records: BTreeMap::new(),
        queued: VecDeque::new(),
        in_flight: JoinSet::new(),
        started: HashMap::new(),
    };
    let mut requests_open = true;
    let timer = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
    tokio::pin!(timer);
    let mut timed_out = false;

    let mut outcome = loop {
        if Instant::now() >= deadline {
            timed_out = true;
            break None;
        }
        tokio::select! {
            biased;
            outcome = &mut done => break outcome.ok(),
            () = &mut timer => {
                timed_out = true;
                break None;
            },
            request = requests.recv(), if requests_open => match request {
                None => requests_open = false,
                Some(Request::Rejected { ordinal, name, kind }) => state.record(CallRecord {
                    ordinal,
                    name,
                    status: CallStatus::Rejected,
                    error_kind: Some(kind),
                    delivery: ScriptDelivery::Delivered,
                }),
                Some(Request::Call { ordinal, name, raw, .. }) if !state.grant.tools.allows(&name) => {
                    state.refuse_not_granted(ordinal, name, raw);
                }
                Some(Request::Call { ordinal, name, arguments, raw }) => {
                    match serde_json::from_str(&arguments) {
                        Ok(arguments) => {
                            state.queued.push_back(Admitted { ordinal, name, arguments, raw });
                            state.start_queued();
                        }
                        Err(_) => {
                            let error = ScriptError::rejected(
                                ToolErrorKind::InvalidArgs,
                                "arguments are not valid JSON",
                                name.clone(),
                            );
                            state.reject(ordinal, name, error);
                        }
                    }
                }
            },
            joined = state.in_flight.join_next_with_id(), if !state.in_flight.is_empty() => {
                match joined {
                    Some(Ok((id, (ordinal, outcome)))) => {
                        if let Some((_, started)) = state.started.remove(&id) {
                            state.finish(ordinal, started, outcome);
                        }
                    }
                    Some(Err(join_error)) => {
                        if let Some((ordinal, started)) = state.started.remove(&join_error.id()) {
                            tracing::error!(
                                parent = %state.parent_call_id,
                                tool = %started.name,
                                "tool dispatch task panicked"
                            );
                            let error = ScriptError::rejected(
                                ToolErrorKind::Other,
                                "tool dispatch failed",
                                started.name.clone(),
                            );
                            let delivery = state.reply(ordinal, Payload::Reject(error), ScriptDelivery::Delivered);
                            state.record(CallRecord {
                                ordinal,
                                name: started.name,
                                status: CallStatus::Failed,
                                error_kind: Some(ToolErrorKind::Other),
                                delivery,
                            });
                        }
                    }
                    None => {}
                }
                state.start_queued();
            },
        }
    };

    // Stop admitting work: abort in-flight dispatches, mark queued calls as
    // never started, and let late replies die with the dropped sender.
    cancel.store(true, Ordering::Relaxed);
    state.stop_dispatches().await;
    drop(state.replies);
    if timed_out {
        outcome = done.await.ok();
        if let Some(outcome) = &mut outcome {
            outcome.status = ExecutionStatus::TimedOut;
        }
    }
    // Requests the guest sent before it finished are still in the channel;
    // record them so every started call has a record.
    while let Ok(request) = requests.try_recv() {
        match request {
            Request::Rejected {
                ordinal,
                name,
                kind,
            } => {
                state.records.insert(
                    ordinal,
                    CallRecord {
                        ordinal,
                        name,
                        status: CallStatus::Rejected,
                        error_kind: Some(kind),
                        delivery: ScriptDelivery::Delivered,
                    },
                );
            }
            Request::Call { ordinal, name, .. } => {
                state.records.insert(
                    ordinal,
                    CallRecord {
                        ordinal,
                        name,
                        status: CallStatus::NotStarted,
                        error_kind: None,
                        delivery: ScriptDelivery::None,
                    },
                );
            }
        }
    }
    for admitted in state.queued.drain(..) {
        state.records.insert(
            admitted.ordinal,
            CallRecord {
                ordinal: admitted.ordinal,
                name: admitted.name,
                status: CallStatus::NotStarted,
                error_kind: None,
                delivery: ScriptDelivery::None,
            },
        );
    }

    let mut report = match outcome {
        Some(outcome) => {
            let mut report = ExecutionReport::new(outcome.status);
            report.output = outcome.output;
            report.returned = outcome.returned;
            report.diagnostic = outcome.diagnostic;
            report.over_budget_calls = outcome.over_budget_calls;
            report
        }
        None => {
            let mut report = ExecutionReport::new(ExecutionStatus::TimedOut);
            report.diagnostic = Some(ScriptDiagnostic {
                message: "the script did not stop by its deadline".into(),
                line: None,
                column: None,
                stack: None,
            });
            report
        }
    };
    report.calls = state.records.into_values().collect();
    report.elapsed = started_at.elapsed();
    report
}

fn status_of(result: &ToolResult) -> (CallStatus, Option<ToolErrorKind>) {
    if result.is_success() {
        (CallStatus::Succeeded, None)
    } else if let Some(error) = result.refusal() {
        (CallStatus::Refused, Some(error.kind()))
    } else if let Some(error) = result.error() {
        (CallStatus::Failed, Some(error.kind()))
    } else {
        (CallStatus::Skipped, None)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;
    use rig_core::tool::{ToolExecutionError, ToolOutput};
    use serde_json::json;

    fn completed() -> WorkerOutcome {
        WorkerOutcome {
            status: ExecutionStatus::Completed,
            output: ScriptOutput::default(),
            returned: None,
            diagnostic: None,
            over_budget_calls: 0,
        }
    }

    #[tokio::test]
    async fn shutdown_keeps_ready_unjoined_outcomes() {
        struct Complete {
            done: std::sync::Mutex<Option<oneshot::Sender<WorkerOutcome>>>,
            result: ToolResult,
        }
        impl HostDispatcher for Complete {
            fn dispatch<'a>(&'a self, _: Invocation) -> crate::DispatchFuture<'a> {
                Box::pin(async move {
                    self.done
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap()
                        .send(completed())
                        .unwrap();
                    self.result.clone().into()
                })
            }
        }
        for (result, expected) in [
            (
                ToolResult::success(ToolOutput::text("ok")),
                CallStatus::Succeeded,
            ),
            (
                ToolResult::failed(ToolExecutionError::invalid_args("bad")),
                CallStatus::Failed,
            ),
            (
                ToolResult::failed(ToolExecutionError::refused("no")),
                CallStatus::Refused,
            ),
            (ToolResult::skipped("skip"), CallStatus::Skipped),
        ] {
            let limits = Limits::default();
            let (worker, host) = channels(&limits);
            worker
                .requests
                .send(Request::Call {
                    ordinal: 0,
                    name: "ready".into(),
                    arguments: "{}".into(),
                    raw: false,
                })
                .await
                .unwrap();
            let dispatcher = Complete {
                done: std::sync::Mutex::new(Some(worker.done)),
                result,
            };
            let report = run_host(
                Arc::new(dispatcher),
                limits,
                Instant::now() + limits.wall_time,
                "p".into(),
                ScriptGrant::catalog(Default::default()),
                host,
            )
            .await;
            assert_eq!(report.calls.len(), 1);
            assert_eq!(report.calls[0].status, expected);
            assert_eq!(report.calls[0].delivery, ScriptDelivery::Discarded);
            assert!(worker.replies.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn delivered_means_sent_even_when_the_reply_is_unread() {
        let (replies, receiver) = std::sync::mpsc::channel();
        let mut state = HostState {
            dispatcher: Arc::new(crate::DynamicToolDispatcher::new([]).unwrap()),
            limits: Limits::default(),
            deadline: Instant::now() + std::time::Duration::from_secs(1),
            parent_call_id: "p".into(),
            grant: ScriptGrant::catalog(Default::default()),
            replies,
            records: BTreeMap::new(),
            queued: VecDeque::new(),
            in_flight: JoinSet::new(),
            started: HashMap::new(),
        };
        state.finish(
            0,
            Started {
                name: "ready".into(),
                raw: false,
            },
            ToolResult::success(ToolOutput::text("ok")).into(),
        );
        state.stop_dispatches().await;
        assert_eq!(state.records[&0].delivery, ScriptDelivery::Delivered);
        assert_eq!(
            receiver.try_recv().unwrap().ordinal,
            0,
            "reply was still queued"
        );
    }

    #[tokio::test]
    async fn deadline_stops_admission_before_worker_done() {
        use std::sync::atomic::AtomicUsize;
        use std::time::Duration;
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let dispatcher = crate::DynamicToolDispatcher::new([rig_core::tool::DynamicTool::new(
            "late",
            "",
            json!({}),
            move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(ToolOutput::text("ran")) })
            },
        )])
        .unwrap();
        let limits = Limits::default();
        let (worker, host) = channels(&limits);
        let simulated_worker = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            worker
                .requests
                .send(Request::Call {
                    ordinal: 0,
                    name: "late".into(),
                    arguments: "{}".into(),
                    raw: false,
                })
                .await
                .unwrap();
            worker.done.send(completed()).unwrap();
        });
        let report = run_host(
            Arc::new(dispatcher),
            limits,
            Instant::now() + Duration::from_millis(10),
            "p".into(),
            ScriptGrant::catalog(Default::default()),
            host,
        )
        .await;
        simulated_worker.await.unwrap();
        assert_eq!(report.status, ExecutionStatus::TimedOut);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(report.calls[0].status, CallStatus::NotStarted);
    }

    fn project(name: &str, result: &ToolResult, raw: bool) -> Payload {
        super::project(name, result, raw, 1024)
    }

    #[test]
    fn bounded_writer_stops_visiting_and_limits_depth() {
        use serde::ser::SerializeSeq;
        struct Many<'a>(&'a std::cell::Cell<usize>);
        impl Serialize for Many<'_> {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let mut seq = serializer.serialize_seq(Some(1_000_000))?;
                for _ in 0..1_000_000 {
                    self.0.set(self.0.get() + 1);
                    seq.serialize_element("abcdefgh")?;
                }
                seq.end()
            }
        }
        let visits = std::cell::Cell::new(0);
        assert!(bounded_json(&Many(&visits), 24).is_err());
        assert_eq!(visits.get(), 3);
        assert_eq!(bounded_json("é", 4).unwrap(), "\"é\"");
        assert!(bounded_json("é", 3).is_err());
        let mut nested = json!(0);
        for _ in 0..64 {
            nested = json!([nested]);
        }
        assert_eq!(
            bounded_json(&nested, 1024).unwrap(),
            format!("{}0{}", "[".repeat(64), "]".repeat(64))
        );
        nested = json!([nested]);
        assert!(bounded_json(&nested, 1024).is_err());
        let escaped = json!({"x": "[\\\"{}]".repeat(80)});
        assert_eq!(bounded_json(&escaped, 1024).unwrap(), escaped.to_string());
        for raw in [false, true] {
            let result = ToolResult::failed(ToolExecutionError::other("x".repeat(2048)));
            assert!(
                matches!(project("t", &result, raw), Payload::Reject(error) if error.status == "size")
            );
        }
    }

    #[test]
    fn json_string_stays_structured_and_text_stays_literal() {
        let json = ToolResult::success(ToolOutput::json(json!("{\"x\":1}")));
        let text = ToolResult::success(ToolOutput::text("{\"x\":1}"));
        match (project("t", &json, false), project("t", &text, false)) {
            (Payload::Resolve(a), Payload::Resolve(b)) => {
                assert_eq!(a, "\"{\\\"x\\\":1}\"");
                assert_eq!(b, "\"{\\\"x\\\":1}\"");
            }
            other => panic!("unexpected {other:?}"),
        }
        let object = ToolResult::success(ToolOutput::json(json!({"x": 1})));
        assert!(matches!(project("t", &object, false), Payload::Resolve(s) if s == "{\"x\":1}"));
    }

    #[test]
    fn mixed_content_uses_envelope_in_order() {
        let output = ToolOutput::content(vec![
            ToolResultContent::text("a"),
            ToolResultContent::json(json!(1)),
        ])
        .unwrap();
        let Payload::Resolve(json) = project("t", &ToolResult::success(output), false) else {
            panic!("expected resolve");
        };
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["content"][0]["type"], "text");
        assert_eq!(value["content"][1]["value"], 1);
    }

    #[test]
    fn errors_reject_with_model_output_only() {
        let error = ToolExecutionError::other("operator detail: db password is hunter2")
            .with_model_feedback("the tool failed");
        let result = ToolResult::failed(error);
        let Payload::Reject(script_error) = project("t", &result, false) else {
            panic!("expected reject");
        };
        assert_eq!(script_error.message, "the tool failed");
        assert_eq!(script_error.status, "error");
        assert_eq!(script_error.kind, ToolErrorKind::Other);

        let Payload::Resolve(raw) = project("t", &result, true) else {
            panic!("expected envelope");
        };
        assert!(!raw.contains("hunter2"), "{raw}");
        let raw: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(raw["status"], "error");
        assert_eq!(raw["error"]["message"], "the tool failed");
        assert_eq!(raw["error"]["retryable"], false);
    }

    #[test]
    fn refusal_and_skip_reject() {
        let refused = ToolResult::failed(ToolExecutionError::refused("no"));
        let Payload::Reject(error) = project("t", &refused, false) else {
            panic!()
        };
        assert_eq!(error.status, "denied");
        assert_eq!(error.kind, ToolErrorKind::PermissionDenied);
        let skipped = ToolResult::skipped("policy skipped");
        let Payload::Reject(error) = project("t", &skipped, false) else {
            panic!()
        };
        assert_eq!(error.status, "skipped");
        let Payload::Resolve(raw) = project("t", &skipped, true) else {
            panic!()
        };
        assert!(raw.contains("\"status\":\"skipped\""));
    }

    #[test]
    fn oversized_payload_becomes_size_error() {
        let payload = Payload::Resolve("x".repeat(11));
        let (payload, delivery) = bound_payload("t", payload, 10);
        assert_eq!(delivery, ScriptDelivery::Oversized);
        assert!(matches!(payload, Payload::Reject(e) if e.status == "size"));
        let (_, delivery) = bound_payload("t", Payload::Resolve("x".repeat(10)), 10);
        assert_eq!(delivery, ScriptDelivery::Delivered);
    }
}
