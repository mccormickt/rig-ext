//! Native QuickJS backend through `rquickjs`.
//!
//! Each script gets a fresh `Runtime` and `Context` on a dedicated OS thread.
//! The runtime enforces the guest heap limit and a stack limit; an interrupt
//! handler stops execution at the deadline or on caller cancellation. The
//! guest has no module loader and no `std`/`os` intrinsics. This backend is
//! in-process: it bounds CPU, memory, and reachable APIs, but it is not a
//! fault boundary against engine bugs. See the crate README.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use rig_core::tool::ToolErrorKind;
use rquickjs::context::EvalOptions;
use rquickjs::convert::Coerced;
use rquickjs::function::Opt;
use rquickjs::promise::PromiseState;
use rquickjs::{Context, Ctx, Exception, FromJs, Function, Object, Promise, Runtime, Value};

use super::{PRELUDE, SCRIPT_FILENAME, SpawnError, locate, wrap_source, write_text};
use crate::bridge::{
    Payload, Reply, Request, ScriptError, WorkerChannels, WorkerConfig, WorkerOutcome, bounded_json,
};
use crate::catalog::{MAX_NAME_BYTES, MAX_QUERY_BYTES};
use crate::report::{ExecutionStatus, ScriptDiagnostic, ScriptOutput};

const WORKER_STACK_BYTES: usize = 8 * 1024 * 1024;
const GUEST_STACK_BYTES: usize = 512 * 1024;
const JOBS_PER_DEADLINE_CHECK: u32 = 64;
const DEFAULT_SEARCH_LIMIT: usize = 10;
const MAX_SEARCH_LIMIT: usize = 50;

/// Start the worker thread. Returns once the thread exists; the outcome
/// arrives on `channels.done`.
pub(crate) fn spawn(
    config: WorkerConfig,
    channels: WorkerChannels,
) -> Result<std::thread::JoinHandle<()>, SpawnError> {
    std::thread::Builder::new()
        .name("rig-codemode-quickjs".into())
        .stack_size(WORKER_STACK_BYTES)
        .spawn(move || {
            let WorkerChannels {
                requests,
                replies,
                done,
                cancel,
            } = channels;
            let outcome = run(config, requests, replies, cancel);
            let _ = done.send(outcome);
        })
        .map_err(SpawnError)
}

struct GuestState<'js> {
    next_ordinal: u32,
    over_budget_calls: u32,
    output: ScriptOutput,
    pending: HashMap<u32, (Function<'js>, Function<'js>)>,
}

type Shared<'js> = Rc<RefCell<GuestState<'js>>>;

fn run(
    config: WorkerConfig,
    requests: tokio::sync::mpsc::Sender<Request>,
    replies: std::sync::mpsc::Receiver<Reply>,
    cancel: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> WorkerOutcome {
    let failed = |message: &str| WorkerOutcome {
        status: ExecutionStatus::ScriptError,
        output: ScriptOutput::default(),
        returned: None,
        diagnostic: Some(ScriptDiagnostic {
            message: message.to_string(),
            line: None,
            column: None,
            stack: None,
        }),
        over_budget_calls: 0,
    };
    let Ok(runtime) = Runtime::new() else {
        return failed("could not create the script runtime");
    };
    runtime.set_memory_limit(config.limits.memory_bytes);
    runtime.set_max_stack_size(GUEST_STACK_BYTES);
    let interrupt_cancel = cancel.clone();
    let deadline = config.deadline;
    runtime.set_interrupt_handler(Some(Box::new(move || {
        interrupt_cancel.load(Ordering::Relaxed) || Instant::now() >= deadline
    })));
    let Ok(context) = Context::full(&runtime) else {
        return failed("could not create the script context");
    };
    let outcome = context.with(|ctx| {
        let state: Shared<'_> = Rc::new(RefCell::new(GuestState {
            next_ordinal: 0,
            over_budget_calls: 0,
            output: ScriptOutput::default(),
            pending: HashMap::new(),
        }));
        let session = Session {
            ctx: ctx.clone(),
            config: &config,
            requests: &requests,
            cancel: &cancel,
            state: state.clone(),
        };
        let outcome = session.run(&replies);
        // Drop guest handles held from Rust before the runtime is destroyed.
        state.borrow_mut().pending.clear();
        outcome
    });
    drop(context);
    drop(runtime);
    outcome
}

struct Session<'js, 'a> {
    ctx: Ctx<'js>,
    config: &'a WorkerConfig,
    requests: &'a tokio::sync::mpsc::Sender<Request>,
    cancel: &'a std::sync::Arc<std::sync::atomic::AtomicBool>,
    state: Shared<'js>,
}

enum Stop {
    Completed(Option<serde_json::Value>),
    Failed(ScriptDiagnostic),
    TimedOut,
    Cancelled,
    Stalled,
}

impl<'js> Session<'js, '_> {
    fn run(&self, replies: &std::sync::mpsc::Receiver<Reply>) -> WorkerOutcome {
        let stop = match self.install() {
            Ok(()) => match self.evaluate() {
                Ok(promise) => self.drive(promise, replies),
                Err(error) => self.classify(error),
            },
            Err(error) => self.classify(error),
        };
        let state = self.state.borrow();
        let (status, returned, diagnostic) = match stop {
            Stop::Completed(returned) => (ExecutionStatus::Completed, returned, None),
            Stop::Failed(diagnostic) => (ExecutionStatus::ScriptError, None, Some(diagnostic)),
            Stop::TimedOut => (
                ExecutionStatus::TimedOut,
                None,
                Some(plain("the script exceeded its wall-time limit")),
            ),
            Stop::Cancelled => (
                ExecutionStatus::Cancelled,
                None,
                Some(plain("the script was cancelled")),
            ),
            Stop::Stalled => (
                ExecutionStatus::Stalled,
                None,
                Some(plain(
                    "the script awaited a promise that no tool call or pending job can settle",
                )),
            ),
        };
        WorkerOutcome {
            status,
            output: state.output.clone(),
            returned,
            diagnostic,
            over_budget_calls: state.over_budget_calls,
        }
    }

    fn interrupted(&self) -> Option<Stop> {
        if self.cancel.load(Ordering::Relaxed) {
            Some(Stop::Cancelled)
        } else if Instant::now() >= self.config.deadline {
            Some(Stop::TimedOut)
        } else {
            None
        }
    }

    /// Turn an engine error into a stop reason. Interrupts win over the
    /// exception text they produce.
    fn classify(&self, error: rquickjs::Error) -> Stop {
        if let Some(stop) = self.interrupted() {
            return stop;
        }
        match error {
            rquickjs::Error::Exception => {
                let thrown = self.ctx.catch();
                Stop::Failed(self.diagnostic_of(thrown))
            }
            rquickjs::Error::Allocation => Stop::Failed(plain("out of memory")),
            other => Stop::Failed(plain(&other.to_string())),
        }
    }

    fn diagnostic_of(&self, thrown: Value<'js>) -> ScriptDiagnostic {
        let budget = self
            .config
            .limits
            .message_bytes
            .min(self.config.limits.output_bytes)
            .min(8192);
        let read = |value: Value<'js>, limit| {
            Coerced::<rquickjs::String>::from_js(&self.ctx, value)
                .and_then(|s| s.0.to_cstring())
                .map(|s| truncate(s.as_str(), limit).to_owned())
                .unwrap_or_else(|_| truncate("error", limit).to_owned())
        };
        if let Some(exception) = thrown.as_object().cloned().and_then(Exception::from_object) {
            let property = |key, limit| {
                exception
                    .as_object()
                    .get::<_, Value>(key)
                    .ok()
                    .filter(|value| !value.is_undefined())
                    .map(|value| read(value, limit))
                    .unwrap_or_default()
            };
            let name = property("name", budget / 4);
            let mut message = if !name.is_empty() && name != "Error" {
                format!("{name}: ")
            } else {
                String::new()
            };
            message.truncate(message.len().min(budget));
            message.push_str(&property("message", budget.saturating_sub(message.len())));
            let stack = property("stack", budget.saturating_sub(message.len()));
            let stack = (!stack.is_empty()).then_some(stack);
            let (line, column) = stack.as_deref().map(locate).unwrap_or((None, None));
            return ScriptDiagnostic {
                message,
                line,
                column,
                stack,
            };
        }
        let message = read(thrown, budget);
        plain(&message)
    }

    fn install(&self) -> rquickjs::Result<()> {
        let ctx = &self.ctx;
        let host = Object::new(ctx.clone())?;

        let names: Vec<String> = self
            .config
            .catalog
            .entries()
            .iter()
            .map(|entry| entry.name().to_string())
            .collect();
        host.set("names", Function::new(ctx.clone(), move || names.clone())?)?;

        let invoke = {
            let state = self.state.clone();
            let catalog = self.config.catalog.clone();
            let limits = self.config.limits;
            let requests = self.requests.clone();
            move |ctx: Ctx<'js>,
                  name: String,
                  args: Value<'js>,
                  raw: bool|
                  -> rquickjs::Result<Promise<'js>> {
                let (promise, resolve, reject) = ctx.promise()?;
                let ordinal = {
                    let mut state = state.borrow_mut();
                    if state.next_ordinal >= limits.max_calls {
                        state.over_budget_calls = state.over_budget_calls.saturating_add(1);
                        None
                    } else {
                        let ordinal = state.next_ordinal;
                        state.next_ordinal += 1;
                        Some(ordinal)
                    }
                };
                let Some(ordinal) = ordinal else {
                    let error = ScriptError::rejected(
                        ToolErrorKind::Other,
                        format!(
                            "call budget of {} tool calls is exhausted",
                            limits.max_calls
                        ),
                        name,
                    );
                    reject_with(&ctx, &reject, &error)?;
                    return Ok(promise);
                };
                if !catalog.contains(&name) {
                    let _ = requests.try_send(Request::Rejected {
                        ordinal,
                        name: name.clone(),
                        kind: ToolErrorKind::NotFound,
                    });
                    let error = ScriptError::rejected(
                        ToolErrorKind::NotFound,
                        format!("tool {name:?} is not available to this script"),
                        name,
                    );
                    reject_with(&ctx, &reject, &error)?;
                    return Ok(promise);
                }
                let serialized = ctx
                    .json_stringify(args)
                    .and_then(|value| value.map(|s| s.to_cstring()).transpose());
                let Ok(Some(arguments)) = serialized else {
                    if matches!(serialized, Err(rquickjs::Error::Exception)) {
                        let _ = ctx.catch();
                    }
                    let _ = requests.try_send(Request::Rejected {
                        ordinal,
                        name: name.clone(),
                        kind: ToolErrorKind::InvalidArgs,
                    });
                    let error = ScriptError::rejected(
                        ToolErrorKind::InvalidArgs,
                        "tool arguments must be a JSON-serializable value",
                        name,
                    );
                    reject_with(&ctx, &reject, &error)?;
                    return Ok(promise);
                };
                if arguments.len() > limits.message_bytes {
                    let _ = requests.try_send(Request::Rejected {
                        ordinal,
                        name: name.clone(),
                        kind: ToolErrorKind::InvalidArgs,
                    });
                    let error = ScriptError::size(
                        format!(
                            "tool arguments are {} bytes; the limit is {} bytes",
                            arguments.len(),
                            limits.message_bytes
                        ),
                        Some(name),
                    );
                    reject_with(&ctx, &reject, &error)?;
                    return Ok(promise);
                }
                let request = Request::Call {
                    ordinal,
                    name: name.clone(),
                    arguments: arguments.as_str().to_owned(),
                    raw,
                };
                if requests.try_send(request).is_err() {
                    let error = ScriptError::rejected(
                        ToolErrorKind::Cancelled,
                        "the host is no longer accepting tool calls",
                        name,
                    );
                    reject_with(&ctx, &reject, &error)?;
                    return Ok(promise);
                }
                state
                    .borrow_mut()
                    .pending
                    .insert(ordinal, (resolve, reject));
                Ok(promise)
            }
        };
        host.set("invoke", Function::new(ctx.clone(), invoke)?)?;

        let text = {
            let state = self.state.clone();
            let limit = self.config.limits.output_bytes;
            move |ctx: Ctx<'js>, value: Value<'js>| -> rquickjs::Result<()> {
                if let Some(string) = value.as_string() {
                    let string = string.to_string()?;
                    write_text(&mut state.borrow_mut().output, limit, &string);
                    return Ok(());
                }
                let json = ctx
                    .json_stringify(value)?
                    .map(|s| s.to_string())
                    .transpose()?
                    .unwrap_or_else(|| "undefined".to_string());
                let mut state = state.borrow_mut();
                let remaining = limit.saturating_sub(state.output.text.len());
                if json.len() >= remaining {
                    drop(state);
                    return Err(Exception::throw_range(
                        &ctx,
                        &format!(
                            "text() value is {} bytes but only {} bytes of the {} byte output budget remain",
                            json.len(),
                            remaining.saturating_sub(1),
                            limit
                        ),
                    ));
                }
                write_text(&mut state.output, limit, &json);
                Ok(())
            }
        };
        host.set("text", Function::new(ctx.clone(), text)?)?;

        let search = {
            let catalog = self.config.catalog.clone();
            let cancel = self.cancel.clone();
            let deadline = self.config.deadline;
            let message_bytes = self.config.limits.message_bytes;
            move |ctx: Ctx<'js>,
                  query: rquickjs::String<'js>,
                  limit: Opt<Option<f64>>|
                  -> rquickjs::Result<Value<'js>> {
                let query = query.to_cstring()?;
                if query.len() > MAX_QUERY_BYTES {
                    return Err(Exception::throw_range(
                        &ctx,
                        "search query exceeds 4096 bytes",
                    ));
                }
                let limit = match limit.0.flatten() {
                    Some(n) if n.is_finite() && n >= 1.0 => (n as usize).min(MAX_SEARCH_LIMIT),
                    Some(_) => 1,
                    None => DEFAULT_SEARCH_LIMIT,
                };
                let results: Vec<_> = catalog
                    .search_checked(query.as_str(), limit, || {
                        cancel.load(Ordering::Relaxed) || Instant::now() >= deadline
                    })
                    .map_err(|message| Exception::throw_range(&ctx, message))?
                    .into_iter()
                    .map(|entry| entry.summarize())
                    .collect();
                to_guest(&ctx, &results, message_bytes)
            }
        };
        host.set("searchTools", Function::new(ctx.clone(), search)?)?;

        let describe = {
            let catalog = self.config.catalog.clone();
            let message_bytes = self.config.limits.message_bytes;
            move |ctx: Ctx<'js>, name: rquickjs::String<'js>| -> rquickjs::Result<Value<'js>> {
                let name = name.to_cstring()?;
                if name.len() > MAX_NAME_BYTES {
                    return Err(Exception::throw_range(&ctx, "tool name exceeds 128 bytes"));
                }
                match catalog.get(name.as_str()) {
                    Some(entry) => to_guest(&ctx, &entry.describe(), message_bytes),
                    None => Ok(Value::new_null(ctx)),
                }
            }
        };
        host.set("describeTool", Function::new(ctx.clone(), describe)?)?;

        ctx.globals().set("__host", host)?;
        ctx.eval::<(), _>(PRELUDE)?;
        Ok(())
    }

    fn evaluate(&self) -> rquickjs::Result<Promise<'js>> {
        let mut options = EvalOptions::default();
        options.strict = false;
        options.filename = Some(SCRIPT_FILENAME.to_string());
        self.ctx
            .eval_with_options::<Promise<'js>, _>(wrap_source(&self.config.code), options)
    }

    fn drive(&self, promise: Promise<'js>, replies: &std::sync::mpsc::Receiver<Reply>) -> Stop {
        loop {
            let mut executed: u32 = 0;
            while self.ctx.execute_pending_job() {
                executed = executed.wrapping_add(1);
                if executed.is_multiple_of(JOBS_PER_DEADLINE_CHECK)
                    && let Some(stop) = self.interrupted()
                {
                    return stop;
                }
            }
            if let Some(stop) = self.interrupted() {
                return stop;
            }
            match promise.state() {
                PromiseState::Pending => {}
                PromiseState::Resolved => {
                    return match promise.result::<Value<'js>>() {
                        Some(Ok(value)) => self.returned(value),
                        Some(Err(error)) => self.classify(error),
                        None => Stop::Stalled,
                    };
                }
                PromiseState::Rejected => {
                    let _ = promise.result::<Value<'js>>();
                    return self.classify(rquickjs::Error::Exception);
                }
            }
            if self.state.borrow().pending.is_empty() {
                return Stop::Stalled;
            }
            let remaining = self
                .config
                .deadline
                .saturating_duration_since(Instant::now());
            match replies.recv_timeout(remaining) {
                Ok(reply) => {
                    if let Err(error) = self.settle(reply) {
                        return self.classify(error);
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => return Stop::TimedOut,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return Stop::Cancelled,
            }
        }
    }

    fn settle(&self, reply: Reply) -> rquickjs::Result<()> {
        let Some((resolve, reject)) = self.state.borrow_mut().pending.remove(&reply.ordinal) else {
            return Ok(());
        };
        match reply.payload {
            Payload::Resolve(json) => match self.ctx.json_parse(json) {
                Ok(value) => resolve.call::<_, ()>((value,)),
                Err(rquickjs::Error::Exception) => {
                    let thrown = self.ctx.catch();
                    reject.call::<_, ()>((thrown,))
                }
                Err(error) => Err(error),
            },
            Payload::Reject(error) => reject_with(&self.ctx, &reject, &error),
        }
    }

    fn returned(&self, value: Value<'js>) -> Stop {
        if value.is_undefined() {
            return Stop::Completed(None);
        }
        let json = match self.ctx.json_stringify(value) {
            Ok(Some(json)) => match json.to_string() {
                Ok(json) => json,
                Err(error) => return self.classify(error),
            },
            Ok(None) => return Stop::Completed(None),
            Err(error) => return self.classify(error),
        };
        if json.len() > self.config.limits.message_bytes {
            return Stop::Failed(plain(&format!(
                "the return value is {} bytes; the limit is {} bytes",
                json.len(),
                self.config.limits.message_bytes
            )));
        }
        match serde_json::from_str(&json) {
            Ok(value) => Stop::Completed(Some(value)),
            Err(_) => Stop::Failed(plain("the return value is not valid JSON")),
        }
    }
}

fn plain(message: &str) -> ScriptDiagnostic {
    ScriptDiagnostic {
        message: message.to_string(),
        line: None,
        column: None,
        stack: None,
    }
}

fn truncate(text: &str, limit: usize) -> &str {
    let mut end = text.len().min(limit);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn to_guest<'js>(
    ctx: &Ctx<'js>,
    value: &impl serde::Serialize,
    limit: usize,
) -> rquickjs::Result<Value<'js>> {
    let json = bounded_json(value, limit).map_err(|_| {
        Exception::throw_range(ctx, "discovery result exceeds the byte or nesting limit")
    })?;
    ctx.json_parse(json)
}

/// Reject a pending promise with a `CodeModeToolError`.
fn reject_with<'js>(
    ctx: &Ctx<'js>,
    reject: &Function<'js>,
    error: &ScriptError,
) -> rquickjs::Result<()> {
    let exception = Exception::from_message(ctx.clone(), &error.message)?;
    let object = exception.as_object();
    object.set("name", "CodeModeToolError")?;
    object.set("kind", error.kind.as_str())?;
    object.set("status", error.status)?;
    if let Some(tool) = &error.tool {
        object.set("tool", tool.as_str())?;
    }
    reject.call::<_, ()>((exception,))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use crate::{Catalog, CatalogEntry, DynamicToolDispatcher, Limits, ScriptGrant};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn large_catalog_search_stops_and_worker_is_joined() {
        let catalog = Arc::new(
            Catalog::new((0..10000).map(|i| {
                CatalogEntry::new(
                    format!("tool_{i}"),
                    "common alpha beta",
                    serde_json::json!({"type": "object"}),
                )
            }))
            .unwrap(),
        );
        let limits = Limits {
            wall_time: Duration::from_millis(200),
            ..Limits::default()
        };
        let (worker, host) = crate::bridge::channels(&limits);
        let started = Instant::now();
        let deadline = started + limits.wall_time;
        let handle = spawn(
            WorkerConfig {
                code: "while (true) searchTools('common alpha beta');".into(),
                catalog: catalog.clone(),
                limits,
                deadline,
            },
            worker,
        )
        .unwrap();
        let report = crate::bridge::run_host(
            Arc::new(DynamicToolDispatcher::new([]).unwrap()),
            limits,
            deadline,
            "p".into(),
            ScriptGrant::catalog(Default::default()),
            host,
        )
        .await;
        handle.join().unwrap();
        assert_eq!(report.status, ExecutionStatus::TimedOut);
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(
            Arc::strong_count(&catalog),
            1,
            "worker released its catalog"
        );
    }
}
