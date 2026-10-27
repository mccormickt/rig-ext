//! Synchronous interpreter for the private replay exchange wire.
//! The caller supplies a bounded request and owns elapsed-time enforcement.
//! This module has no dispatcher, scheduler, transport, or host I/O.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use rquickjs::{Context, Runtime};
use serde::Serialize;

use crate::Catalog;
use crate::projection::bounded_json_with_depth;
use crate::replay::ReplayError;
use crate::replay::protocol::{
    Boundary, EventBatch, RunnerRequest, RunnerResponse, RunnerUsage, VERSION,
};
use crate::runtime::quickjs::{Session, Stop};

/// Independent request and response transport ceiling.
pub const MAX_TRANSPORT: usize = 16 * 1024 * 1024;
const MAX_INTERRUPTS: u32 = 10_000;
const MAX_JOBS: u32 = 100_000;
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

struct Transport {
    remaining: Cell<usize>,
    exceeded: Cell<bool>,
}

impl Transport {
    fn check(&self, value: &impl Serialize) -> Option<usize> {
        if self.exceeded.get() {
            return None;
        }
        match bounded_json_with_depth(value, self.remaining.get(), 128) {
            Ok(json) => Some(json.len()),
            Err(_) => {
                self.exceeded.set(true);
                None
            }
        }
    }

    fn charge(&self, value: &impl Serialize, comma: bool) -> bool {
        let Some(bytes) = self.check(value) else {
            return false;
        };
        match self.remaining.get().checked_sub(bytes + usize::from(comma)) {
            Some(remaining) => {
                self.remaining.set(remaining);
                true
            }
            None => {
                self.exceeded.set(true);
                false
            }
        }
    }
}

/// Run one fresh interpreter. Prefixes are checked before each selected reply.
/// The returned JSON uses the same protocol as the JavaScript replay adapter.
/// `execute_job` executes one job without discarding its pending exception.
pub fn exchange(
    body: &str,
    execute_job: fn(&rquickjs::Ctx<'_>) -> rquickjs::Result<bool>,
) -> Result<String, ReplayError> {
    if body.len() > MAX_TRANSPORT {
        return Err(ReplayError::BudgetExceeded);
    }
    let request: RunnerRequest = serde_json::from_str(body).map_err(|_| ReplayError::Protocol)?;
    if request.version != VERSION
        || request.limits.validate().is_err()
        || request.code.len() > request.limits.source_bytes
        || request.budget.work == 0
        || request.budget.work > MAX_SAFE_INTEGER
        || request.budget.bytes == 0
        || request.budget.bytes as u128 > MAX_SAFE_INTEGER as u128
        || [
            request.limits.source_bytes,
            request.limits.memory_bytes,
            request.limits.message_bytes,
            request.limits.output_bytes,
        ]
        .into_iter()
        .any(|n| n as u128 > MAX_SAFE_INTEGER as u128)
    {
        return Err(ReplayError::Protocol);
    }
    let cap = MAX_TRANSPORT.min(request.budget.bytes);
    let transport = Rc::new(Transport {
        remaining: Cell::new(cap),
        exceeded: Cell::new(false),
    });
    let mut response = RunnerResponse {
        version: VERSION,
        batches: Vec::new(),
        usage: RunnerUsage {
            work: 1,
            response_bytes: 0,
            elapsed: Duration::ZERO,
        },
    };
    if !transport.charge(&response, false) {
        return Err(ReplayError::BudgetExceeded);
    }
    let work = Rc::new(Cell::new(1_u64));
    let interrupted = Rc::new(Cell::new(false));
    let runtime =
        Runtime::new().map_err(|_| ReplayError::Runner("runtime allocation failed".into()))?;
    runtime.set_memory_limit(request.limits.memory_bytes);
    runtime.set_max_stack_size(64 * 1024);
    let unhandled = Rc::new(Cell::new(0_usize));
    let rejections = unhandled.clone();
    runtime.set_host_promise_rejection_tracker(Some(Box::new(move |_, _, _, handled| {
        rejections.set(if handled {
            rejections.get().saturating_sub(1)
        } else {
            rejections.get().saturating_add(1)
        });
    })));
    let poll_work = work.clone();
    let poll_interrupted = interrupted.clone();
    let poll_transport = transport.clone();
    let mut interrupts = 0;
    runtime.set_interrupt_handler(Some(Box::new(move || {
        interrupts += 1;
        poll_work.set(poll_work.get() + 1);
        let stop = poll_interrupted.get()
            || poll_transport.exceeded.get()
            || interrupts > MAX_INTERRUPTS
            || poll_work.get() > request.budget.work;
        poll_interrupted.set(stop);
        stop
    })));
    let context = Context::full(&runtime)
        .map_err(|_| ReplayError::Runner("context allocation failed".into()))?;
    let catalog = Arc::new(Catalog::new(request.catalog).map_err(|_| ReplayError::Protocol)?);
    context.with(|ctx| {
        let events = Rc::new(RefCell::new(Vec::new()));
        let emit_events = events.clone();
        let emit_transport = transport.clone();
        let stop_work = work.clone();
        let stop_interrupted = interrupted.clone();
        let stop_transport = transport.clone();
        let output_transport = transport.clone();
        let return_transport = transport.clone();
        let session = Session::new(
            ctx,
            catalog,
            request.limits,
            Rc::new(move |event| {
                let mut events = emit_events.borrow_mut();
                if !emit_transport.charge(&event, !events.is_empty()) {
                    return false;
                }
                events.push(event);
                true
            }),
            Rc::new(move || {
                // Includes native bridge work, such as each discovery document.
                stop_work.set(stop_work.get() + 1);
                if stop_work.get() > request.budget.work || stop_transport.exceeded.get() {
                    stop_interrupted.set(true);
                }
                stop_interrupted.get().then_some(Stop::TimedOut)
            }),
            Rc::new(move |output, text| {
                if output.truncated {
                    return !output_transport.exceeded.get();
                }
                let remaining = request
                    .limits
                    .output_bytes
                    .saturating_sub(output.text.len());
                let append = &text[..text.floor_char_boundary(remaining.min(text.len()))];
                let newline = usize::from(text.len() < remaining) * 2;
                let Some(prefix) = output_transport.check(&output.text) else {
                    return false;
                };
                let Some(suffix) = output_transport.check(&append) else {
                    return false;
                };
                let fits = prefix + suffix - 2 + newline <= output_transport.remaining.get();
                output_transport.exceeded.set(!fits);
                fits
            }),
            Rc::new(move |bytes| {
                if bytes > return_transport.remaining.get() {
                    return_transport.exceeded.set(true);
                }
                !return_transport.exceeded.get()
            }),
        );
        let root = session
            .install()
            .and_then(|()| session.evaluate(&request.code));
        let (root, mut failure) = match root {
            Ok(root) => (Some(root), None),
            Err(error) => (None, Some(session.classify(error))),
        };
        let mut jobs = 0;
        for index in 0..=request.prefix.len() {
            while !interrupted.get() {
                let result = execute_job(&session.ctx);
                if matches!(result, Ok(false)) {
                    break;
                }
                jobs += 1;
                work.set(work.get() + 1);
                if jobs > MAX_JOBS || work.get() > request.budget.work {
                    interrupted.set(true);
                    break;
                }
                if let Err(error) = result {
                    failure = Some(session.classify(error));
                    break;
                }
            }
            let stop = if interrupted.get() {
                Some(Stop::TimedOut)
            } else {
                failure
                    .take()
                    .or_else(|| root.as_ref().and_then(|root| session.boundary(root)))
            };
            let boundary = match stop {
                Some(Stop::Stalled) if unhandled.get() > 0 => Boundary::Done(session.outcome(
                    Stop::Failed(crate::report::ScriptDiagnostic {
                        message: "the script stalled with an unhandled promise rejection".into(),
                        line: None,
                        column: None,
                        stack: None,
                    }),
                )),
                Some(stop) => Boundary::Done(session.outcome(stop)),
                None => Boundary::Idle {
                    output: session.output(),
                },
            };
            let mut batch = EventBatch {
                events: Vec::new(),
                boundary,
            };
            if !transport.charge(&batch, !response.batches.is_empty()) {
                return Err(ReplayError::BudgetExceeded);
            }
            batch.events = std::mem::take(&mut *events.borrow_mut());
            if let Some(expected) = request.prefix.get(index) {
                if batch != expected.batch
                    || !matches!(batch.boundary, Boundary::Idle { .. })
                    || !session.has_pending(expected.reply.ordinal)
                {
                    return Err(ReplayError::Protocol);
                }
                if let Err(error) = session.settle(expected.reply.clone()) {
                    failure = Some(session.classify(error));
                }
            }
            response.batches.push(batch);
        }
        Ok(())
    })?;
    response.usage.work = work.get();
    bounded_json_with_depth(&response, cap, 128).map_err(|_| ReplayError::BudgetExceeded)
}
