//! Replay-only QuickJS-WASM planner in a fresh Dynamic Worker per exchange.
//!
//! This backend has no dispatcher and does not change `CodeMode::execute`.
//! Persist the planner's runtime identity with source, catalog, and limits.
//! An engine or asset change requires an explicit history migration.

use std::time::Duration;

const MAX_TRANSPORT: usize = 16 * 1024 * 1024;
const COMPATIBILITY_DATE: &str = "2026-07-01";
const CPU_MS: u32 = 100;
const SUBREQUESTS: u32 = 0;

/// Aggregate ceilings for one planning invocation, including repeated prefixes.
#[derive(Debug, Clone, Copy)]
pub struct ReplayBudget {
    /// Total encoded request and received response bytes across exchanges.
    pub bytes: usize,
    /// Total QuickJS interrupts and executed jobs across exchanges.
    pub work: u64,
    /// Full host-observed startup, fetch, and body-consumption time.
    pub elapsed: Duration,
}

impl Default for ReplayBudget {
    fn default() -> Self {
        Self {
            bytes: 64 * 1024 * 1024,
            work: 1_000_000,
            elapsed: Duration::from_secs(30),
        }
    }
}

/// Full immutable planner identity: assets, adapter profile, and aggregate limits.
/// Use the planner's `ReplayPlanner::runtime_identity()` when persisting history.
pub fn runtime_identity(budget: ReplayBudget) -> String {
    format!(
        "{}:adapter-v3:compat={COMPATIBILITY_DATE}:cpu={CPU_MS}:subrequests={SUBREQUESTS}:transport={MAX_TRANSPORT}:bytes={}:work={}:elapsed-ns={}",
        include_str!("../runtime/identity.txt").trim(),
        budget.bytes,
        budget.work,
        budget.elapsed.as_nanos(),
    )
}

// Decode the envelope without interpreting guest JSON. A valid envelope can
// carry a return string that serde_json cannot represent (depth or surrogates).
// Keep this separate from WorkerOutcome's ordinary native wire adapter.
#[cfg(any(test, all(target_arch = "wasm32", target_os = "unknown")))]
fn decode_response(
    body: &[u8],
    limits: crate::Limits,
) -> Result<crate::replay::protocol::RunnerResponse, crate::replay::ReplayError> {
    use crate::bridge::{Request, WorkerOutcome};
    use crate::replay::protocol::{Boundary, EventBatch, RunnerResponse, RunnerUsage};
    use crate::{ExecutionStatus, ScriptDiagnostic, ScriptOutput};
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Envelope {
        version: u32,
        batches: Vec<Batch>,
        usage: RunnerUsage,
    }
    #[derive(Deserialize)]
    struct Batch {
        events: Vec<Request>,
        boundary: WireBoundary,
    }
    #[derive(Deserialize)]
    enum WireBoundary {
        Idle { output: ScriptOutput },
        Done(WireOutcome),
    }
    #[derive(Deserialize)]
    struct WireOutcome {
        status: ExecutionStatus,
        output: ScriptOutput,
        // Like the native serde adapter, require the field even when null.
        #[serde(deserialize_with = "return_text")]
        returned: Option<String>,
        diagnostic: Option<ScriptDiagnostic>,
        over_budget_calls: u32,
    }

    fn return_text<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<String>, D::Error> {
        Option::<String>::deserialize(deserializer)
    }

    let envelope: Envelope =
        serde_json::from_slice(body).map_err(|_| crate::replay::ReplayError::Protocol)?;
    Ok(RunnerResponse {
        version: envelope.version,
        usage: envelope.usage,
        batches: envelope
            .batches
            .into_iter()
            .map(|batch| EventBatch {
                events: batch.events,
                boundary: match batch.boundary {
                    WireBoundary::Idle { output } => Boundary::Idle { output },
                    WireBoundary::Done(wire) => {
                        let mut outcome = WorkerOutcome {
                            status: wire.status,
                            output: wire.output,
                            returned: None,
                            diagnostic: wire.diagnostic,
                            over_budget_calls: wire.over_budget_calls,
                        };
                        if let Some(json) = wire.returned {
                            match serde_json::from_str(&json) {
                                Ok(value) => outcome.returned = Some(value),
                                Err(_) => {
                                    outcome.status = ExecutionStatus::ScriptError;
                                    let message = "the return value is not valid JSON";
                                    let bound =
                                        limits.message_bytes.min(limits.output_bytes).min(8192);
                                    outcome.diagnostic = Some(ScriptDiagnostic {
                                        message: message[..message.len().min(bound)].into(),
                                        line: None,
                                        column: None,
                                        stack: None,
                                    });
                                }
                            }
                        }
                        Boundary::Done(outcome)
                    }
                },
            })
            .collect(),
    })
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use wasm::WorkerReplayPlanner;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod wasm {
    use super::{
        COMPATIBILITY_DATE, CPU_MS, MAX_TRANSPORT, ReplayBudget, SUBREQUESTS, runtime_identity,
    };
    use crate::replay::protocol::{
        CheckedPlanner, ExchangeBudget, ReplayRunner, RunnerRequest, RunnerResponse,
    };
    use crate::replay::{ReplayError, ReplayFuture, ReplayInput, ReplayPlanner};
    use futures_util::future::{Either, select};
    use rig_celld::{OutboundPolicy, WorkerCode, WorkerLimits, WorkerLoader, WorkerModule};
    use rig_core::wasm_compat::WasmBoxedFuture;
    use std::collections::BTreeMap;
    use std::time::Duration;
    use worker::{Delay, Env, Method, Request, RequestInit, js_sys::Date, wasm_bindgen::JsValue};

    const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

    /// A Workers planner. Every exchange loads an anonymous, disposable Worker.
    pub struct WorkerReplayPlanner {
        planner: CheckedPlanner<WorkerRunner>,
        identity: String,
    }

    impl WorkerReplayPlanner {
        /// Read a Loader binding. No tool bindings or environment are inherited.
        pub fn from_env(env: &Env, binding: &str, budget: ReplayBudget) -> worker::Result<Self> {
            Ok(Self::new(WorkerLoader::from_env(env, binding)?, budget))
        }

        /// Use an existing Loader binding with fixed aggregate budgets.
        pub fn new(loader: WorkerLoader, budget: ReplayBudget) -> Self {
            Self {
                identity: runtime_identity(budget),
                planner: CheckedPlanner::new(
                    WorkerRunner { loader },
                    ExchangeBudget {
                        bytes: budget.bytes,
                        work: budget.work,
                        elapsed: budget.elapsed,
                    },
                ),
            }
        }
    }

    impl ReplayPlanner for WorkerReplayPlanner {
        fn runtime_identity(&self) -> &str {
            &self.identity
        }

        fn plan<'a>(&'a self, input: ReplayInput<'a>) -> ReplayFuture<'a> {
            Box::pin(self.planner.plan(input))
        }
    }

    struct WorkerRunner {
        loader: WorkerLoader,
    }

    impl ReplayRunner for WorkerRunner {
        fn run<'a>(
            &'a self,
            request: RunnerRequest,
        ) -> WasmBoxedFuture<'a, Result<RunnerResponse, ReplayError>> {
            Box::pin(async move {
                if request.budget.work > MAX_SAFE_INTEGER
                    || request.budget.bytes as u128 > MAX_SAFE_INTEGER as u128
                    || request.budget.elapsed.is_zero()
                    || request.budget.elapsed.as_millis() > i32::MAX as u128
                {
                    return Err(ReplayError::BudgetExceeded);
                }
                let started = Date::now();
                let body = crate::projection::bounded_json_with_depth(&request, MAX_TRANSPORT, 128)
                    .map_err(|_| ReplayError::BudgetExceeded)?;
                let mut init = RequestInit::new();
                init.with_method(Method::Post)
                    .with_body(Some(JsValue::from_str(&body)));
                let transport = Request::new_with_init("https://replay.invalid/exchange", &init)
                    .map_err(|error| ReplayError::Runner(error.to_string()))?;
                let loaded = self
                    .loader
                    .load(WorkerCode {
                        main_module: "worker.js".into(),
                        modules: BTreeMap::from([
                            (
                                "worker.js".into(),
                                WorkerModule::JavaScript(
                                    include_str!("../runtime/worker.js").into(),
                                ),
                            ),
                            (
                                "quickjs.wasm".into(),
                                WorkerModule::Wasm(
                                    include_bytes!("../runtime/quickjs.wasm").to_vec(),
                                ),
                            ),
                        ]),
                        compatibility_date: COMPATIBILITY_DATE.into(),
                        compatibility_flags: Vec::new(),
                        outbound: OutboundPolicy::Deny,
                        limits: WorkerLimits {
                            cpu_ms: CPU_MS,
                            subrequests: SUBREQUESTS,
                        },
                    })
                    .map_err(|error| ReplayError::Runner(error.to_string()))?;
                let remaining = request.budget.elapsed.as_secs_f64() * 1000.0
                    - (Date::now() - started).max(0.0);
                if remaining <= 0.0 {
                    return Err(ReplayError::BudgetExceeded);
                }
                let exchange = Box::pin(async {
                    let response = loaded
                        .fetch_bounded(None, transport, MAX_TRANSPORT.min(request.budget.bytes))
                        .await
                        .map_err(|error| ReplayError::Runner(error.to_string()))?;
                    if response.status != 200 {
                        if response.status == 429 {
                            return Err(ReplayError::BudgetExceeded);
                        }
                        return Err(ReplayError::Protocol);
                    }
                    let mut decoded = super::decode_response(&response.body, request.limits)?;
                    // An I/O checkpoint advances clocks frozen during Workers CPU work.
                    Delay::from(Duration::from_millis(1)).await;
                    let elapsed = (Date::now() - started).max(0.0);
                    if !elapsed.is_finite()
                        || elapsed > request.budget.elapsed.as_secs_f64() * 1000.0
                    {
                        return Err(ReplayError::BudgetExceeded);
                    }
                    decoded.usage.response_bytes = response.body.len();
                    decoded.usage.elapsed = Duration::from_secs_f64(elapsed / 1000.0);
                    Ok(decoded)
                });
                let timer = Box::pin(Delay::from(Duration::from_secs_f64(remaining / 1000.0)));
                match select(exchange, timer).await {
                    Either::Left((response, _)) => response,
                    Either::Right(_) => Err(ReplayError::BudgetExceeded),
                }
            })
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    fn envelope(returned: serde_json::Value) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "batches": [{"events": [], "boundary": {"Done": {
                "status": "completed", "output": {"text": "before\n", "truncated": false},
                "returned": returned, "diagnostic": null, "over_budget_calls": 2
            }}}],
            "usage": {"response_bytes": 17, "work": 23, "elapsed": {"secs": 0, "nanos": 29}}
        }))
        .unwrap()
    }

    #[test]
    fn guest_return_conversion_is_not_envelope_decoding() {
        use crate::replay::protocol::{Boundary, RunnerResponse};
        for json in [
            r#""\uD800""#.to_owned(),
            format!("{}0{}", "[".repeat(128), "]".repeat(128)),
        ] {
            let body = envelope(json.into());
            // Native wire deserialization rejects unrepresentable guest JSON.
            assert!(serde_json::from_slice::<RunnerResponse>(&body).is_err());
            let response = decode_response(&body, crate::Limits::default()).unwrap();
            assert_eq!(response.usage.work, 23);
            assert_eq!(response.usage.response_bytes, 17);
            assert_eq!(response.usage.elapsed, Duration::from_nanos(29));
            let Boundary::Done(outcome) = &response.batches[0].boundary else {
                panic!()
            };
            assert_eq!(outcome.status, crate::ExecutionStatus::ScriptError);
            assert_eq!(outcome.returned, None);
            assert_eq!(outcome.output.text, "before\n");
            assert_eq!(outcome.over_budget_calls, 2);
            let diagnostic = outcome.diagnostic.as_ref().unwrap();
            assert_eq!(diagnostic.message, "the return value is not valid JSON");
            assert_eq!(
                (diagnostic.line, diagnostic.column, &diagnostic.stack),
                (None, None, &None)
            );
        }
    }

    #[test]
    fn valid_guest_return_preserves_native_wire_and_depth_127() {
        use crate::replay::protocol::RunnerResponse;
        for returned in [
            serde_json::Value::Null,
            "null".into(),
            "123".into(),
            format!("{}0{}", "[".repeat(127), "]".repeat(127)).into(),
        ] {
            let body = envelope(returned);
            let old: RunnerResponse = serde_json::from_slice(&body).unwrap();
            let new = decode_response(&body, crate::Limits::default()).unwrap();
            assert_eq!(old.batches, new.batches);
            assert_eq!(
                serde_json::to_vec(&old).unwrap(),
                serde_json::to_vec(&new).unwrap()
            );
        }
    }

    #[test]
    fn malformed_envelope_remains_protocol_and_conversion_diagnostic_is_bounded() {
        let mut missing: serde_json::Value =
            serde_json::from_slice(&envelope("123".into())).unwrap();
        missing["batches"][0]["boundary"]["Done"]
            .as_object_mut()
            .unwrap()
            .remove("returned");
        for body in [
            b"{".to_vec(),
            envelope(serde_json::json!([1])),
            serde_json::to_vec(&missing).unwrap(),
            envelope("123".into()).into_iter().take(40).collect(),
        ] {
            assert!(matches!(
                decode_response(&body, crate::Limits::default()),
                Err(crate::replay::ReplayError::Protocol)
            ));
        }
        let limits = crate::Limits {
            output_bytes: 7,
            ..crate::Limits::default()
        };
        let response = decode_response(&envelope(r#""\uD800""#.into()), limits).unwrap();
        let crate::replay::protocol::Boundary::Done(outcome) = &response.batches[0].boundary else {
            panic!()
        };
        assert_eq!(outcome.diagnostic.as_ref().unwrap().message, "the ret");
    }

    #[test]
    fn identity_versions_each_aggregate_ceiling() {
        let budget = ReplayBudget::default();
        let identity = runtime_identity(budget);
        for changed in [
            ReplayBudget {
                bytes: budget.bytes - 1,
                ..budget
            },
            ReplayBudget {
                work: budget.work - 1,
                ..budget
            },
            ReplayBudget {
                elapsed: budget.elapsed - Duration::from_nanos(1),
                ..budget
            },
        ] {
            assert_ne!(identity, runtime_identity(changed));
        }
        assert_eq!(identity, runtime_identity(budget));
        assert!(identity.contains(":adapter-v3:compat=2026-07-01:cpu=100:subrequests=0:"));
    }
}
