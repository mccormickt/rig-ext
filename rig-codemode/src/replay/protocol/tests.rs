#![allow(clippy::unwrap_used, clippy::panic, clippy::indexing_slicing)]

use super::*;
use crate::bridge::Payload;
use crate::replay::{ReplayFuture, ReplayPlanner, ScriptCall, SettledCall};
use crate::{CallStatus, Catalog, ExecutionStatus, ScriptDelivery};
use rig_core::tool::{ToolOutput, ToolResult};
use serde_json::json;

fn budget() -> ExchangeBudget {
    ExchangeBudget {
        bytes: 1_000_000,
        work: 10_000,
        elapsed: Duration::from_secs(5),
    }
}

fn catalog() -> Catalog {
    Catalog::new([CatalogEntry::new(
        "agent",
        "Delegate",
        json!({
            "type":"object", "properties":{"prompt":{"type":"string"}}, "required":["prompt"]
        }),
    )])
    .unwrap()
}

fn call(ordinal: u32, prompt: serde_json::Value) -> ScriptCall {
    ScriptCall {
        ordinal,
        name: "agent".into(),
        arguments: json!({"prompt":prompt}),
        raw: false,
    }
}

fn event(call: &ScriptCall) -> Request {
    Request::Call {
        ordinal: call.ordinal,
        name: call.name.clone(),
        arguments: call.arguments.to_string(),
        raw: call.raw,
    }
}

fn idle(events: Vec<Request>, text: &str) -> EventBatch {
    EventBatch {
        events,
        boundary: Boundary::Idle {
            output: ScriptOutput {
                text: text.into(),
                truncated: false,
            },
        },
    }
}

fn done(events: Vec<Request>, returned: serde_json::Value, text: &str) -> EventBatch {
    EventBatch {
        events,
        boundary: Boundary::Done(WorkerOutcome {
            status: ExecutionStatus::Completed,
            output: ScriptOutput {
                text: text.into(),
                truncated: false,
            },
            returned: Some(returned),
            diagnostic: None,
            over_budget_calls: 0,
        }),
    }
}

fn settled(call: &ScriptCall, answer: &str) -> SettledCall {
    SettledCall {
        call: call.clone(),
        result: ToolResult::success(ToolOutput::text(answer)),
    }
}

fn decode_response(
    response: RunnerResponse,
    max_bytes: usize,
    padding: usize,
) -> Result<RunnerResponse, ReplayError> {
    let mut bytes = serde_json::to_vec(&response).unwrap();
    bytes.extend(std::iter::repeat_n(b' ', padding));
    if bytes.len() > max_bytes {
        return Err(ReplayError::BudgetExceeded);
    }
    let mut response: RunnerResponse = serde_json::from_slice(&bytes).unwrap();
    response.usage.response_bytes = bytes.len();
    Ok(response)
}

#[derive(Clone, Copy)]
enum Fault {
    None,
    Arguments,
    Raw,
    Output,
    LostOutput,
    EarlyDone,
    ExtraBatch,
    Version,
    Duplicate,
    Work,
    Elapsed,
    Padding,
}

struct Transcript {
    batches: Vec<EventBatch>,
    fault: Fault,
}

impl ReplayRunner for Transcript {
    fn run<'a>(
        &'a self,
        request: RunnerRequest,
    ) -> WasmBoxedFuture<'a, Result<RunnerResponse, ReplayError>> {
        Box::pin(async move {
            let mut batches = self.batches[..=request.prefix.len()].to_vec();
            let mut usage = RunnerUsage {
                response_bytes: 0,
                work: 3,
                elapsed: Duration::from_millis(1),
            };
            let mut version = VERSION;
            // Faults in the second exchange must be rejected before its suffix is admitted.
            if !request.prefix.is_empty() {
                match self.fault {
                    Fault::Arguments => {
                        if let Request::Call { arguments, .. } = &mut batches[0].events[0] {
                            *arguments = "{}".into();
                        }
                    }
                    Fault::Raw => {
                        if let Request::Call { raw, .. } = &mut batches[0].events[0] {
                            *raw = !*raw;
                        }
                    }
                    Fault::Output => {
                        if let Boundary::Idle { output } = &mut batches[0].boundary {
                            output.text.push('!');
                        }
                    }
                    Fault::LostOutput => {
                        if let Boundary::Done(outcome) = &mut batches.last_mut().unwrap().boundary {
                            outcome.output.text.clear();
                        }
                    }
                    Fault::EarlyDone => {
                        batches.remove(0);
                    }
                    Fault::ExtraBatch => batches.push(batches[0].clone()),
                    Fault::Version => version += 1,
                    Fault::Duplicate => batches
                        .last_mut()
                        .unwrap()
                        .events
                        .push(event(&call(0, json!("slow")))),
                    Fault::Work => usage.work = request.budget.work + 1,
                    Fault::Elapsed => {
                        usage.elapsed = request.budget.elapsed + Duration::from_nanos(1)
                    }
                    Fault::None | Fault::Padding => {}
                }
            }
            decode_response(
                RunnerResponse {
                    version,
                    batches,
                    usage,
                },
                request.budget.bytes,
                if matches!(self.fault, Fault::Padding) {
                    2000
                } else {
                    0
                },
            )
        })
    }
}

struct Planner<R>(CheckedPlanner<R>);

impl<R: ReplayRunner> ReplayPlanner for Planner<R> {
    fn runtime_identity(&self) -> &str {
        "test/transcript-v1"
    }
    fn plan<'a>(&'a self, input: ReplayInput<'a>) -> ReplayFuture<'a> {
        Box::pin(self.0.plan(input))
    }
}

#[cfg_attr(target_family = "wasm", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_family = "wasm"), tokio::test)]
async fn portable_schema_race_preserves_admission_and_discarded_results() {
    let good = call(0, json!("valid"));
    let bad = call(1, json!(12));
    let catalog = catalog();
    let batches = vec![
        idle(vec![event(&good), event(&bad)], "before\n"),
        done(vec![], json!("invalid_args"), "before\nafter\n"),
    ];
    let planner: Box<dyn ReplayPlanner> = Box::new(Planner(CheckedPlanner::new(
        Transcript {
            batches,
            fault: Fault::None,
        },
        budget(),
    )));
    let mut input = ReplayInput {
        code: "fixture",
        catalog: &catalog,
        limits: Limits::default(),
        admitted: &[],
        history: &[],
    };
    let ScriptStep::Calls(calls) = planner.plan(input).await.unwrap() else {
        panic!()
    };
    assert_eq!(
        calls,
        vec![good.clone()],
        "valid parallel call is admitted before local rejection"
    );
    let history = [settled(&good, "recorded answer")];
    input.admitted = &calls;
    input.history = &history;
    let ScriptStep::Done(report) = planner.plan(input).await.unwrap() else {
        panic!()
    };
    assert_eq!(report.returned, Some(json!("invalid_args")));
    assert_eq!(
        report.output.text, "before\nafter\n",
        "prefix output must not accumulate twice"
    );
    assert_eq!(report.calls[0].delivery, ScriptDelivery::Discarded);
    assert_eq!(report.calls[1].delivery, ScriptDelivery::Delivered);
    assert_eq!(report.calls[1].status, CallStatus::Failed);
}

#[cfg_attr(target_family = "wasm", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_family = "wasm"), tokio::test)]
async fn checked_prefix_rejects_corruption_and_aggregate_budget_overruns() {
    let catalog = catalog();
    let slow = call(0, json!("slow"));
    let fast = call(1, json!("fast"));
    let admitted = [slow.clone(), fast.clone()];
    let history = [settled(&fast, "fast answer")];
    let input = ReplayInput {
        code: "fixture",
        catalog: &catalog,
        limits: Limits::default(),
        admitted: &admitted,
        history: &history,
    };
    let batches = vec![
        idle(vec![event(&slow), event(&fast)], "prefix\n"),
        done(vec![], json!("fast answer"), "prefix\n"),
    ];
    for fault in [
        Fault::None,
        Fault::Arguments,
        Fault::Raw,
        Fault::Output,
        Fault::LostOutput,
        Fault::EarlyDone,
        Fault::ExtraBatch,
        Fault::Version,
        Fault::Duplicate,
        Fault::Work,
        Fault::Elapsed,
    ] {
        let planner = Planner(CheckedPlanner::new(
            Transcript {
                batches: batches.clone(),
                fault,
            },
            budget(),
        ));
        let result = planner.plan(input).await;
        match fault {
            Fault::None => {
                let ScriptStep::Done(report) = result.unwrap() else {
                    panic!()
                };
                assert_eq!(report.returned, Some(json!("fast answer")));
                assert_eq!(report.calls[0].status, CallStatus::NotStarted);
                assert_eq!(report.calls[1].delivery, ScriptDelivery::Delivered);
            }
            Fault::Work | Fault::Elapsed => {
                assert!(matches!(result, Err(ReplayError::BudgetExceeded)))
            }
            Fault::Duplicate => assert!(matches!(result, Err(ReplayError::Diverged))),
            _ => assert!(matches!(result, Err(ReplayError::Protocol))),
        }
    }
    // Each exchange costs three units; five units cannot pay for two exchanges.
    for ceiling in [
        ExchangeBudget {
            work: 5,
            ..budget()
        },
        ExchangeBudget {
            bytes: 1,
            ..budget()
        },
        ExchangeBudget {
            work: 1 << 53,
            ..budget()
        },
    ] {
        let planner = Planner(CheckedPlanner::new(
            Transcript {
                batches: batches.clone(),
                fault: Fault::None,
            },
            ceiling,
        ));
        assert!(matches!(
            planner.plan(input).await,
            Err(ReplayError::BudgetExceeded)
        ));
    }
    let padded = Planner(CheckedPlanner::new(
        Transcript {
            batches: batches.clone(),
            fault: Fault::Padding,
        },
        ExchangeBudget {
            bytes: 5000,
            ..budget()
        },
    ));
    assert!(
        matches!(padded.plan(input).await, Err(ReplayError::BudgetExceeded)),
        "whitespace bytes must be charged across exchanges, not discarded during decoding"
    );
    // The request fits the aggregate budget even when it exceeds the remaining
    // response allowance. It must be charged once, not checked against both.
    let large_source = " ".repeat(8192);
    let planner = Planner(CheckedPlanner::new(
        Transcript {
            batches: vec![done(vec![], json!("small response"), "")],
            fault: Fault::None,
        },
        ExchangeBudget {
            bytes: 12_000,
            ..budget()
        },
    ));
    let ScriptStep::Done(report) = planner
        .plan(ReplayInput {
            code: &large_source,
            admitted: &[],
            history: &[],
            ..input
        })
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(report.returned, Some(json!("small response")));

    let duplicate = [history[0].clone(), history[0].clone()];
    let changed = [call(0, json!("changed")), fast];
    let planner = Planner(CheckedPlanner::new(
        Transcript {
            batches,
            fault: Fault::None,
        },
        budget(),
    ));
    for input in [
        ReplayInput {
            history: &duplicate,
            ..input
        },
        ReplayInput {
            admitted: &changed,
            ..input
        },
        ReplayInput {
            admitted: &[],
            ..input
        },
    ] {
        assert!(matches!(
            planner.plan(input).await,
            Err(ReplayError::Diverged)
        ));
    }
}

#[cfg_attr(target_family = "wasm", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_family = "wasm"), tokio::test)]
async fn terminal_new_calls_and_wire_errors_are_rejected() {
    let catalog = catalog();
    let planner = Planner(CheckedPlanner::new(
        Transcript {
            batches: vec![done(vec![event(&call(0, json!("new")))], json!(1), "")],
            fault: Fault::None,
        },
        budget(),
    ));
    assert!(matches!(
        planner
            .plan(ReplayInput {
                code: "fixture",
                catalog: &catalog,
                limits: Limits::default(),
                admitted: &[],
                history: &[]
            })
            .await,
        Err(ReplayError::UnawaitedCall)
    ));
    let payload = Payload::Reject(crate::bridge::ScriptError::size(
        "too large",
        Some("agent".into()),
    ));
    let wire = serde_json::to_value(&payload).unwrap();
    assert_eq!(
        serde_json::from_value::<Payload>(wire.clone()).unwrap(),
        payload
    );
    let mut corrupt = wire;
    corrupt["Reject"]["status"] = json!("unknown");
    assert!(serde_json::from_value::<Payload>(corrupt).is_err());

    let invalid =
        Catalog::new([CatalogEntry::new("agent", "", json!({"type":"not-a-type"}))]).unwrap();
    assert!(matches!(
        planner
            .plan(ReplayInput {
                code: "fixture",
                catalog: &invalid,
                limits: Limits::default(),
                admitted: &[],
                history: &[]
            })
            .await,
        Err(ReplayError::InputSchema(_))
    ));
}

#[cfg(target_family = "wasm")]
#[wasm_bindgen_test::wasm_bindgen_test]
async fn planner_allows_local_non_send_state_and_futures() {
    use std::{cell::Cell, rc::Rc};
    struct Local(Rc<Cell<bool>>);
    impl ReplayPlanner for Local {
        fn runtime_identity(&self) -> &str {
            "local-v1"
        }
        fn plan<'a>(&'a self, _: ReplayInput<'a>) -> ReplayFuture<'a> {
            let state = self.0.clone();
            Box::pin(async move {
                rig_core::wasm_compat::sleep(Duration::from_millis(1)).await;
                state.set(true);
                Ok(ScriptStep::Calls(vec![call(0, json!("local"))]))
            })
        }
    }
    let called = Rc::new(Cell::new(false));
    let planner: Box<dyn ReplayPlanner> = Box::new(Local(called.clone()));
    let catalog = catalog();
    let ScriptStep::Calls(calls) = planner
        .plan(ReplayInput {
            code: "fixture",
            catalog: &catalog,
            limits: Limits::default(),
            admitted: &[],
            history: &[],
        })
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(calls, vec![call(0, json!("local"))]);
    assert!(called.get());
}

#[cfg(all(feature = "quickjs", not(target_family = "wasm")))]
mod native {
    use super::*;
    use crate::bridge::{CancelOnDrop, WorkerConfig, channels};
    use std::{sync::Arc, time::Instant};

    // Real fresh QuickJS executions behind the same transport used by Workers.
    // Idle snapshots are empty in these fixtures; terminal output is real.
    struct FreshQuickJs;
    impl ReplayRunner for FreshQuickJs {
        fn run<'a>(
            &'a self,
            request: RunnerRequest,
        ) -> WasmBoxedFuture<'a, Result<RunnerResponse, ReplayError>> {
            Box::pin(async move {
                let started = Instant::now();
                let (worker, mut host) = channels(&request.limits);
                let handle = crate::runtime::quickjs::spawn(
                    WorkerConfig {
                        code: request.code,
                        catalog: Arc::new(Catalog::new(request.catalog).unwrap()),
                        limits: request.limits,
                        deadline: started + request.budget.elapsed,
                    },
                    worker,
                )
                .unwrap();
                let stop = CancelOnDrop(host.cancel.clone());
                let result = async {
                    let mut batches = Vec::new();
                    let mut events = Vec::new();
                    let mut open = true;
                    loop {
                        tokio::select! { biased;
                            event = host.requests.recv(), if open => match event {
                                None => open = false,
                                Some(Request::Idle) => {
                                    let batch = idle(std::mem::take(&mut events), "");
                                    if let Some(entry) = request.prefix.get(batches.len()) {
                                        if entry.batch != batch { return Err(ReplayError::Protocol); }
                                        host.replies.send(entry.reply.clone()).map_err(|_| ReplayError::WorkerStopped)?;
                                        batches.push(batch);
                                    } else {
                                        batches.push(batch);
                                        break;
                                    }
                                },
                                Some(event) => events.push(event),
                            },
                            outcome = &mut host.done => {
                                if batches.len() != request.prefix.len() { return Err(ReplayError::Protocol); }
                                batches.push(EventBatch { events, boundary: Boundary::Done(outcome.map_err(|_| ReplayError::WorkerStopped)?) });
                                break;
                            }
                        }
                    }
                    Ok(RunnerResponse { version: VERSION, usage: RunnerUsage { response_bytes: 0, work: batches.len() as u64, elapsed: started.elapsed() }, batches })
                }.await;
                drop(stop);
                drop(host.replies);
                handle.join().unwrap();
                result.and_then(|response| decode_response(response, request.budget.bytes, 0))
            })
        }
    }

    async fn compare(input: ReplayInput<'_>) -> Result<ScriptStep, ReplayError> {
        let native = crate::replay::step(
            input.code,
            input.catalog,
            input.limits,
            input.admitted,
            input.history,
        )
        .await;
        let portable = Planner(CheckedPlanner::new(FreshQuickJs, budget()))
            .plan(input)
            .await;
        match (&native, &portable) {
            (Ok(a), Ok(b)) => assert_eq!(
                serde_json::to_value(a).unwrap(),
                serde_json::to_value(b).unwrap()
            ),
            (Err(a), Err(b)) => assert_eq!(std::mem::discriminant(a), std::mem::discriminant(b)),
            _ => panic!("native={native:?}, portable={portable:?}"),
        }
        portable
    }

    #[tokio::test]
    async fn real_fresh_runs_match_native_sparse_races_schema_projection_and_rejections() {
        let catalog = catalog();
        for (code, order, expected) in [
            (
                "let inner;const outer=tools.agent({toJSON(){inner=tools.agent({prompt:'inner'});return {prompt:'outer'};}});return await Promise.all([outer,inner]);",
                vec![0, 1],
                json!(["answer-0", "answer-1"]),
            ),
            (
                "let inner;const outer=tools.agent({toJSON(){inner=tools.agent({prompt:'inner'});throw new Error('bad');}}).catch(e=>e.kind);return await Promise.all([outer,inner]);",
                vec![1],
                json!(["invalid_args", "answer-1"]),
            ),
            (
                "return await Promise.race([tools.agent({prompt:'slow'}),tools.agent({prompt:'fast'})]);",
                vec![1, 0],
                json!("answer-1"),
            ),
            (
                "const a=tools.agent({prompt:'slow'}),b=tools.agent({prompt:'fast'}); const c=Promise.race([a,b]).then(x=>tools.agent({prompt:x})); return await Promise.all([a,b,c]);",
                vec![1, 2, 0],
                json!(["answer-0", "answer-1", "answer-2"]),
            ),
            (
                "const good=tools.agent({prompt:'valid'}),bad=tools.agent({prompt:12}).catch(e=>e.kind); return await Promise.race([good,bad]);",
                vec![0],
                json!("invalid_args"),
            ),
            (
                "return await Promise.all([tools.agent({prompt:'valid'}),tools.agent({prompt:12}).catch(e=>e.kind)]);",
                vec![0],
                json!(["answer-0", "invalid_args"]),
            ),
            (
                "const a=await tools.agent({prompt:'first'}); return await Promise.resolve().then(()=>tools.agent({prompt:a}));",
                vec![0, 1],
                json!("answer-1"),
            ),
            (
                "try { await tools.agent({prompt:12}); } catch(e) { return e.kind; }",
                vec![],
                json!("invalid_args"),
            ),
            (
                "return await tools.agent({prompt:'value'}).then(x=>{text(x); return x;});",
                vec![0],
                json!("answer-0"),
            ),
        ] {
            let mut admitted = Vec::new();
            let mut history = Vec::new();
            for ordinal in order {
                let planned = compare(ReplayInput {
                    code,
                    catalog: &catalog,
                    limits: Limits::default(),
                    admitted: &admitted,
                    history: &history,
                })
                .await
                .unwrap();
                if let ScriptStep::Calls(calls) = planned {
                    for call in calls {
                        if !admitted
                            .iter()
                            .any(|old: &ScriptCall| old.ordinal == call.ordinal)
                        {
                            admitted.push(call);
                        }
                    }
                }
                let call = admitted
                    .iter()
                    .find(|call| call.ordinal == ordinal)
                    .unwrap();
                history.push(settled(call, &format!("answer-{ordinal}")));
            }
            let ScriptStep::Done(report) = compare(ReplayInput {
                code,
                catalog: &catalog,
                limits: Limits::default(),
                admitted: &admitted,
                history: &history,
            })
            .await
            .unwrap() else {
                panic!()
            };
            assert_eq!(report.returned, Some(expected));
        }
        for code in [
            "tools.agent({prompt:'unawaited'});return 1;",
            "return Date.now();",
            "return Math.random();",
            "return new Promise(()=>{});",
            "try{await tools.agent(Symbol());}catch(e){return e.kind;}",
        ] {
            let result = compare(ReplayInput {
                code,
                catalog: &catalog,
                limits: Limits::default(),
                admitted: &[],
                history: &[],
            })
            .await;
            if code.starts_with("tools.agent") {
                assert!(matches!(result, Err(ReplayError::UnawaitedCall)));
            }
        }

        for (raw, result, expected) in [
            (
                false,
                ToolResult::success(ToolOutput::text("x".repeat(200))),
                "size",
            ),
            (
                false,
                ToolResult::failed(
                    rig_core::tool::ToolExecutionError::refused("private")
                        .with_model_feedback("safe"),
                ),
                "denied",
            ),
            (true, ToolResult::skipped("skip"), "skipped"),
        ] {
            let code = if raw {
                "return (await tools.agent.raw({prompt:'value'})).status;"
            } else {
                "try{return await tools.agent({prompt:'value'});}catch(e){return e.status;}"
            };
            let admitted = [ScriptCall {
                raw,
                ..call(0, json!("value"))
            }];
            let history = [SettledCall {
                call: admitted[0].clone(),
                result,
            }];
            let ScriptStep::Done(report) = compare(ReplayInput {
                code,
                catalog: &catalog,
                limits: Limits {
                    message_bytes: 128,
                    ..Limits::default()
                },
                admitted: &admitted,
                history: &history,
            })
            .await
            .unwrap() else {
                panic!()
            };
            assert_eq!(report.returned, Some(json!(expected)));
        }
        for depth in [65, 123, 124, 127] {
            let code = format!("let value=0;for(let i=0;i<{depth};i++)value=[value];return value;");
            let ScriptStep::Done(report) = compare(ReplayInput {
                code: &code,
                catalog: &catalog,
                limits: Limits::default(),
                admitted: &[],
                history: &[],
            })
            .await
            .unwrap() else {
                panic!()
            };
            assert_eq!(
                serde_json::to_string(&report.returned.unwrap()).unwrap(),
                format!("{}0{}", "[".repeat(depth), "]".repeat(depth))
            );
        }
    }
}
