#![cfg(all(feature = "wasmtime-replay", not(target_family = "wasm")))]
#![allow(clippy::unwrap_used, clippy::panic)]

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use rig_codemode::replay::{
    ReplayError, ReplayInput, ReplayPlanner, ScriptCall, ScriptStep, SettledCall,
};
use rig_codemode::wasmtime_replay::{WasmtimeBudget, WasmtimeReplayPlanner};
use rig_codemode::{Catalog, CatalogEntry, ExecutionStatus, Limits, ScriptDelivery};
use rig_core::tool::ToolResult;
use serde_json::json;

fn planner() -> &'static WasmtimeReplayPlanner {
    static PLANNER: OnceLock<WasmtimeReplayPlanner> = OnceLock::new();
    PLANNER.get_or_init(|| WasmtimeReplayPlanner::new(WasmtimeBudget::default()).unwrap())
}

fn input<'a>(code: &'a str, catalog: &'a Catalog) -> ReplayInput<'a> {
    ReplayInput {
        code,
        catalog,
        limits: Limits::default(),
        admitted: &[],
        history: &[],
    }
}

fn done(step: ScriptStep) -> rig_codemode::ExecutionReport {
    let ScriptStep::Done(report) = step else {
        panic!("expected completion")
    };
    report
}

#[tokio::test]
async fn embedded_engine_confines_scripts_and_reconstructs_sparse_races() {
    let planner = planner();
    let catalog = Catalog::new(["slow", "fast", "follow"].map(|name| {
        CatalogEntry::new(
            name,
            "test tool",
            json!({"type":"object","properties":{"n":{"type":"integer"}},"required":["n"]}),
        )
    }))
    .unwrap();
    let code = "const winner=await Promise.race([tools.slow({n:1}),tools.fast({n:2})]);text(winner);return await tools.follow({n:winner+1});";
    let mut request = input(code, &catalog);
    let ScriptStep::Calls(mut admitted) = planner.plan(request).await.unwrap() else {
        panic!()
    };
    assert_eq!(
        admitted.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        ["slow", "fast"]
    );
    let mut history = vec![SettledCall {
        call: admitted[1].clone(),
        result: ToolResult::success(json!(7).into()),
    }];
    request.admitted = &admitted;
    request.history = &history;
    let ScriptStep::Calls(calls) = planner.plan(request).await.unwrap() else {
        panic!()
    };
    assert_eq!(
        calls.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        ["slow", "follow"]
    );
    assert_eq!(calls[1].arguments, json!({"n":8}));
    admitted.push(calls[1].clone());
    history.push(SettledCall {
        call: calls[1].clone(),
        result: ToolResult::success(json!(9).into()),
    });
    history.push(SettledCall {
        call: admitted[0].clone(),
        result: ToolResult::success(json!(-1).into()),
    });
    let report = done(
        planner
            .plan(ReplayInput {
                admitted: &admitted,
                history: &history,
                ..input(code, &catalog)
            })
            .await
            .unwrap(),
    );
    assert_eq!(report.status, ExecutionStatus::Completed);
    assert_eq!(report.returned, Some(json!(9)));
    assert_eq!(report.output.text, "7\n");
    assert_eq!(report.calls[0].delivery, ScriptDelivery::Discarded);
    history[0].call.arguments = json!({"n":999});
    assert!(matches!(
        planner
            .plan(ReplayInput {
                admitted: &admitted,
                history: &history,
                ..input(code, &catalog)
            })
            .await,
        Err(ReplayError::Diverged)
    ));

    let invalid = "await Promise.allSettled([tools.slow({n:'invalid'}),tools.fast({n:3})]);";
    let ScriptStep::Calls(calls) = planner.plan(input(invalid, &catalog)).await.unwrap() else {
        panic!()
    };
    assert_eq!(calls.len(), 1);
    assert_eq!((calls[0].ordinal, calls[0].name.as_str()), (1, "fast"));

    let report = done(planner.plan(input("globalThis.marker=1;return [typeof fetch,typeof process,typeof require,typeof WebAssembly,typeof Date,typeof __host,Function('return typeof fetch')(),Object.isFrozen(tools)]", &catalog)).await.unwrap());
    assert_eq!(
        report.returned,
        Some(json!([
            "undefined",
            "undefined",
            "undefined",
            "undefined",
            "undefined",
            "undefined",
            "undefined",
            true
        ]))
    );
    let report = done(
        planner
            .plan(input("return typeof marker", &catalog))
            .await
            .unwrap(),
    );
    assert_eq!(report.returned, Some(json!("undefined")));
}

#[tokio::test]
async fn rust_bindings_keep_descriptors_and_discovery_coercion() {
    let catalog = Catalog::new(
        ["one", "two", "three"]
            .map(|name| CatalogEntry::new(name, "test tool", json!({"type":"object"}))),
    )
    .unwrap();
    let code = r#"
        const fixed = ['tools', 'text', 'searchTools', 'describeTool'].every(name => {
            const d = Object.getOwnPropertyDescriptor(globalThis, name);
            return d.enumerable && !d.writable && !d.configurable;
        });
        const frozen = Object.isFrozen(tools) && Object.isFrozen(tools.one)
            && Object.isFrozen(tools.one.raw) && Object.getPrototypeOf(tools) === null;
        let reads = 0;
        const count = searchTools('test', {get limit() { reads++; return 2.9; }}).length;
        Number.prototype.limit = 1;
        globalThis.Object = () => { throw Error('guest Object must not be called'); };
        const boxed = searchTools('test', 3).length;
        const named = describeTool({toString() {return 'two';}}).name;
        return [fixed, frozen, reads, count, boxed, named];
    "#;
    for _ in 0..3 {
        let report = done(planner().plan(input(code, &catalog)).await.unwrap());
        assert_eq!(report.status, ExecutionStatus::Completed, "{report:?}");
        assert_eq!(report.returned, Some(json!([true, true, 3, 2, 1, "two"])));
    }
    let ScriptStep::Calls(calls) = planner()
        .plan(input(
            "await Promise.all([tools.one(), tools.two.raw(undefined)])",
            &catalog,
        ))
        .await
        .unwrap()
    else {
        panic!("expected calls")
    };
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].arguments, json!({}));
    assert_eq!(calls[1].arguments, json!({}));
    assert!(!calls[0].raw);
    assert!(calls[1].raw);
}

#[tokio::test]
async fn wasm_limits_and_invalid_returns_do_not_poison_the_planner() {
    let catalog = Catalog::default();
    for code in [
        "return '\\uD800'",
        "return Array(128).fill(0).reduce(x=>[x],0)",
    ] {
        let report = done(planner().plan(input(code, &catalog)).await.unwrap());
        assert_eq!(report.status, ExecutionStatus::ScriptError, "{report:?}");
    }
    for code in [
        "return 'x'.repeat(16 * 1024 * 1024)",
        "function f(){return f()} f()",
    ] {
        let report = done(
            planner()
                .plan(ReplayInput {
                    limits: Limits {
                        memory_bytes: 8 * 1024 * 1024,
                        ..Limits::default()
                    },
                    ..input(code, &catalog)
                })
                .await
                .unwrap(),
        );
        assert_eq!(report.status, ExecutionStatus::ScriptError, "{report:?}");
    }
    for budget in [
        WasmtimeBudget {
            fuel: 1,
            ..WasmtimeBudget::default()
        },
        WasmtimeBudget {
            bytes: 1,
            ..WasmtimeBudget::default()
        },
    ] {
        let limited = WasmtimeReplayPlanner::new(budget).unwrap();
        assert!(matches!(
            limited.plan(input("return 1", &catalog)).await,
            Err(ReplayError::BudgetExceeded)
        ));
        assert_ne!(limited.runtime_identity(), planner().runtime_identity());
    }
    let limited = WasmtimeReplayPlanner::new(WasmtimeBudget {
        memory_bytes: 1024 * 1024,
        ..WasmtimeBudget::default()
    })
    .unwrap();
    assert!(limited.plan(input("return 1", &catalog)).await.is_err());
    let report = done(
        planner()
            .plan(input("text('héllo');return 42", &catalog))
            .await
            .unwrap(),
    );
    assert_eq!(report.returned, Some(json!(42)));
    assert_eq!(report.output.text, "héllo\n");
}

#[tokio::test]
async fn fuel_is_shared_across_reconstructed_prefixes() {
    let limited = WasmtimeReplayPlanner::new(WasmtimeBudget {
        fuel: 5_000_000,
        ..WasmtimeBudget::default()
    })
    .unwrap();
    let catalog =
        Catalog::new([CatalogEntry::new("echo", "echo", json!({"type":"object"}))]).unwrap();
    let code = "for(let n=0;n<12;n++) await tools.echo({n});return 13;";
    assert!(matches!(
        limited.plan(input(code, &catalog)).await.unwrap(),
        ScriptStep::Calls(_)
    ));
    let admitted: Vec<_> = (0..12)
        .map(|ordinal| ScriptCall {
            ordinal,
            name: "echo".into(),
            arguments: json!({"n":ordinal}),
            raw: false,
        })
        .collect();
    let history: Vec<_> = admitted
        .iter()
        .map(|call| SettledCall {
            call: call.clone(),
            result: ToolResult::success(json!(call.ordinal).into()),
        })
        .collect();
    let request = ReplayInput {
        admitted: &admitted,
        history: &history,
        ..input(code, &catalog)
    };
    assert_eq!(
        done(planner().plan(request).await.unwrap()).returned,
        Some(json!(13))
    );
    assert!(matches!(
        limited.plan(request).await,
        Err(ReplayError::BudgetExceeded)
    ));
}

#[tokio::test]
async fn host_deadline_and_drop_interrupt_wasm_builtins() {
    let planner = WasmtimeReplayPlanner::new(WasmtimeBudget {
        fuel: 1_000_000_000_000,
        elapsed: Duration::from_millis(50),
        ..WasmtimeBudget::default()
    })
    .unwrap();
    let catalog = Catalog::default();
    let code = "return 'a'.repeat(3000000).indexOf('a'.repeat(100000)+'b')";
    let started = Instant::now();
    assert!(matches!(
        planner.plan(input(code, &catalog)).await,
        Err(ReplayError::BudgetExceeded)
    ));
    assert!(started.elapsed() < Duration::from_secs(2));
    // A timer on this single-thread executor must run while WASM computes.
    tokio::select! {
        result = planner.plan(input(code, &catalog)) => panic!("completed before cancellation: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(5)) => {}
    }
    let report = done(planner.plan(input("return 17", &catalog)).await.unwrap());
    assert_eq!(report.returned, Some(json!(17)));
}
