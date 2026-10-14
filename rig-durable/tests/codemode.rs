#![cfg(feature = "codemode")]
#![allow(clippy::unwrap_used, clippy::panic)]

use rig::{
    test_utils::MockAddTool,
    tool::{ToolOutput, ToolResult, ToolSet},
};
use rig_durable::codemode::{CodeModeConfig, run};
use tokio::sync::Notify;

#[tokio::test]
async fn observed_completions_precede_results_that_arrive_during_planning() {
    let config = CodeModeConfig {
        limits: rig_codemode::Limits {
            max_in_flight: 3,
            ..Default::default()
        },
        definitions: ToolSet::from_tools(vec![MockAddTool]).tool_definitions(),
    };
    let release = Notify::new();
    let report = run(
        r#"
            const slow = tools.add({x: 1, y: 0});
            const first = tools.add({x: 2, y: 0});
            const second = tools.add({x: 3, y: 0});
            await first;
            return await Promise.race([slow, second]);
        "#
        .into(),
        config.limits,
        |input| {
            let config = &config;
            let release = &release;
            async move {
                if input.history.len() == 1 {
                    release.notify_one();
                }
                config.step(input).await
            }
        },
        |call| {
            let release = &release;
            async move {
                if call.ordinal == 0 {
                    release.notified().await;
                }
                Ok(ToolResult::success(ToolOutput::json(
                    call.arguments["x"].clone(),
                )))
            }
        },
    )
    .await
    .unwrap();
    assert_eq!(report.returned, Some(serde_json::json!(3)));
    assert_eq!(
        report.calls[0].delivery,
        rig_codemode::ScriptDelivery::Discarded
    );
}

#[tokio::test]
async fn planning_failure_drains_admitted_calls_and_checkpoints_their_results() {
    use std::{pin::pin, sync::Mutex, time::Duration};

    let config = CodeModeConfig {
        limits: rig_codemode::Limits {
            max_in_flight: 2,
            ..Default::default()
        },
        definitions: ToolSet::from_tools(vec![MockAddTool]).tool_definitions(),
    };
    let failed = Notify::new();
    let release = Notify::new();
    let completed = Mutex::new(Vec::new());
    let checkpoints = Mutex::new(Vec::new());
    let mut workflow = pin!(run(
        r#"
            await Promise.race([
                tools.add({x: 1, y: 0}),
                tools.add({x: 2, y: 0}),
                tools.add({x: 3, y: 0})
            ]);
            tools.add({x: 4, y: 0});
            return "done";
        "#
        .into(),
        config.limits,
        |input| {
            let config = &config;
            let failed = &failed;
            let checkpoints = &checkpoints;
            async move {
                checkpoints.lock().unwrap().push(input.clone());
                let result = config.step(input).await;
                if result.is_err() {
                    failed.notify_one();
                }
                result
            }
        },
        |call| {
            let release = &release;
            let completed = &completed;
            async move {
                if call.ordinal == 0 {
                    release.notified().await;
                }
                completed.lock().unwrap().push(call.ordinal);
                Ok(ToolResult::success(ToolOutput::json(
                    call.arguments["x"].clone(),
                )))
            }
        },
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            biased;
            result = &mut workflow => panic!("returned before admitted work finished: {result:?}"),
            _ = failed.notified() => {},
        }
        assert!(futures::poll!(workflow.as_mut()).is_pending());
        release.notify_one();
        let error = workflow.await.unwrap_err();
        assert!(error.contains("must await every new tool call"), "{error}");
    })
    .await
    .unwrap();
    let mut completed = completed.lock().unwrap();
    completed.sort_unstable();
    assert_eq!(*completed, [0, 1, 2]);
    let checkpoints = checkpoints.lock().unwrap();
    let final_checkpoint = checkpoints.last().unwrap();
    assert_eq!(final_checkpoint.admitted.len(), 3);
    assert_eq!(final_checkpoint.history.len(), 3);
    for item in &final_checkpoint.history {
        assert_eq!(
            item.result.output().as_json(),
            Some(&item.call.arguments["x"])
        );
    }
}

#[test]
fn scheduler_reopens_active_ready_and_failed_draining_work() {
    use rig_codemode::replay::{ScriptCall, ScriptStep};
    use rig_durable::codemode::{SchedulerAction, SchedulerState};

    let calls: Vec<_> = [3, 8, 20]
        .into_iter()
        .map(|ordinal| ScriptCall {
            ordinal,
            name: "add".into(),
            arguments: serde_json::json!({"x":ordinal,"y":0}),
            raw: false,
        })
        .collect();
    let mut state = SchedulerState::new(
        "script".into(),
        rig_codemode::Limits {
            max_in_flight: 2,
            ..Default::default()
        },
    )
    .unwrap();
    state.apply_plan(Ok(ScriptStep::Calls(calls.clone())));
    assert!(matches!(state.action(), SchedulerAction::Plan));
    assert!(
        state.admit_next().is_none(),
        "admissions must be replanned before effects"
    );
    state.apply_plan(Ok(ScriptStep::Calls(calls.clone())));
    assert_eq!(state.admit_next().unwrap().ordinal, 3);
    assert_eq!(state.admit_next().unwrap().ordinal, 8);
    let value = || Ok(ToolResult::success(ToolOutput::json(serde_json::json!(80))));
    assert!(state.complete(8, value()).unwrap());
    let snapshot = serde_json::to_value(&state).unwrap();
    let mut state: SchedulerState = serde_json::from_value(snapshot.clone()).unwrap();
    assert_eq!(serde_json::to_value(&state).unwrap(), snapshot);
    assert_eq!(state.active()[0].ordinal, 3);
    assert_eq!(state.ready()[0].call.ordinal, 8);
    assert_eq!(state.queued()[0].ordinal, 20);
    assert!(
        state.admit_next().is_none(),
        "ready results retain their slot after reopen"
    );
    assert!(!state.complete(8, Err("duplicate".into())).unwrap());
    assert!(state.complete(20, value()).is_err(), "queued is not active");
    assert!(state.complete(999, value()).is_err());
    assert_eq!(serde_json::to_value(&state).unwrap(), snapshot);
    assert_eq!(state.deliver_next().unwrap().call.ordinal, 8);
    assert!(
        !state.complete(8, value()).unwrap(),
        "delivered duplicates are ignored"
    );
    // Completion during planning occupies the retained FIFO, not a new slot.
    state.complete(3, value()).unwrap();
    state.apply_plan(Err("original planning failure".into()));
    let mut state: SchedulerState =
        serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
    assert!(state.is_draining());
    assert_eq!(state.failure(), Some("original planning failure"));
    assert_eq!(state.queued()[0].ordinal, 20);
    assert_eq!(state.admit_next().unwrap().ordinal, 20);
    assert!(state.admit_next().is_none());
    assert_eq!(state.deliver_next().unwrap().call.ordinal, 3);
    assert!(matches!(state.action(), SchedulerAction::Execute));
    assert!(state.deliver_next().is_none());
    state.complete(20, Err("nested failure".into())).unwrap();
    state.deliver_next().unwrap();
    assert_eq!(
        state
            .input()
            .history
            .iter()
            .map(|item| item.call.ordinal)
            .collect::<Vec<_>>(),
        [8, 3, 20]
    );
    assert!(
        matches!(state.action(), SchedulerAction::Checkpoint { error } if error == "original planning failure")
    );
    let mut state: SchedulerState =
        serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
    state.apply_plan(Ok(ScriptStep::Calls(calls)));
    assert!(
        state.admit_next().is_none(),
        "checkpoint cannot admit calls"
    );
    state.finish_checkpoint();
    assert!(
        matches!(state.action(), SchedulerAction::Complete(Err(error)) if error == "original planning failure")
    );
}

#[tokio::test]
async fn scheduler_replans_provisional_report_after_all_admitted_work_drains() {
    use rig_codemode::replay::ScriptStep;
    use rig_durable::codemode::{SchedulerAction, SchedulerState, StepInput};
    let config = CodeModeConfig {
        limits: rig_codemode::Limits {
            max_in_flight: 1,
            ..Default::default()
        },
        definitions: ToolSet::from_tools(vec![MockAddTool]).tool_definitions(),
    };
    let mut state = SchedulerState::new(
        "return await Promise.race([tools.add({x:1,y:0}),tools.add({x:2,y:0})]);".into(),
        config.limits,
    )
    .unwrap();
    for _ in 0..2 {
        let plan = config.step(state.input().clone()).await;
        state.apply_plan(plan);
    }
    assert_eq!(state.admit_next().unwrap().ordinal, 0);
    state
        .complete(
            0,
            Ok(ToolResult::success(ToolOutput::json(serde_json::json!(1)))),
        )
        .unwrap();
    state.deliver_next().unwrap();
    let provisional = config.step(state.input().clone()).await.unwrap();
    assert!(matches!(provisional, ScriptStep::Done(_)));
    state.apply_plan(Ok(provisional));
    assert!(state.is_draining());
    let mut state: SchedulerState =
        serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
    assert_eq!(state.admit_next().unwrap().ordinal, 1);
    state
        .complete(
            1,
            Ok(ToolResult::success(ToolOutput::json(serde_json::json!(2)))),
        )
        .unwrap();
    state.deliver_next().unwrap();
    assert!(matches!(state.action(), SchedulerAction::Plan));
    let StepInput { history, .. } = state.input();
    assert_eq!(history.len(), 2);
    state.apply_plan(config.step(state.input().clone()).await);
    let SchedulerAction::Complete(Ok(report)) = state.action() else {
        panic!("must finish after final plan")
    };
    assert_eq!(report.returned, Some(serde_json::json!(1)));
    assert_eq!(
        report.calls[1].delivery,
        rig_codemode::ScriptDelivery::Discarded
    );
}
