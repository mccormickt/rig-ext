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
