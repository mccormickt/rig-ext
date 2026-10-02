//! Replay real histories recorded with the pre-policy baseline.

#[cfg(feature = "temporal")]
#[tokio::test]
async fn temporal_replays_recorded_retries_approval_and_steering() {
    use rig_durable::temporal::{TemporalAgentSessionWorkflow, TemporalAgentWorkflow};
    use temporalio_client::WorkflowHistory;
    use temporalio_sdk::workflow_replayer::{WorkflowReplayer, WorkflowReplayerOptions};

    let mut options = WorkflowReplayerOptions::new().build();
    options
        .register_workflow::<TemporalAgentWorkflow>()
        .unwrap();
    options
        .register_workflow::<TemporalAgentSessionWorkflow>()
        .unwrap();
    let replayer = WorkflowReplayer::new(options).unwrap();
    for bytes in [
        include_bytes!("fixtures/legacy/temporal-retry.json").as_slice(),
        include_bytes!("fixtures/legacy/temporal-approval.json").as_slice(),
        include_bytes!("fixtures/legacy/temporal-steering.json").as_slice(),
    ] {
        replayer
            .replay_workflow(WorkflowHistory::from_json(bytes).unwrap())
            .await
            .unwrap();
    }
}

#[cfg(feature = "duroxide")]
#[tokio::test]
async fn duroxide_replays_each_recorded_continuation_window() {
    use duroxide::{
        Event, EventKind, RetryPolicy,
        runtime::replay_engine::{ReplayEngine, TurnResult},
    };
    use rig::{test_utils::MockAddTool, tool::ToolSet};
    use rig_durable::{
        CheckpointConfig, CheckpointPolicy, DurableAgentConfig, catalog_from_toolset,
        orchestration_registry,
    };
    use std::num::NonZeroU32;

    let config = DurableAgentConfig {
        tools: catalog_from_toolset(&ToolSet::from_tools(vec![MockAddTool]), RetryPolicy::new(1))
            .await,
        checkpoint: CheckpointConfig {
            policy: CheckpointPolicy::Every(NonZeroU32::new(1).unwrap()),
            target_version: None,
        },
        ..Default::default()
    };
    let registry = orchestration_registry(config);
    for bytes in [
        include_bytes!("fixtures/legacy/duroxide-continuation-1.json").as_slice(),
        include_bytes!("fixtures/legacy/duroxide-continuation-2.json").as_slice(),
        include_bytes!("fixtures/legacy/duroxide-continuation-3.json").as_slice(),
    ] {
        let mut history: Vec<Event> = serde_json::from_slice(bytes).unwrap();
        let terminal = history.pop().unwrap();
        let EventKind::OrchestrationStarted {
            name,
            version,
            input,
            ..
        } = history[0].kind.clone()
        else {
            panic!("fixture must start with orchestration input")
        };
        let (_, handler) = registry.resolve_handler(&name).unwrap();
        let mut replay = ReplayEngine::new("checkpointed".into(), terminal.execution_id, history);
        let result = replay.execute_orchestration(handler, input, name, version, "fixture-replay");
        match (terminal.kind, result) {
            (
                EventKind::OrchestrationContinuedAsNew {
                    input: expected, ..
                },
                TurnResult::ContinueAsNew { input, .. },
            ) => {
                // Check all recorded fields, including the complete Rig state.
                let expected: serde_json::Value = serde_json::from_str(&expected).unwrap();
                let actual: serde_json::Value = serde_json::from_str(&input).unwrap();
                for (key, value) in expected["continuation"].as_object().unwrap() {
                    assert_eq!(
                        &actual["continuation"][key], value,
                        "continuation field {key}"
                    );
                }
                assert_eq!(actual["prompt"], expected["prompt"]);
                assert_eq!(actual["history"], expected["history"]);
            }
            (
                EventKind::OrchestrationCompleted { output: expected },
                TurnResult::Completed(output),
            ) => {
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&output).unwrap(),
                    serde_json::from_str::<serde_json::Value>(&expected).unwrap()
                );
            }
            (expected, actual) => panic!("replay mismatch: {expected:?} / {actual:?}"),
        }
        assert!(
            !replay
                .pending_actions()
                .iter()
                .any(|action| matches!(action, duroxide::Action::CallActivity { .. }))
        );
    }
}
