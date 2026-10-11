//! Old histories must never resolve to incompatible generation 2 code.

#[cfg(feature = "temporal")]
#[tokio::test]
async fn temporal_excludes_every_legacy_workflow_identity() {
    use rig_durable::temporal::{TemporalAgentSessionWorkflow, TemporalAgentWorkflow};
    use temporalio_client::WorkflowHistory;
    use temporalio_sdk::workflow_replayer::{
        WorkflowReplayError, WorkflowReplayFailure, WorkflowReplayer, WorkflowReplayerOptions,
    };
    let mut options = WorkflowReplayerOptions::new().build();
    options
        .register_workflow::<TemporalAgentWorkflow>()
        .unwrap();
    options
        .register_workflow::<TemporalAgentSessionWorkflow>()
        .unwrap();
    let replayer = WorkflowReplayer::new(options).unwrap();
    for (bytes, old_type) in [
        (
            include_bytes!("fixtures/legacy/temporal-retry.json").as_slice(),
            "RigTemporalAgentV1",
        ),
        (
            include_bytes!("fixtures/legacy/temporal-approval.json").as_slice(),
            "RigTemporalAgentV1",
        ),
        (
            include_bytes!("fixtures/legacy/temporal-steering.json").as_slice(),
            "RigTemporalAgentSessionV1",
        ),
    ] {
        let error = replayer
            .replay_workflow(WorkflowHistory::from_json(bytes).unwrap())
            .await
            .unwrap_err();
        let WorkflowReplayError::Replay(WorkflowReplayFailure::WorkflowTaskFailure { message }) =
            error
        else {
            panic!("expected missing workflow type, got {error:?}");
        };
        assert!(
            message.contains(&format!("Workflow type {old_type} not found")),
            "{message}"
        );
    }
}

#[cfg(feature = "duroxide")]
#[test]
fn duroxide_excludes_every_legacy_orchestration_identity() {
    use duroxide::{Event, EventKind};
    let registry = rig_durable::orchestration_registry(Default::default());
    for bytes in [
        include_bytes!("fixtures/legacy/duroxide-continuation-1.json").as_slice(),
        include_bytes!("fixtures/legacy/duroxide-continuation-2.json").as_slice(),
        include_bytes!("fixtures/legacy/duroxide-continuation-3.json").as_slice(),
    ] {
        let history: Vec<Event> = serde_json::from_slice(bytes).unwrap();
        let EventKind::OrchestrationStarted {
            name,
            version,
            input,
            ..
        } = &history[0].kind
        else {
            panic!("missing orchestration start");
        };
        assert!(
            registry
                .resolve_handler_exact(name, &semver::Version::parse(version).unwrap())
                .is_none()
        );
        let value: serde_json::Value = serde_json::from_str(input).unwrap();
        if value.get("continuation").is_some() {
            assert!(
                serde_json::from_str::<rig_durable::AgentInput>(input)
                    .unwrap_err()
                    .to_string()
                    .contains("unsupported agent continuation persistence generation")
            );
        }
    }
}

#[cfg(feature = "duroxide")]
#[test]
fn old_continuation_is_rejected_before_rig_deserialization() {
    let mut input = serde_json::to_value(rig_durable::AgentInput::new("start")).unwrap();
    input["continuation"] =
        serde_json::json!({"kind": "v1", "agent_run": "invalid nested Rig state"});
    let error = serde_json::from_value::<rig_durable::AgentInput>(input)
        .unwrap_err()
        .to_string();
    assert_eq!(
        error,
        "unsupported agent continuation persistence generation"
    );
}

#[cfg(feature = "duroxide")]
#[test]
fn old_session_is_rejected_before_rig_deserialization() {
    let error = serde_json::from_value::<rig_durable::session::SessionInput>(serde_json::json!({
        "format_version": 1, "context": "invalid nested Rig state"
    }))
    .unwrap_err()
    .to_string();
    assert_eq!(error, "unsupported session persistence generation");
}
