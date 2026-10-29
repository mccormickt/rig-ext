//! Checkpoint envelopes reject unsupported formats before decoding run state.

#![cfg(feature = "duroxide")]
#![allow(clippy::unwrap_used)]

#[test]
fn unsupported_continuation_is_rejected_before_rig_deserialization() {
    let mut input = serde_json::to_value(rig_durable::AgentInput::new("start")).unwrap();
    for envelope in [
        serde_json::json!({"kind": "unsupported", "agent_run": "invalid Rig state"}),
        serde_json::json!({"kind": "v3", "format_version": 999, "agent_run": "invalid Rig state"}),
    ] {
        input["continuation"] = envelope;
        let error = serde_json::from_value::<rig_durable::AgentInput>(input.clone())
            .unwrap_err()
            .to_string();
        assert_eq!(
            error,
            "unsupported agent continuation persistence generation"
        );
    }
}

#[test]
fn unsupported_session_is_rejected_before_state_deserialization() {
    let error = serde_json::from_value::<rig_durable::session::SessionInput>(serde_json::json!({
        "format_version": 999, "context": "invalid nested state"
    }))
    .unwrap_err()
    .to_string();
    assert_eq!(error, "unsupported session persistence generation");
}

#[test]
fn current_session_format_rejects_corrupt_state() {
    let mut input =
        serde_json::to_value(rig_durable::session::SessionInput::new("session", 4096)).unwrap();
    input["context"] = serde_json::json!("invalid nested state");
    assert!(serde_json::from_value::<rig_durable::session::SessionInput>(input).is_err());
}
