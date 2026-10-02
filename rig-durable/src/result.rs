//! Versioned, host-inspectable tool result retained in durable history.

use rig::{
    message::ToolResultContent,
    tool::{ToolResult, portable::ToolResultContext},
};
use serde::{Deserialize, Serialize};

use crate::policy::MetadataRetention;

pub const RESULT_FORMAT_VERSION: u32 = 1;

/// Host-visible disposition of one tool call. The model receives canonical
/// content either way; the host can separate these cases.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolDisposition {
    Success,
    Error,
    Refused,
    Skipped,
    /// The attempt did not run the tool, and an earlier attempt's effect may
    /// have happened. This is not a failure.
    Interrupted,
}

/// Why an attempt was interrupted instead of run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterruptionReason {
    /// Another attempt holds the invocation claim and has not settled it.
    ClaimHeld,
    /// The stored claim was made with different arguments, policy, or tool
    /// implementation version.
    ClaimMismatch,
    /// The tool is not registered on the recovering worker.
    ToolUnavailable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Interruption {
    pub reason: InterruptionReason,
    pub message: String,
}

/// Versioned result envelope. Rig's serializable `ToolResult` is retained in
/// full; result metadata is limited to keys the tool policy approved.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DurableToolResult {
    pub format_version: u32,
    pub disposition: ToolDisposition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<ToolResult>,
    #[serde(default, skip_serializing_if = "ToolResultContext::is_empty_context")]
    pub metadata: ToolResultContext,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interruption: Option<Interruption>,
}

trait ContextExt {
    fn is_empty_context(&self) -> bool;
}

impl ContextExt for ToolResultContext {
    fn is_empty_context(&self) -> bool {
        self == &ToolResultContext::default()
    }
}

/// Model feedback for an interrupted call. The model must not assume the
/// effect failed.
pub fn interrupted_feedback(tool_name: &str) -> String {
    format!(
        "Tool `{tool_name}` was interrupted before its result was recorded. \
         Its effect may have happened; it was not run again. Verify the \
         external state before repeating it."
    )
}

impl DurableToolResult {
    /// Build an envelope from a Rig result and the context the tool published,
    /// keeping only approved metadata keys within the size limit. Metadata
    /// over the limit is dropped and reported through `Err`, with the
    /// envelope still returned so callers can decide how to proceed.
    pub fn from_rig(
        result: ToolResult,
        published: &ToolResultContext,
        retention: &MetadataRetention,
    ) -> (Self, Option<MetadataDropped>) {
        let disposition = if result.is_success() {
            ToolDisposition::Success
        } else if result.is_refused() {
            ToolDisposition::Refused
        } else if result.is_skipped() {
            ToolDisposition::Skipped
        } else {
            ToolDisposition::Error
        };
        let (metadata, dropped) = retain_metadata(published, retention);
        (
            Self {
                format_version: RESULT_FORMAT_VERSION,
                disposition,
                result: Some(result),
                metadata,
                interruption: None,
            },
            dropped,
        )
    }

    pub fn interrupted(reason: InterruptionReason, message: impl Into<String>) -> Self {
        Self {
            format_version: RESULT_FORMAT_VERSION,
            disposition: ToolDisposition::Interrupted,
            result: None,
            metadata: ToolResultContext::default(),
            interruption: Some(Interruption {
                reason,
                message: message.into(),
            }),
        }
    }

    /// Canonical content for the model.
    pub fn model_content(&self, tool_name: &str) -> Vec<ToolResultContent> {
        match &self.result {
            Some(result) => result.output().as_content().to_vec(),
            None => vec![ToolResultContent::text(interrupted_feedback(tool_name))],
        }
    }

    /// Whether the model should treat the content as an error. Interruptions
    /// count: the model must not assume success.
    pub fn is_error_for_model(&self) -> bool {
        !matches!(
            self.disposition,
            ToolDisposition::Success | ToolDisposition::Skipped
        )
    }

    pub fn is_supported(&self) -> bool {
        self.format_version == RESULT_FORMAT_VERSION
    }
}

/// Approved metadata exceeded the configured size and was not retained.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataDropped {
    pub bytes: usize,
    pub limit: usize,
}

fn retain_metadata(
    published: &ToolResultContext,
    retention: &MetadataRetention,
) -> (ToolResultContext, Option<MetadataDropped>) {
    if retention.keys().is_empty() {
        return (ToolResultContext::default(), None);
    }
    let Ok(serde_json::Value::Object(all)) = serde_json::to_value(published) else {
        return (ToolResultContext::default(), None);
    };
    let kept: serde_json::Map<String, serde_json::Value> = all
        .into_iter()
        .filter(|(key, _)| retention.retains(key))
        .collect();
    let bytes = serde_json::to_vec(&kept)
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX);
    if bytes > retention.limit() {
        return (
            ToolResultContext::default(),
            Some(MetadataDropped {
                bytes,
                limit: retention.limit(),
            }),
        );
    }
    let kept = serde_json::from_value(serde_json::Value::Object(kept)).unwrap_or_default();
    (kept, None)
}

#[cfg(test)]
mod tests {
    use rig::tool::{ContextValue, ToolContext, ToolExecutionError, ToolOutput};

    use super::*;

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Receipt(String);
    impl ContextValue for Receipt {
        const KEY: &'static str = "receipt";
    }

    #[derive(Serialize, Deserialize)]
    struct Secret(String);
    impl ContextValue for Secret {
        const KEY: &'static str = "secret";
    }

    #[test]
    fn equal_content_keeps_distinct_dispositions_through_serde() {
        let text = "same words";
        let cases = [
            (
                ToolResult::success(ToolOutput::text(text)),
                ToolDisposition::Success,
                false,
            ),
            (
                ToolResult::failed(ToolExecutionError::other(text)),
                ToolDisposition::Error,
                true,
            ),
            (
                ToolResult::failed(ToolExecutionError::refused(text)),
                ToolDisposition::Refused,
                true,
            ),
            (ToolResult::skipped(text), ToolDisposition::Skipped, false),
        ];
        for (result, expected, is_error) in cases {
            let (envelope, dropped) = DurableToolResult::from_rig(
                result,
                &ToolResultContext::default(),
                &MetadataRetention::none(),
            );
            assert!(dropped.is_none());
            let json = serde_json::to_string(&envelope).unwrap();
            let decoded: DurableToolResult = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded.disposition, expected);
            assert_eq!(decoded.is_error_for_model(), is_error);
            assert_eq!(
                decoded.model_content("tool"),
                vec![ToolResultContent::text(text)]
            );
        }
    }

    #[test]
    fn only_approved_metadata_is_retained() {
        let mut context = ToolContext::new();
        context.insert(Secret("inbound-token".into())).unwrap();
        context.insert_result(Receipt("r-1".into())).unwrap();
        context
            .insert_result(Secret("published-token".into()))
            .unwrap();
        let (envelope, dropped) = DurableToolResult::from_rig(
            ToolResult::success(ToolOutput::text("ok")),
            &context.result_context(),
            &MetadataRetention::none().key(Receipt::KEY),
        );
        assert!(dropped.is_none());
        let json = serde_json::to_string(&envelope).unwrap();
        assert!(json.contains("r-1"));
        assert!(!json.contains("token"));
        assert_eq!(
            envelope.metadata.get::<Receipt>().unwrap(),
            Some(Receipt("r-1".into()))
        );
    }

    #[test]
    fn oversized_metadata_is_dropped_and_reported() {
        let mut context = ToolContext::new();
        context.insert_result(Receipt("x".repeat(100))).unwrap();
        let (envelope, dropped) = DurableToolResult::from_rig(
            ToolResult::success(ToolOutput::text("ok")),
            &context.result_context(),
            &MetadataRetention::none().key(Receipt::KEY).max_bytes(32),
        );
        assert!(dropped.is_some());
        assert_eq!(envelope.metadata, ToolResultContext::default());
    }

    #[test]
    fn interrupted_result_gives_the_model_explicit_feedback() {
        let envelope = DurableToolResult::interrupted(InterruptionReason::ClaimHeld, "claim held");
        assert!(envelope.is_error_for_model());
        let content = envelope.model_content("pay");
        let text = match &content[0] {
            ToolResultContent::Text(text) => text.text.clone(),
            other => panic!("unexpected content {other:?}"),
        };
        assert!(text.contains("may have happened"));
        assert!(text.contains("`pay`"));
    }
}
