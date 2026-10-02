//! What one script execution produced: status, bounded output, return value,
//! ordered child-call records, and a safe script diagnostic.

use std::fmt::Write as _;
use std::time::Duration;

use rig_core::tool::{ToolErrorKind, ToolExecutionError, ToolOutput};
use serde::{Deserialize, Serialize};

/// How a script execution ended.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    /// The async function body returned.
    Completed,
    /// The script threw, rejected, or failed to compile.
    ScriptError,
    /// The wall-time limit stopped the script.
    TimedOut,
    /// The caller dropped the execution before it finished.
    Cancelled,
    /// The script awaited a promise no host call or job could settle.
    Stalled,
}

/// What the tool did for one script-initiated call.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallStatus {
    /// The tool succeeded.
    Succeeded,
    /// The tool failed.
    Failed,
    /// The tool or host policy refused the call.
    Refused,
    /// Runtime policy skipped the call before the tool body ran.
    Skipped,
    /// The host rejected the call before dispatch: unknown name, call budget,
    /// or oversized arguments. No tool effect happened.
    Rejected,
    /// The script ended before the queued call started. No tool effect happened.
    NotStarted,
    /// The call was in flight when the script ended; cancellation was
    /// requested. Whether the external effect stopped is unknown.
    CancellationRequested,
}

/// What happened to the projected reply, not proof that the script observed it.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptDelivery {
    /// The reply was sent to the worker, or the worker created a rejection.
    /// A sent reply can remain unread when the script ends; this does not
    /// prove that a promise settled or that script code observed the result.
    Delivered,
    /// The projected result exceeded a message limit; a size error was sent
    /// instead. The worker may not have consumed it.
    Oversized,
    /// The result arrived after the script ended and was discarded.
    Discarded,
    /// The call never produced a result.
    None,
}

/// One child call, in the order the script started it.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallRecord {
    /// Zero-based position among the script's calls.
    pub ordinal: u32,
    /// Exact requested tool name.
    pub name: String,
    /// Tool-side outcome.
    pub status: CallStatus,
    /// Normalized error kind for failures, refusals, and rejections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<ToolErrorKind>,
    /// Whether the script received the result.
    pub delivery: ScriptDelivery,
}

/// A script failure safe to show the model: the script's own exception text
/// and position. Host and dispatcher internals never appear here.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScriptDiagnostic {
    /// Exception message.
    pub message: String,
    /// One-based line in the submitted source, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    /// One-based column, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<u32>,
    /// Guest stack trace, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stack: Option<String>,
}

/// Text the script emitted with `text()`, bounded by the output limit.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScriptOutput {
    /// Valid UTF-8, at most the output limit in bytes.
    pub text: String,
    /// Whether writes were cut at the limit.
    pub truncated: bool,
}

/// The result of one script execution.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionReport {
    /// How the execution ended.
    pub status: ExecutionStatus,
    /// Bounded emitted text, retained on failure as well.
    pub output: ScriptOutput,
    /// The async body's return value, when it completed with a JSON value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub returned: Option<serde_json::Value>,
    /// Child calls in start order. At most `max_calls` entries.
    pub calls: Vec<CallRecord>,
    /// Calls the guest rejected after the call budget was exhausted. These
    /// have no records and caused no tool effect.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub over_budget_calls: u32,
    /// Script failure details.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<ScriptDiagnostic>,
    /// Wall time from request to report.
    pub elapsed: Duration,
}

impl ExecutionReport {
    pub(crate) fn new(status: ExecutionStatus) -> Self {
        Self {
            status,
            output: ScriptOutput::default(),
            returned: None,
            calls: Vec::new(),
            over_budget_calls: 0,
            diagnostic: None,
            elapsed: Duration::ZERO,
        }
    }

    /// Whether the script completed.
    pub fn is_completed(&self) -> bool {
        self.status == ExecutionStatus::Completed
    }

    /// A compact one-line summary of child calls by status.
    pub fn call_summary(&self) -> String {
        let count = |status: CallStatus| self.calls.iter().filter(|c| c.status == status).count();
        let mut parts = vec![format!("{} total", self.calls.len())];
        for (status, label) in [
            (CallStatus::Succeeded, "succeeded"),
            (CallStatus::Failed, "failed"),
            (CallStatus::Refused, "refused"),
            (CallStatus::Skipped, "skipped"),
            (CallStatus::Rejected, "rejected"),
            (CallStatus::NotStarted, "not started"),
            (CallStatus::CancellationRequested, "cancellation requested"),
        ] {
            let n = count(status);
            if n > 0 {
                parts.push(format!("{n} {label}"));
            }
        }
        if self.over_budget_calls > 0 {
            parts.push(format!("{} over budget", self.over_budget_calls));
        }
        parts.join(", ")
    }

    /// Render the model-visible text: emitted output, a truncation marker when
    /// cut, the return value, the diagnostic, and the call summary.
    pub fn render_text(&self) -> String {
        let mut text = String::new();
        text.push_str(&self.output.text);
        if self.output.truncated {
            if !text.is_empty() && !text.ends_with('\n') {
                text.push('\n');
            }
            let _ = writeln!(text, "[output truncated at the host limit]");
        }
        match self.status {
            ExecutionStatus::Completed => {
                if let Some(returned) = &self.returned {
                    if !text.is_empty() && !text.ends_with('\n') {
                        text.push('\n');
                    }
                    let rendered = serde_json::to_string_pretty(returned).unwrap_or_default();
                    let _ = writeln!(text, "Return value:\n{rendered}");
                }
            }
            status => {
                if !text.is_empty() && !text.ends_with('\n') {
                    text.push('\n');
                }
                let _ = write!(text, "Script {}", status_label(status));
                if let Some(diagnostic) = &self.diagnostic {
                    if let (Some(line), Some(column)) = (diagnostic.line, diagnostic.column) {
                        let _ = write!(text, " at line {line}, column {column}");
                    } else if let Some(line) = diagnostic.line {
                        let _ = write!(text, " at line {line}");
                    }
                    let _ = write!(text, ": {}", diagnostic.message);
                }
                text.push('\n');
            }
        }
        if !self.calls.is_empty() || self.over_budget_calls > 0 {
            let _ = writeln!(text, "Tool calls: {}", self.call_summary());
        }
        text
    }

    /// Convert to a Rig tool result: a completed script is successful text
    /// output; any other status is a typed failure whose model output still
    /// carries the retained partial text and call summary.
    pub fn into_tool_result(self) -> Result<ToolOutput, ToolExecutionError> {
        let output = ToolOutput::text(self.render_text());
        match self.status {
            ExecutionStatus::Completed => Ok(output),
            ExecutionStatus::TimedOut => Err(ToolExecutionError::timeout("script timed out")
                .with_retryable(false)
                .with_model_output(output)),
            ExecutionStatus::Cancelled => {
                Err(ToolExecutionError::cancelled("script cancelled").with_model_output(output))
            }
            ExecutionStatus::ScriptError | ExecutionStatus::Stalled => {
                let message = self
                    .diagnostic
                    .as_ref()
                    .map(|d| d.message.clone())
                    .unwrap_or_else(|| status_label(self.status).to_string());
                Err(ToolExecutionError::other(format!(
                    "script {}: {message}",
                    status_label(self.status)
                ))
                .with_retryable(false)
                .with_model_output(output))
            }
        }
    }
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

fn status_label(status: ExecutionStatus) -> &'static str {
    match status {
        ExecutionStatus::Completed => "completed",
        ExecutionStatus::ScriptError => "failed",
        ExecutionStatus::TimedOut => "timed out",
        ExecutionStatus::Cancelled => "was cancelled",
        ExecutionStatus::Stalled => "stalled waiting on a promise that nothing could settle",
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn renders_partial_output_diagnostic_and_summary() {
        let mut report = ExecutionReport::new(ExecutionStatus::ScriptError);
        report.output.text = "partial".into();
        report.diagnostic = Some(ScriptDiagnostic {
            message: "boom".into(),
            line: Some(2),
            column: Some(7),
            stack: None,
        });
        report.calls.push(CallRecord {
            ordinal: 0,
            name: "a".into(),
            status: CallStatus::Succeeded,
            error_kind: None,
            delivery: ScriptDelivery::Delivered,
        });
        let text = report.render_text();
        assert_eq!(
            text,
            "partial\nScript failed at line 2, column 7: boom\nTool calls: 1 total, 1 succeeded\n"
        );
        let error = report.into_tool_result().unwrap_err();
        assert_eq!(error.kind(), ToolErrorKind::Other);
        assert_eq!(error.model_feedback(), Some(text.as_str()));
    }

    #[test]
    fn completed_report_renders_return_value() {
        let mut report = ExecutionReport::new(ExecutionStatus::Completed);
        report.returned = Some(serde_json::json!({"x": 1}));
        report.output.truncated = true;
        report.output.text = "abc".into();
        assert_eq!(
            report.render_text(),
            "abc\n[output truncated at the host limit]\nReturn value:\n{\n  \"x\": 1\n}\n"
        );
    }
}
