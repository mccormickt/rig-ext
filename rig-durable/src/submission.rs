//! Backend-neutral submission contract for long-lived sessions.
//!
//! A session deduplicates submissions on `(logical_session_id, request_id)`
//! before it checks whether it is busy. The ledger keeps one receipt per
//! admitted request for the life of the logical session. When the ledger
//! reaches its size limit it rejects admission; it never evicts receipts.

use std::collections::BTreeMap;

use rig::completion::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const LEDGER_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_LEDGER_MAX_BYTES: usize = 48 * 1024;

/// How a session treats a submission while another prompt is active.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionMode {
    /// Queue the message and run it after the active prompt answers.
    #[default]
    FollowUp,
    /// Reject the message when the session is busy.
    RejectIfBusy,
}

/// A client request to run one message in a session.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SubmitInput {
    /// Client-chosen identity. Retries reuse it; distinct intents must not.
    pub request_id: String,
    pub message: Message,
    #[serde(default)]
    pub mode: SubmissionMode,
}

impl SubmitInput {
    pub fn new(request_id: impl Into<String>, message: impl Into<Message>) -> Self {
        Self {
            request_id: request_id.into(),
            message: message.into(),
            mode: SubmissionMode::FollowUp,
        }
    }

    pub fn mode(mut self, mode: SubmissionMode) -> Self {
        self.mode = mode;
        self
    }

    /// Hex SHA-256 over the canonical JSON of the message and the mode.
    pub fn payload_digest(&self) -> Result<String, serde_json::Error> {
        let payload = serde_json::to_vec(&(&self.message, self.mode))?;
        Ok(format!("{:x}", Sha256::digest(payload)))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SubmissionState {
    Queued,
    Running,
    Answered,
    Failed { error: String },
    Cancelled,
}

impl SubmissionState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Answered | Self::Failed { .. } | Self::Cancelled)
    }
}

/// Receipt for one admitted submission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Submission {
    pub logical_session_id: String,
    pub request_id: String,
    /// Identity of the prompt inside the session. Tool logical keys use it.
    pub submission_id: String,
    pub prompt_index: u64,
    pub mode: SubmissionMode,
    pub payload_digest: String,
    #[serde(flatten)]
    pub state: SubmissionState,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum SubmissionError {
    #[error("request `{request_id}` was already admitted with a different message or mode")]
    Conflict { request_id: String },
    #[error("session is processing another prompt")]
    Busy,
    #[error("session is closed")]
    Closed,
    #[error("submission ledger reached its size limit of {max_bytes} bytes")]
    LedgerFull { max_bytes: usize },
    #[error("submission is invalid: {message}")]
    Invalid { message: String },
}

impl SubmissionError {
    /// The serde tag of this variant, for machine-readable error messages.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Conflict { .. } => "conflict",
            Self::Busy => "busy",
            Self::Closed => "closed",
            Self::LedgerFull { .. } => "ledger_full",
            Self::Invalid { .. } => "invalid",
        }
    }
}

/// Outcome of asking the ledger to admit a submission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Admission {
    /// The request was admitted earlier; this is its receipt.
    Existing(Submission),
    /// The request was admitted now and queued.
    Admitted(Submission),
}

impl Admission {
    pub fn submission(&self) -> &Submission {
        match self {
            Self::Existing(submission) | Self::Admitted(submission) => submission,
        }
    }

    pub fn into_submission(self) -> Submission {
        match self {
            Self::Existing(submission) | Self::Admitted(submission) => submission,
        }
    }
}

/// Session availability at admission time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionAvailability {
    pub busy: bool,
    pub closed: bool,
}

/// Receipts for one logical session, ordered by request ID.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionLedger {
    pub format_version: u32,
    pub logical_session_id: String,
    pub max_bytes: usize,
    pub next_prompt_index: u64,
    /// Receipts by request ID.
    pub receipts: BTreeMap<String, Submission>,
}

impl SubmissionLedger {
    pub fn new(logical_session_id: impl Into<String>, max_bytes: usize) -> Self {
        Self {
            format_version: LEDGER_FORMAT_VERSION,
            logical_session_id: logical_session_id.into(),
            max_bytes: max_bytes.max(1),
            next_prompt_index: 0,
            receipts: BTreeMap::new(),
        }
    }

    /// Continue numbering prompts after those an earlier mechanism admitted.
    pub fn with_next_prompt_index(mut self, next_prompt_index: u64) -> Self {
        self.next_prompt_index = next_prompt_index;
        self
    }

    pub fn is_supported(&self) -> bool {
        self.format_version == LEDGER_FORMAT_VERSION
    }

    pub fn receipt(&self, request_id: &str) -> Option<&Submission> {
        self.receipts.get(request_id)
    }

    pub fn by_submission_id(&self, submission_id: &str) -> Option<&Submission> {
        self.receipts
            .values()
            .find(|receipt| receipt.submission_id == submission_id)
    }

    /// Allocate a prompt index outside the submission contract (for example a
    /// steering message) so logical keys stay distinct.
    pub fn allocate_prompt_index(&mut self) -> Result<u64, SubmissionError> {
        let index = self.next_prompt_index;
        self.next_prompt_index = index
            .checked_add(1)
            .ok_or_else(|| SubmissionError::Invalid {
                message: "prompt index overflow".into(),
            })?;
        Ok(index)
    }

    /// Decide admission. Deduplication runs before the busy and closed checks,
    /// so a retried request receives its original receipt in every state.
    pub fn admit(
        &mut self,
        input: &SubmitInput,
        availability: SessionAvailability,
    ) -> Result<Admission, SubmissionError> {
        if input.request_id.is_empty() {
            return Err(SubmissionError::Invalid {
                message: "request_id must not be empty".into(),
            });
        }
        let digest = input
            .payload_digest()
            .map_err(|error| SubmissionError::Invalid {
                message: error.to_string(),
            })?;
        if let Some(existing) = self.receipts.get(&input.request_id) {
            if existing.payload_digest != digest || existing.mode != input.mode {
                return Err(SubmissionError::Conflict {
                    request_id: input.request_id.clone(),
                });
            }
            return Ok(Admission::Existing(existing.clone()));
        }
        if availability.closed {
            return Err(SubmissionError::Closed);
        }
        if availability.busy && input.mode == SubmissionMode::RejectIfBusy {
            return Err(SubmissionError::Busy);
        }
        let prompt_index = self.next_prompt_index;
        let candidate = Submission {
            logical_session_id: self.logical_session_id.clone(),
            request_id: input.request_id.clone(),
            submission_id: crate::identity::LogicalCallKey::submission_for_prompt(prompt_index),
            prompt_index,
            mode: input.mode,
            payload_digest: digest,
            state: SubmissionState::Queued,
        };
        let projected = self.serialized_len_with(&candidate)?;
        if projected > self.max_bytes {
            return Err(SubmissionError::LedgerFull {
                max_bytes: self.max_bytes,
            });
        }
        self.next_prompt_index =
            prompt_index
                .checked_add(1)
                .ok_or_else(|| SubmissionError::Invalid {
                    message: "prompt index overflow".into(),
                })?;
        self.receipts
            .insert(candidate.request_id.clone(), candidate.clone());
        Ok(Admission::Admitted(candidate))
    }

    fn serialized_len_with(&self, candidate: &Submission) -> Result<usize, SubmissionError> {
        let mut projected = self.clone();
        // Measure the largest terminal form so later transitions cannot
        // overflow the limit.
        let mut widest = candidate.clone();
        widest.state = SubmissionState::Failed {
            error: String::new(),
        };
        projected.receipts.insert(widest.request_id.clone(), widest);
        serde_json::to_vec(&projected)
            .map(|bytes| bytes.len())
            .map_err(|error| SubmissionError::Invalid {
                message: error.to_string(),
            })
    }

    pub fn set_state(&mut self, request_id: &str, state: SubmissionState) -> Option<&Submission> {
        let receipt = self.receipts.get_mut(request_id)?;
        receipt.state = state;
        Some(receipt)
    }

    /// Mark every non-terminal receipt cancelled; used when a session closes
    /// with queued work.
    pub fn cancel_pending(&mut self) {
        for receipt in self.receipts.values_mut() {
            if !receipt.state.is_terminal() {
                receipt.state = SubmissionState::Cancelled;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ledger() -> SubmissionLedger {
        SubmissionLedger::new("session-1", DEFAULT_LEDGER_MAX_BYTES)
    }

    const IDLE: SessionAvailability = SessionAvailability {
        busy: false,
        closed: false,
    };
    const BUSY: SessionAvailability = SessionAvailability {
        busy: true,
        closed: false,
    };

    #[test]
    fn duplicate_returns_original_receipt_and_altered_duplicate_is_rejected() {
        let mut ledger = ledger();
        let input = SubmitInput::new("req-1", "hello");
        let first = ledger.admit(&input, IDLE).unwrap();
        let Admission::Admitted(receipt) = first else {
            panic!("first admission must admit");
        };
        assert_eq!(receipt.prompt_index, 0);
        assert_eq!(receipt.submission_id, "prompt-0");
        ledger.set_state("req-1", SubmissionState::Running);

        let again = ledger.admit(&input, BUSY).unwrap();
        assert_eq!(
            again,
            Admission::Existing(ledger.receipt("req-1").unwrap().clone())
        );
        assert_eq!(again.submission().state, SubmissionState::Running);

        let altered = SubmitInput::new("req-1", "hello?");
        assert_eq!(
            ledger.admit(&altered, IDLE).unwrap_err(),
            SubmissionError::Conflict {
                request_id: "req-1".into()
            }
        );
        let altered_mode = input.clone().mode(SubmissionMode::RejectIfBusy);
        assert_eq!(
            ledger.admit(&altered_mode, IDLE).unwrap_err(),
            SubmissionError::Conflict {
                request_id: "req-1".into()
            }
        );
        assert_eq!(ledger.receipts.len(), 1);
        assert_eq!(ledger.next_prompt_index, 1);
    }

    #[test]
    fn dedup_precedes_busy_and_closed_checks() {
        let mut ledger = ledger();
        let input = SubmitInput::new("req-busy", "go").mode(SubmissionMode::RejectIfBusy);
        ledger.admit(&input, IDLE).unwrap();
        assert!(matches!(
            ledger.admit(&input, BUSY).unwrap(),
            Admission::Existing(_)
        ));
        let closed = SessionAvailability {
            busy: false,
            closed: true,
        };
        assert!(matches!(
            ledger.admit(&input, closed).unwrap(),
            Admission::Existing(_)
        ));
        assert_eq!(
            ledger
                .admit(&SubmitInput::new("req-new", "go"), closed)
                .unwrap_err(),
            SubmissionError::Closed
        );
    }

    #[test]
    fn reject_if_busy_is_rejected_while_follow_up_queues() {
        let mut ledger = ledger();
        let rejected = SubmitInput::new("req-r", "now").mode(SubmissionMode::RejectIfBusy);
        assert_eq!(
            ledger.admit(&rejected, BUSY).unwrap_err(),
            SubmissionError::Busy
        );
        assert!(
            ledger.receipt("req-r").is_none(),
            "rejections take no receipt"
        );
        let queued = SubmitInput::new("req-f", "later");
        let receipt = ledger.admit(&queued, BUSY).unwrap().into_submission();
        assert_eq!(receipt.state, SubmissionState::Queued);
        // The rejected request can be retried once the session is idle.
        assert!(matches!(
            ledger.admit(&rejected, IDLE).unwrap(),
            Admission::Admitted(_)
        ));
    }

    #[test]
    fn full_ledger_rejects_admission_without_evicting() {
        let empty = SubmissionLedger::new("session-1", 1);
        let base = serde_json::to_vec(&empty).unwrap().len();
        // Room for one receipt, measured in its widest terminal form.
        let mut ledger = SubmissionLedger::new("session-1", base + 260);
        let first = ledger.admit(&SubmitInput::new("req-1", "a"), IDLE).unwrap();
        assert!(matches!(first, Admission::Admitted(_)));
        let second = ledger.admit(&SubmitInput::new("req-2", "b"), IDLE);
        assert_eq!(
            second.unwrap_err(),
            SubmissionError::LedgerFull {
                max_bytes: base + 260
            }
        );
        assert!(ledger.receipt("req-1").is_some());
        assert!(matches!(
            ledger.admit(&SubmitInput::new("req-1", "a"), IDLE).unwrap(),
            Admission::Existing(_)
        ));
        assert!(serde_json::to_vec(&ledger).unwrap().len() <= base + 260);
    }

    #[test]
    fn ledger_round_trips_through_serde() {
        let mut ledger = ledger();
        ledger.admit(&SubmitInput::new("req-1", "a"), IDLE).unwrap();
        ledger.set_state(
            "req-1",
            SubmissionState::Failed {
                error: "boom".into(),
            },
        );
        let json = serde_json::to_string(&ledger).unwrap();
        let decoded: SubmissionLedger = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, ledger);
        assert!(json.contains(r#""state":"failed""#));
    }
}
