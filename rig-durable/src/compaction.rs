//! Bound the active model context separately from the audit transcript.
//!
//! Compaction composes two Rig abstractions. A [`MemoryPolicy`] from
//! `rig-memory` decides which transcript prefix leaves the active window. A
//! [`Compactor`] from `rig::memory` folds that prefix, together with the prior
//! artifact, into a new artifact. Both run inside the compaction activity, so
//! workflow code never executes policy or model logic. The workflow owns only
//! the durable watermark: a [`ContextState`] keeps the full transcript and one
//! [`CompactionRecord`] that stands in for a prefix of it. This is the durable
//! form of `rig_memory::CompactingMemory`, whose watermark lives in process
//! memory.
//!
//! Compaction runs after a completed prompt, never inside one. The transcript
//! itself is never shortened by compaction.

use std::{fmt, sync::Arc};

use rig::{
    DynModel,
    completion::{CompletionRequest, Message, Usage},
    id::ConversationId,
    memory::{Compactor, MemoryError},
    operation::Completion,
    transcript::{TranscriptError, validate_canonical},
    wasm_compat::{WasmBoxedFuture, WasmCompatSend, WasmCompatSync},
};
use rig_memory::MemoryPolicy;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

pub const COMPACTION_FORMAT_VERSION: u32 = 1;

const DEFAULT_INSTRUCTIONS: &str = "You compact conversation history for an assistant that will \
continue the conversation. Write a concise summary that preserves user goals, decisions, facts, \
tool results, and open questions. Do not add commentary.";

const SUMMARY_PROMPT: &str = "Summarize the conversation above for an assistant that will \
continue it. Reply with the summary only.";

/// Serializable compaction settings recorded with a configuration. The
/// policy and compactor are worker code; only their version is recorded.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionConfig {
    /// Version of the policy and compactor pair. It is recorded with every
    /// artifact.
    pub version: String,
}

/// Worker-side compaction: a [`MemoryPolicy`] chooses what leaves the active
/// window and a [`Compactor`] folds it into an artifact.
///
/// The compactor's artifact must serialize, because the workflow retains it
/// and passes it back as `carry_over` on the next round.
///
/// Use Rig's [`rig_memory::SlidingWindowMemory`] to select the retained window
/// and [`ModelCompactor`] to summarize the removed prefix. Pass this value to
/// either backend's `.compaction(...)` builder method. It applies to sessions,
/// not independent single runs.
///
/// ```no_run
/// use rig::{DynModel, operation::Completion};
/// use rig_durable::{Compaction, ModelCompactor};
/// use rig_memory::SlidingWindowMemory;
///
/// fn summaries(model: DynModel<Completion>) -> Compaction {
///     Compaction::new(
///         SlidingWindowMemory::last_messages(40),
///         ModelCompactor::new(model),
///     ).version("summary-v1")
/// }
/// ```
///
/// Change the version when the policy or artifact format changes. The worker
/// rejects mismatched versions before running the policy or model. A failed
/// summary leaves the context intact and does not fail the session; the next
/// completed prompt can trigger another attempt. Compaction does not remove
/// messages from the audit transcript or reduce its serialized byte count.
#[derive(Clone)]
pub struct Compaction {
    policy: Arc<dyn MemoryPolicy>,
    compactor: Arc<dyn ErasedCompactor>,
    version: String,
}

impl Compaction {
    pub fn new<P, C>(policy: P, compactor: C) -> Self
    where
        P: MemoryPolicy + 'static,
        C: Compactor + 'static,
        C::Artifact: Serialize + DeserializeOwned,
    {
        Self {
            policy: Arc::new(policy),
            compactor: Arc::new(compactor),
            version: "1".into(),
        }
    }

    /// Record a version for this policy and compactor pair. Change it when
    /// either changes in a way that alters the artifacts it produces.
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    pub fn config(&self) -> CompactionConfig {
        CompactionConfig {
            version: self.version.clone(),
        }
    }

    /// Apply the policy to the transcript and compact what it demoted after
    /// the absorbed watermark. The output is a no-op when nothing new was
    /// demoted.
    pub async fn run(&self, request: CompactionRequest) -> Result<CompactionOutput, String> {
        if request.expected_version != self.version
            || (request.carry_over.is_some()
                && request.carry_over_version.as_deref() != Some(self.version.as_str()))
        {
            return Err("compaction policy or carry-over version mismatch".into());
        }
        let absorbed = request.absorbed;
        let (_kept, demoted) = self
            .policy
            .apply_with_demoted(request.transcript)
            .map_err(|error| error.to_string())?;
        let cutoff = demoted.len();
        if cutoff <= absorbed {
            return Ok(CompactionOutput {
                cutoff: absorbed,
                policy_version: self.version.clone(),
                input_messages: 0,
                artifact: None,
            });
        }
        let evicted = demoted
            .get(absorbed..)
            .ok_or("compaction watermark exceeds demoted slice length")?;
        let conversation_id = ConversationId::new(request.conversation_id);
        let artifact = self
            .compactor
            .compact(&conversation_id, evicted, request.carry_over.as_ref())
            .await?;
        Ok(CompactionOutput {
            cutoff,
            policy_version: self.version.clone(),
            input_messages: evicted.len(),
            artifact: Some(artifact),
        })
    }
}

impl fmt::Debug for Compaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Compaction")
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

/// Object-safe view of a [`Compactor`] whose artifact round-trips through
/// JSON.
trait ErasedCompactor: WasmCompatSend + WasmCompatSync {
    fn compact<'a>(
        &'a self,
        conversation_id: &'a ConversationId,
        evicted: &'a [Message],
        carry_over: Option<&'a Value>,
    ) -> WasmBoxedFuture<'a, Result<CompactionArtifact, String>>;
}

impl<C> ErasedCompactor for C
where
    C: Compactor,
    C::Artifact: Serialize + DeserializeOwned,
{
    fn compact<'a>(
        &'a self,
        conversation_id: &'a ConversationId,
        evicted: &'a [Message],
        carry_over: Option<&'a Value>,
    ) -> WasmBoxedFuture<'a, Result<CompactionArtifact, String>> {
        Box::pin(async move {
            let carry_over = carry_over
                .map(|value| serde_json::from_value::<C::Artifact>(value.clone()))
                .transpose()
                .map_err(|error| format!("prior compaction artifact does not decode: {error}"))?;
            let artifact = Compactor::compact(self, conversation_id, evicted, carry_over.as_ref())
                .await
                .map_err(|error| error.to_string())?;
            let value = serde_json::to_value(&artifact)
                .map_err(|error| format!("compaction artifact does not encode: {error}"))?;
            Ok(CompactionArtifact {
                value,
                message: artifact.into(),
            })
        })
    }
}

/// A compactor artifact in the two forms the workflow needs: the encoded
/// value it carries over, and the message it splices into the context.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompactionArtifact {
    pub value: Value,
    pub message: Message,
}

/// One applied artifact. Transcript messages before `cutoff` are represented
/// by `artifact.message` in the active context.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompactionRecord {
    pub format_version: u32,
    /// Number of transcript messages the artifact stands in for.
    pub cutoff: usize,
    pub policy_version: String,
    pub artifact: CompactionArtifact,
    /// Transcript messages compacted in this round, after the prior cutoff.
    pub input_messages: usize,
}

/// Input of the compaction activity.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompactionRequest {
    /// Session identity handed to the compactor.
    pub conversation_id: String,
    pub expected_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub carry_over_version: Option<String>,
    /// The full transcript. The policy decides the window over it.
    pub transcript: Vec<Message>,
    /// Transcript messages the current artifact already stands in for.
    pub absorbed: usize,
    /// Encoded artifact of the applied record, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub carry_over: Option<Value>,
}

/// Output of the compaction activity. `artifact` is `None` when the policy
/// demoted nothing beyond the absorbed watermark.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompactionOutput {
    pub cutoff: usize,
    pub policy_version: String,
    pub input_messages: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<CompactionArtifact>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CompactionError {
    #[error("compaction cutoff {cutoff} exceeds the transcript length {len}")]
    OutOfRange { cutoff: usize, len: usize },
    #[error("compaction cutoff {cutoff} leaves a non-canonical context: {error}")]
    NotCanonical { cutoff: usize, error: String },
    #[error("unsupported compaction record format version {0}")]
    UnsupportedFormat(u32),
    #[error("compaction policy or carry-over version mismatch")]
    VersionMismatch,
}

/// Audit transcript plus the artifact that currently stands in for its prefix.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ContextState {
    /// Every message, in order. Compaction never removes from it.
    pub transcript: Vec<Message>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction: Option<CompactionRecord>,
}

impl ContextState {
    pub fn new(transcript: Vec<Message>) -> Self {
        Self {
            transcript,
            compaction: None,
        }
    }

    pub fn applied_cutoff(&self) -> usize {
        self.compaction
            .as_ref()
            .map(|record| record.cutoff)
            .unwrap_or(0)
    }

    pub fn append(&mut self, messages: impl IntoIterator<Item = Message>) {
        self.transcript.extend(messages);
    }

    /// Messages the next prompt is built on: the artifact message, if any,
    /// followed by the transcript after the cutoff.
    pub fn active_context(&self) -> Vec<Message> {
        match &self.compaction {
            Some(record) => splice(&record.artifact.message, &self.transcript, record.cutoff),
            None => self.transcript.clone(),
        }
    }

    /// Validate that the active context is a canonical transcript.
    pub fn validate_active_context(&self) -> Result<(), TranscriptError> {
        validate_canonical(&self.active_context())
    }

    /// The activity input for the next compaction round.
    pub fn request(
        &self,
        conversation_id: impl Into<String>,
        expected_version: &str,
    ) -> CompactionRequest {
        CompactionRequest {
            conversation_id: conversation_id.into(),
            expected_version: expected_version.into(),
            carry_over_version: self
                .compaction
                .as_ref()
                .map(|record| record.policy_version.clone()),
            transcript: self.transcript.clone(),
            absorbed: self.applied_cutoff(),
            carry_over: self
                .compaction
                .as_ref()
                .map(|record| record.artifact.value.clone()),
        }
    }

    /// Apply a finished round. Returns `false`, and leaves the context
    /// unchanged, when the output carries no artifact or does not advance
    /// past the applied cutoff. Rejects a cutoff beyond the transcript or
    /// one that leaves a non-canonical active context.
    pub fn apply(
        &mut self,
        output: CompactionOutput,
        expected_version: &str,
    ) -> Result<bool, CompactionError> {
        if output.policy_version != expected_version
            || self
                .compaction
                .as_ref()
                .is_some_and(|record| record.policy_version != expected_version)
        {
            return Err(CompactionError::VersionMismatch);
        }
        let Some(artifact) = output.artifact else {
            return Ok(false);
        };
        if output.cutoff <= self.applied_cutoff() {
            return Ok(false);
        }
        if output.cutoff > self.transcript.len() {
            return Err(CompactionError::OutOfRange {
                cutoff: output.cutoff,
                len: self.transcript.len(),
            });
        }
        validate_canonical(&splice(&artifact.message, &self.transcript, output.cutoff)).map_err(
            |error| CompactionError::NotCanonical {
                cutoff: output.cutoff,
                error: error.to_string(),
            },
        )?;
        self.compaction = Some(CompactionRecord {
            format_version: COMPACTION_FORMAT_VERSION,
            cutoff: output.cutoff,
            policy_version: output.policy_version,
            artifact,
            input_messages: output.input_messages,
        });
        Ok(true)
    }

    /// Restore a record retained by an earlier execution.
    pub fn with_compaction(
        mut self,
        record: Option<CompactionRecord>,
    ) -> Result<Self, CompactionError> {
        if let Some(record) = &record {
            if record.format_version != COMPACTION_FORMAT_VERSION {
                return Err(CompactionError::UnsupportedFormat(record.format_version));
            }
            if record.cutoff > self.transcript.len() {
                return Err(CompactionError::OutOfRange {
                    cutoff: record.cutoff,
                    len: self.transcript.len(),
                });
            }
        }
        self.compaction = record;
        self.validate_active_context()
            .map_err(|error| CompactionError::NotCanonical {
                cutoff: self.applied_cutoff(),
                error: error.to_string(),
            })?;
        Ok(self)
    }
}

fn splice(summary: &Message, transcript: &[Message], cutoff: usize) -> Vec<Message> {
    let tail = &transcript[cutoff..];
    let mut context = Vec::with_capacity(tail.len() + 1);
    context.push(summary.clone());
    context.extend(tail.iter().cloned());
    context
}

/// A [`Compactor`] that summarizes the evicted window with one model call.
/// Its artifact serializes, so it works across the durable boundary.
pub struct ModelCompactor {
    model: DynModel<Completion>,
    instructions: String,
}

impl ModelCompactor {
    pub fn new(model: impl Into<DynModel<Completion>>) -> Self {
        Self {
            model: model.into(),
            instructions: DEFAULT_INSTRUCTIONS.into(),
        }
    }

    /// System instructions for the summarization model call.
    pub fn instructions(mut self, instructions: impl Into<String>) -> Self {
        self.instructions = instructions.into();
        self
    }
}

impl fmt::Debug for ModelCompactor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelCompactor")
            .field("instructions", &self.instructions)
            .finish_non_exhaustive()
    }
}

/// Artifact of [`ModelCompactor`]: the summary text and the usage of the
/// call that produced it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelSummary {
    pub text: String,
    pub usage: Usage,
}

impl From<ModelSummary> for Message {
    fn from(summary: ModelSummary) -> Self {
        Message::user(format!(
            "Summary of the earlier conversation:\n{}",
            summary.text
        ))
    }
}

impl Compactor for ModelCompactor {
    type Artifact = ModelSummary;

    fn compact<'a>(
        &'a self,
        _conversation_id: &'a ConversationId,
        evicted: &'a [Message],
        carry_over: Option<&'a Self::Artifact>,
    ) -> WasmBoxedFuture<'a, Result<Self::Artifact, MemoryError>> {
        Box::pin(async move {
            let mut chat_history = Vec::with_capacity(evicted.len() + 3);
            chat_history.push(Message::system(self.instructions.clone()));
            if let Some(prior) = carry_over {
                chat_history.push(Message::user(format!(
                    "Summary of the conversation before this window:\n{}",
                    prior.text
                )));
            }
            chat_history.extend(evicted.iter().cloned());
            chat_history.push(Message::user(SUMMARY_PROMPT));
            let completion = CompletionRequest {
                model: None,
                chat_history,
                documents: Vec::new(),
                tools: Vec::new(),
                temperature: None,
                max_tokens: None,
                tool_choice: None,
                additional_params: None,
                output_schema: None,
                record_telemetry_content: false,
            };
            let response = self
                .model
                .call(completion)
                .await
                .map_err(|error| MemoryError::Backend(Box::new(error)))?;
            let text = response.text();
            if text.trim().is_empty() {
                return Err(MemoryError::Policy(
                    "summarization model returned no text".into(),
                ));
            }
            Ok(ModelSummary {
                text,
                usage: response.usage,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use rig::message::{
        AssistantContent, ToolCall, ToolFunction, ToolName, ToolResultContent, UserContent,
    };
    use rig_memory::SlidingWindowMemory;

    use super::*;

    fn tool_exchange(id: &str) -> Vec<Message> {
        let call = ToolCall::from_wire(
            id,
            ToolFunction::new(
                ToolName::new("add").unwrap(),
                serde_json::json!({"x": 1, "y": 2}),
            ),
        );
        vec![
            Message::Assistant {
                id: None,
                content: vec![AssistantContent::ToolCall(call.clone())],
            },
            Message::User {
                content: vec![UserContent::tool_result(
                    call.id.clone(),
                    call.function.name.clone(),
                    vec![ToolResultContent::text("3")],
                )],
            },
        ]
    }

    /// prompt, tool call, tool result, answer, prompt, answer, prompt, tool
    /// call, tool result, answer.
    fn transcript() -> Vec<Message> {
        let mut messages = vec![Message::user("first")];
        messages.extend(tool_exchange("call-1"));
        messages.push(Message::assistant("three"));
        messages.push(Message::user("second"));
        messages.push(Message::assistant("ok"));
        messages.push(Message::user("third"));
        messages.extend(tool_exchange("call-2"));
        messages.push(Message::assistant("three again"));
        messages
    }

    /// Joins the evicted user texts; the artifact is its own text.
    struct JoinCompactor;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Joined(String);

    impl From<Joined> for Message {
        fn from(joined: Joined) -> Self {
            Message::system(joined.0)
        }
    }

    impl Compactor for JoinCompactor {
        type Artifact = Joined;

        fn compact<'a>(
            &'a self,
            _conversation_id: &'a ConversationId,
            evicted: &'a [Message],
            carry_over: Option<&'a Self::Artifact>,
        ) -> WasmBoxedFuture<'a, Result<Self::Artifact, MemoryError>> {
            Box::pin(async move {
                let mut parts: Vec<String> = carry_over
                    .map(|prior| prior.0.clone())
                    .into_iter()
                    .collect();
                parts.extend(evicted.iter().filter_map(|message| match message {
                    Message::User { content } => content.iter().find_map(|item| match item {
                        UserContent::Text(text) => Some(text.text.clone()),
                        _ => None,
                    }),
                    _ => None,
                }));
                Ok(Joined(parts.join("|")))
            })
        }
    }

    fn output(cutoff: usize, text: &str) -> CompactionOutput {
        CompactionOutput {
            cutoff,
            policy_version: "1".into(),
            input_messages: cutoff,
            artifact: Some(CompactionArtifact {
                value: Value::String(text.into()),
                message: Message::system(text),
            }),
        }
    }

    #[tokio::test]
    async fn policy_evicts_and_compactor_carries_the_prior_artifact() {
        let compaction =
            Compaction::new(SlidingWindowMemory::last_messages(2), JoinCompactor).version("test");
        let mut state = ContextState::new(transcript());
        // 10 messages, keep 2: the window would start at the second tool
        // result, so the policy demotes through it. Cutoff 9.
        let output = compaction.run(state.request("s", "test")).await.unwrap();
        assert_eq!(output.cutoff, 9);
        assert_eq!(output.input_messages, 9);
        assert_eq!(output.policy_version, "test");
        assert!(state.apply(output, "test").unwrap());
        let context = state.active_context();
        assert_eq!(context.len(), 2);
        validate_canonical(&context).unwrap();
        assert_eq!(state.transcript.len(), 10);
        let record = state.compaction.as_ref().unwrap();
        assert_eq!(
            record.artifact.value,
            serde_json::json!("first|second|third")
        );

        // Nothing new demoted: a no-op output leaves the record in place.
        let output = compaction.run(state.request("s", "test")).await.unwrap();
        assert!(output.artifact.is_none());
        assert_eq!(output.cutoff, 9);
        assert!(!state.apply(output, "test").unwrap());
        assert_eq!(state.applied_cutoff(), 9);

        // The next round compacts only the new prefix and carries the prior
        // artifact into the compactor.
        state.append([
            Message::user("fourth"),
            Message::assistant("done"),
            Message::user("fifth"),
            Message::assistant("done again"),
        ]);
        let output = compaction.run(state.request("s", "test")).await.unwrap();
        assert_eq!(output.cutoff, 12);
        assert_eq!(output.input_messages, 3);
        assert!(state.apply(output, "test").unwrap());
        validate_canonical(&state.active_context()).unwrap();
        assert_eq!(
            state.compaction.as_ref().unwrap().artifact.value,
            serde_json::json!("first|second|third|fourth")
        );
    }

    #[test]
    fn stale_output_does_not_move_the_cutoff_backwards() {
        let mut state = ContextState::new(transcript());
        assert!(state.apply(output(6, "newer"), "1").unwrap());
        assert!(!state.apply(output(4, "older"), "1").unwrap());
        assert!(!state.apply(output(6, "same cutoff"), "1").unwrap());
        assert_eq!(
            state.compaction.as_ref().unwrap().artifact.value,
            serde_json::json!("newer")
        );
        assert_eq!(state.applied_cutoff(), 6);
    }

    #[test]
    fn outputs_that_split_a_tool_exchange_are_rejected() {
        let mut state = ContextState::new(transcript());
        assert!(matches!(
            state.apply(output(2, "mid exchange"), "1").unwrap_err(),
            CompactionError::NotCanonical { cutoff: 2, .. }
        ));
        assert_eq!(
            state.apply(output(11, "beyond"), "1").unwrap_err(),
            CompactionError::OutOfRange {
                cutoff: 11,
                len: 10
            }
        );
        assert!(state.compaction.is_none());
    }

    #[test]
    fn restored_records_are_checked() {
        let record = CompactionRecord {
            format_version: COMPACTION_FORMAT_VERSION + 1,
            cutoff: 1,
            policy_version: "1".into(),
            artifact: CompactionArtifact {
                value: Value::Null,
                message: Message::system("x"),
            },
            input_messages: 1,
        };
        assert_eq!(
            ContextState::new(transcript())
                .with_compaction(Some(record.clone()))
                .unwrap_err(),
            CompactionError::UnsupportedFormat(COMPACTION_FORMAT_VERSION + 1)
        );
        let mut record = record;
        record.format_version = COMPACTION_FORMAT_VERSION;
        record.cutoff = 11;
        assert!(matches!(
            ContextState::new(transcript()).with_compaction(Some(record.clone())),
            Err(CompactionError::OutOfRange { .. })
        ));
        record.cutoff = 2;
        assert!(matches!(
            ContextState::new(transcript()).with_compaction(Some(record)),
            Err(CompactionError::NotCanonical { .. })
        ));
    }

    #[tokio::test]
    async fn version_mismatch_calls_neither_policy_nor_model() {
        struct NoPolicy;
        impl MemoryPolicy for NoPolicy {
            fn apply(&self, _: Vec<Message>) -> Result<Vec<Message>, MemoryError> {
                panic!("version mismatch must precede policy execution")
            }
        }
        let model = rig::test_utils::MockCompletionModel::from_turns([]);
        let compaction =
            Compaction::new(NoPolicy, ModelCompactor::new(model.clone())).version("v2");
        let mut state = ContextState::new(transcript());
        assert!(
            compaction
                .run(state.request("s", "v1"))
                .await
                .unwrap_err()
                .contains("version")
        );
        let mut prior = output(4, "prior");
        prior.policy_version = "v1".into();
        state.apply(prior, "v1").unwrap();
        assert!(
            compaction
                .run(state.request("s", "v2"))
                .await
                .unwrap_err()
                .contains("version")
        );
        let before = state.clone();
        assert_eq!(
            state.apply(output(6, "new"), "v1"),
            Err(CompactionError::VersionMismatch)
        );
        assert_eq!(state, before);
        assert!(model.requests().is_empty());
    }
}
