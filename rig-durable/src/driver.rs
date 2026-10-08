use rig::{
    agent::{
        AgentRun, AgentRunStep, InvalidToolCallAction, ModelTurn, ModelTurnOutcome,
        PendingToolCall, PromptResponse,
    },
    completion::{CompletionRequest, Message, ToolDefinition},
    message::{ToolCall, ToolChoice, ToolResultContent, UserContent},
};
use sha2::{Digest, Sha256};

use crate::{
    approval::ApprovalRequest,
    identity::{LogicalCallKey, arguments_digest},
};

pub(crate) struct CompletionOptions {
    pub preamble: Option<String>,
    pub tools: Vec<ToolDefinition>,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u64>,
    pub tool_choice: Option<ToolChoice>,
    pub additional_params: Option<serde_json::Value>,
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum Effect {
    Model {
        request: CompletionRequest,
        turn: usize,
    },
    Tools {
        calls: Vec<PendingToolCall>,
    },
    Done(PromptResponse),
}

/// Advance Rig's pure state machine to the next external operation.
pub(crate) fn next_effect(
    agent: &mut AgentRun,
    options: CompletionOptions,
) -> Result<Effect, String> {
    Ok(
        match agent.next_step().map_err(|error| error.to_string())? {
            AgentRunStep::CallModel {
                prompt,
                history,
                turn,
            } => Effect::Model {
                request: completion_request(prompt, history, options),
                turn,
            },
            AgentRunStep::CallTools { calls } => Effect::Tools { calls },
            AgentRunStep::Done(response) => Effect::Done(response),
        },
    )
}

impl From<&crate::DurableAgentConfig> for CompletionOptions {
    fn from(config: &crate::DurableAgentConfig) -> Self {
        Self {
            preamble: config.preamble.clone(),
            tools: config.tools.definitions(),
            temperature: config.completion.temperature,
            max_tokens: config.completion.max_tokens,
            tool_choice: config.completion.tool_choice.clone(),
            additional_params: config.completion.additional_params.clone(),
        }
    }
}

pub(crate) fn completion_request(
    prompt: Message,
    mut history: Vec<Message>,
    options: CompletionOptions,
) -> CompletionRequest {
    if let Some(preamble) = options.preamble {
        history.insert(0, Message::system(preamble));
    }
    history.push(prompt);
    CompletionRequest {
        model: None,
        chat_history: history,
        documents: Vec::new(),
        tools: options.tools,
        temperature: options.temperature,
        max_tokens: options.max_tokens,
        tool_choice: options.tool_choice,
        additional_params: options.additional_params,
        output_schema: None,
        record_telemetry_content: false,
    }
}

pub(crate) fn apply_model_turn(agent: &mut AgentRun, turn: ModelTurn) -> Result<(), String> {
    if matches!(
        agent
            .model_response(turn)
            .map_err(|error| error.to_string())?,
        ModelTurnOutcome::NeedsResolution(_)
    ) {
        agent
            .resolve_invalid_tool_call(InvalidToolCallAction::fail())
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

#[cfg(any(feature = "duroxide", feature = "temporal"))]
pub(crate) fn approval_request(
    call: &ToolCall,
    prompt_index: u64,
    turn: usize,
    call_index: usize,
) -> Result<ApprovalRequest, String> {
    let arguments = call.function.arguments.clone();
    let digest = Sha256::digest(serde_json::to_vec(&arguments).map_err(|error| error.to_string())?);
    Ok(ApprovalRequest {
        approval_id: format!(
            "prompt-{prompt_index}-turn-{turn}-call-{call_index}-{}-{digest:x}",
            call.id
        ),
        tool_name: call.function.name.to_string(),
        arguments,
        tool_call_id: call.id.to_string(),
        call_id: call.id.provider().map(|id| id.call_id.clone()),
    })
}

/// Approval bound to the logical call, the final argument digest, and the
/// tool implementation version. Rewritten arguments or a new implementation
/// version produce a new request, so an old decision cannot authorize them.
pub(crate) fn logical_approval_request(
    call: &ToolCall,
    key: &LogicalCallKey,
    implementation_version: &str,
) -> Result<ApprovalRequest, String> {
    let arguments = call.function.arguments.clone();
    let digest = arguments_digest(&arguments).map_err(|error| error.to_string())?;
    let binding = format!("{}\n{digest}\n{implementation_version}", key.canonical());
    Ok(ApprovalRequest {
        approval_id: format!("approval-v2-{:x}", Sha256::digest(binding.as_bytes())),
        tool_name: call.function.name.to_string(),
        arguments,
        tool_call_id: call.id.to_string(),
        call_id: call.id.provider().map(|id| id.call_id.clone()),
    })
}

pub(crate) fn deny_tool(mut pending: PendingToolCall, reason: Option<String>) -> PendingToolCall {
    let content = vec![ToolResultContent::text(format!(
        "Tool execution denied by human approval{}",
        reason.map(|value| format!(": {value}")).unwrap_or_default()
    ))];
    pending.preresolved_result = Some(tool_result(&pending.tool_call, content));
    pending
}

pub(crate) fn tool_result(call: &ToolCall, content: Vec<ToolResultContent>) -> UserContent {
    UserContent::tool_result(call.id.clone(), call.function.name.clone(), content)
}
