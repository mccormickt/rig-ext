use rig::{
    agent::{AgentRun, InvalidToolCallAction, ModelTurn, ModelTurnOutcome, PendingToolCall},
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
