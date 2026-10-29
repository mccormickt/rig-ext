use rig::message::{ToolCall, ToolResultContent, UserContent};
use sha2::{Digest, Sha256};

use crate::{
    activities::completion::CompletionOutput,
    approval::ApprovalRequest,
    identity::{LogicalCallKey, arguments_digest},
    persistent::{PendingToolCall, Run},
};

pub(crate) use crate::persistent::{CompletionOptions, Effect};

pub(crate) fn next_effect(agent: &mut Run, options: CompletionOptions) -> Result<Effect, String> {
    agent.plan(options)
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

pub(crate) fn apply_model_turn(agent: &mut Run, turn: CompletionOutput) -> Result<(), String> {
    agent.model_response(turn.response)
}

#[cfg(any(feature = "duroxide", feature = "temporal"))]
pub(crate) fn approval_request(
    call: &ToolCall,
    prompt_index: u64,
    turn: usize,
    call_index: usize,
) -> Result<ApprovalRequest, String> {
    let arguments = serde_json::Value::Object(call.function.arguments.clone());
    let digest = arguments_digest(&arguments).map_err(|error| error.to_string())?;
    Ok(ApprovalRequest {
        approval_id: format!(
            "prompt-{prompt_index}-turn-{turn}-call-{call_index}-{}-{digest}",
            call.id
        ),
        tool_name: call.function.name.to_string(),
        arguments,
        tool_call_id: call.id.to_string(),
        call_id: call.id.provider().map(|id| id.as_str().to_owned()),
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
    let arguments = serde_json::Value::Object(call.function.arguments.clone());
    let digest = arguments_digest(&arguments).map_err(|error| error.to_string())?;
    let binding = format!("{}\n{digest}\n{implementation_version}", key.canonical());
    Ok(ApprovalRequest {
        approval_id: format!("approval-v2-{:x}", Sha256::digest(binding.as_bytes())),
        tool_name: call.function.name.to_string(),
        arguments,
        tool_call_id: call.id.to_string(),
        call_id: call.id.provider().map(|id| id.as_str().to_owned()),
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
