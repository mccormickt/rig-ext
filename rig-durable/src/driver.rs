use rig::message::{ToolCall, ToolResultContent, UserContent};
use sha2::{Digest, Sha256};

use crate::{
    activities::completion::CompletionOutput,
    approval::ApprovalRequest,
    identity::{LogicalCallKey, arguments_digest},
    persistent::{PendingToolCall, Run},
};

pub(crate) use crate::persistent::{CompletionOptions, Effect};

pub(crate) trait State {
    fn next(&mut self, options: CompletionOptions) -> Result<Effect, String>;
    fn complete(&mut self, output: CompletionOutput) -> Result<(), String>;
}

pub(crate) fn next_effect(
    agent: &mut impl State,
    options: CompletionOptions,
) -> Result<Effect, String> {
    agent.next(options)
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

impl State for Run {
    fn next(&mut self, options: CompletionOptions) -> Result<Effect, String> {
        self.plan(options)
    }
    fn complete(&mut self, output: CompletionOutput) -> Result<(), String> {
        self.model_response(output.response)
    }
}

pub(crate) fn apply_model_turn(
    agent: &mut impl State,
    turn: CompletionOutput,
) -> Result<(), String> {
    agent.complete(turn)
}

#[cfg(feature = "temporal")]
impl State for rig::AgentRun {
    fn next(&mut self, options: CompletionOptions) -> Result<Effect, String> {
        use rig::{AgentRun, agent::AgentRunStep, run::PendingToolCall as Call};
        loop {
            match AgentRun::next_step(self).map_err(|error| error.to_string())? {
                AgentRunStep::CallModel {
                    prompt,
                    mut history,
                    turn,
                } => {
                    if let Some(preamble) = options.preamble {
                        history.insert(
                            0,
                            std::sync::Arc::new(rig::completion::Message::system(preamble)),
                        );
                    }
                    history.push(prompt);
                    let mut request = rig::completion::CompletionRequest::new("");
                    request.chat_history = history;
                    request.tools = options.tools.into_iter().map(std::sync::Arc::new).collect();
                    request.temperature = options.temperature;
                    request.max_tokens = options.max_tokens;
                    request.tool_choice = options.tool_choice;
                    request.additional_params = options.additional_params;
                    return Ok(Effect::Model { request, turn });
                }
                AgentRunStep::CallTools { calls } => {
                    let mut executable = Vec::new();
                    for (call_index, call) in calls.into_iter().enumerate() {
                        match call {
                            Call::Execute(call) => executable.push(PendingToolCall {
                                tool_call: call.tool_call().clone(),
                                call_index,
                                dispatch_id: rig::effect::EffectId::from_raw(call_index as u64),
                                preresolved_result: None,
                            }),
                            Call::Malformed(call) => self
                                .answer(call.answer(None))
                                .map_err(|error| error.to_string())?,
                            _ => return Err("unsupported Rig pending call".into()),
                        }
                    }
                    if !executable.is_empty() {
                        return Ok(Effect::Tools { calls: executable });
                    }
                }
                AgentRunStep::Done(response) => return Ok(Effect::Done(response)),
            }
        }
    }

    fn complete(&mut self, output: CompletionOutput) -> Result<(), String> {
        use rig::run::{InvalidToolCallAction, ModelTurn, ModelTurnOutcome};
        if matches!(
            self.model_response(ModelTurn::from_policy(&output.response, output.policy))
                .map_err(|error| error.to_string())?,
            ModelTurnOutcome::NeedsResolution(_)
        ) {
            self.resolve_invalid_tool_call(InvalidToolCallAction::fail())
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

#[cfg(feature = "temporal")]
pub(crate) fn temporal_tool_results(
    agent: &mut rig::AgentRun,
    results: Vec<UserContent>,
) -> Result<(), String> {
    use rig::{
        run::{AgentRunStep, PendingToolCall},
        tool::{ToolOutput, ToolResult},
    };
    let AgentRunStep::CallTools { calls } = agent.next_step().map_err(|error| error.to_string())?
    else {
        return Err("Rig run has no pending tools".into());
    };
    if calls.len() != results.len() {
        return Err("tool settlement count mismatch".into());
    }
    for (call, result) in calls.into_iter().zip(results) {
        let (PendingToolCall::Execute(call), UserContent::ToolResult(result)) = (call, result)
        else {
            return Err("expected executable tool settlement".into());
        };
        let output = ToolOutput::content(result.content).map_err(|error| error.to_string())?;
        agent
            .answer(call.answer(ToolResult::success(output)))
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
