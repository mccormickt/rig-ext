use rig::{
    DynModel,
    completion::{CompletionRequest, Message},
    operation::Completion,
};

use crate::compaction::{CompactionOutput, CompactionRequest};

const SUMMARY_PROMPT: &str = "Summarize the conversation above for an assistant that will \
continue it. Reply with the summary only.";

/// Summarize a transcript window in one model call. The output carries the
/// cutoff and policy version of its request so the caller can reject a
/// summary that no longer matches the context it was planned for.
pub async fn summarize(
    model: &DynModel<Completion>,
    request: CompactionRequest,
) -> Result<CompactionOutput, String> {
    let mut chat_history = Vec::with_capacity(request.messages.len() + 3);
    chat_history.push(Message::system(request.instructions.clone()));
    if let Some(prior) = &request.prior_summary {
        chat_history.push(Message::user(format!(
            "Summary of the conversation before this window:\n{prior}"
        )));
    }
    chat_history.extend(request.messages.iter().cloned());
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
    let response = model
        .call(completion)
        .await
        .map_err(|error| error.to_string())?;
    let summary = response.text();
    if summary.trim().is_empty() {
        return Err("summarization model returned no text".into());
    }
    Ok(CompactionOutput {
        cutoff: request.cutoff,
        policy_version: request.policy_version,
        summary,
        usage: response.usage,
        input_messages: request.messages.len(),
    })
}
