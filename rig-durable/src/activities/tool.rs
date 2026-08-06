use rig::tool::{ToolContext, ToolErrorKind, ToolSet};

use crate::activity_types::{ToolActivityInput, ToolActivityOutput};

pub async fn execute(
    toolset: &ToolSet,
    input: ToolActivityInput,
) -> Result<ToolActivityOutput, String> {
    let result = toolset
        .execute(&input.name, input.arguments, &mut ToolContext::new())
        .await;
    if let Some(error) = result.error() {
        let is_infrastructure_failure =
            error.retryable() == Some(true) || matches!(error.kind(), ToolErrorKind::Provider);
        if is_infrastructure_failure {
            return Err(error.to_string());
        }
    }
    Ok(ToolActivityOutput {
        content: result.output().as_content().clone(),
        is_error: result.is_error() || result.is_refused(),
    })
}
