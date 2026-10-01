use rig::tool::{ToolContext, ToolErrorKind, ToolSet};

use crate::activity_types::{ToolActivityInput, ToolActivityOutput};

pub async fn execute(
    toolset: &ToolSet,
    input: ToolActivityInput,
) -> Result<ToolActivityOutput, String> {
    let mut context = ToolContext::new();
    context.insert(input.invocation);
    let result = toolset
        .execute(&input.name, input.arguments, &mut context)
        .await;
    if let Some(error) = result.error() {
        let is_infrastructure_failure =
            error.retryable() == Some(true) || matches!(error.kind(), ToolErrorKind::Provider);
        if is_infrastructure_failure {
            return Err(error.to_string());
        }
    }
    Ok(ToolActivityOutput {
        content: result.output().as_content().to_vec(),
        is_error: result.is_error() || result.is_refused(),
    })
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use rig::tool::Tool;
    use serde::Deserialize;

    use super::*;
    use crate::activity_types::ToolInvocation;

    struct ReadInvocation;

    #[derive(Deserialize)]
    struct NoArgs {}

    impl Tool for ReadInvocation {
        const NAME: &'static str = "invocation";
        type Args = NoArgs;
        type Output = String;
        type Error = Infallible;

        fn description(&self) -> String {
            "Return the durable invocation identity".into()
        }

        fn parameters(&self) -> serde_json::Value {
            serde_json::json!({"type":"object"})
        }

        async fn call(
            &self,
            context: &mut ToolContext,
            _args: NoArgs,
        ) -> Result<Self::Output, Self::Error> {
            let invocation = context.require::<ToolInvocation>().unwrap();
            Ok(format!(
                "{}:{}:{}:{}",
                invocation.execution_id,
                invocation.prompt_index,
                invocation.turn,
                invocation.call_index
            ))
        }
    }

    #[tokio::test]
    async fn exposes_the_stable_invocation_to_rig_tools() {
        let input = ToolActivityInput {
            name: "invocation".into(),
            arguments: "{}".into(),
            invocation: ToolInvocation {
                execution_id: "run-1".into(),
                prompt_index: 4,
                turn: 2,
                call_index: 3,
            },
        };
        let output = execute(&ToolSet::from_tools(vec![ReadInvocation]), input)
            .await
            .unwrap();

        assert_eq!(
            output.content,
            vec![rig::message::ToolResultContent::text("run-1:4:2:3")]
        );
    }
}
