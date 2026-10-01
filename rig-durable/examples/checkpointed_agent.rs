use std::num::NonZeroU32;

use rig::test_utils::{MockAddTool, MockCompletionModel, MockTurn};
use rig_durable::{AgentOrchestrator, CheckpointConfig, CheckpointPolicy, DurableAgent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let model = MockCompletionModel::new([
        MockTurn::tool_call("first", "add", serde_json::json!({"x":20,"y":22})),
        MockTurn::tool_call("second", "add", serde_json::json!({"x":42,"y":1})),
        MockTurn::text("The final answer is 43."),
    ]);
    let definition = DurableAgent::builder("checkpointed-calculator", model)
        .tool(MockAddTool)
        .checkpoint(CheckpointConfig {
            policy: CheckpointPolicy::Every(NonZeroU32::new(1).unwrap()),
            target_version: None,
        })
        .build()?;
    let orchestrator = AgentOrchestrator::sqlite("sqlite::memory:")
        .await?
        .register(definition)?
        .start()
        .await?;
    let run = orchestrator
        .agent("checkpointed-calculator")?
        .start_with_id("checkpoint-example", "Calculate")
        .await?;
    let response = run.wait().await?;
    let executions = orchestrator
        .client()
        .list_executions(run.instance_id())
        .await?;
    println!("{}", response.output);
    println!("Duroxide executions: {executions:?}");
    orchestrator.shutdown(None).await;
    Ok(())
}
