use duroxide::RetryPolicy;
use rig::test_utils::{MockAddTool, MockCompletionModel, MockTurn};
use rig_durable::{AgentOrchestrator, DurableAgent, ToolOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let model = MockCompletionModel::new([
        MockTurn::tool_call(
            "approved-call",
            "add",
            serde_json::json!({"x": 20, "y": 22}),
        ),
        MockTurn::text("The approved answer is 42."),
    ]);
    let definition = DurableAgent::builder("approved-calculator", model)
        .tool_with(
            MockAddTool,
            ToolOptions::default()
                .retry(RetryPolicy::new(1))
                .require_approval(),
        )
        .build()?;
    let orchestrator = AgentOrchestrator::sqlite("sqlite::memory:")
        .await?
        .register(definition)?
        .start()
        .await?;
    let run = orchestrator
        .agent("approved-calculator")?
        .start_with_id("human-approval-example", "Add 20 and 22")
        .await?;

    let request = run.next_approval().await?;
    println!("approval requested: {request:?}");
    run.approve(&request).await?;
    println!("{}", run.wait().await?.output);
    orchestrator.shutdown(None).await;
    Ok(())
}
