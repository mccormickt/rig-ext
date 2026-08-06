use rig::test_utils::{MockCompletionModel, MockTurn};
use rig_duroxide::{AgentOrchestrator, DurableAgent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let child = DurableAgent::builder(
        "weather-researcher",
        MockCompletionModel::new([MockTurn::text("Oslo: clear")]),
    )
    .description("Research weather through a durable child agent")
    .build()?;
    let parent = DurableAgent::builder(
        "travel-assistant",
        MockCompletionModel::new([
            MockTurn::tool_call(
                "weather-call",
                "research_weather",
                serde_json::json!({"prompt":"Weather in Oslo"}),
            ),
            MockTurn::text("The durable child agent reports clear weather."),
        ]),
    )
    .sub_agent("research_weather", child)?
    .build()?;
    let orchestrator = AgentOrchestrator::sqlite("sqlite::memory:")
        .await?
        .register(parent)?
        .start()
        .await?;
    let answer = orchestrator
        .agent("travel-assistant")?
        .prompt("Get Oslo weather")
        .await?;
    println!("{answer}");
    orchestrator.shutdown(None).await;
    Ok(())
}
