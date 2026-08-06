use rig::test_utils::{MockAddTool, MockCompletionModel, MockStreamEvent};
use rig_duroxide::{AgentOrchestrator, CompletionMode, DurableAgent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let model = MockCompletionModel::from_stream_turns([
        vec![
            MockStreamEvent::tool_call_name_delta("call-1", "internal-1", "add"),
            MockStreamEvent::tool_call_arguments_delta(
                "call-1",
                "internal-1",
                r#"{"x":20,"y":22}"#,
            ),
            MockStreamEvent::tool_call("call-1", "add", serde_json::json!({"x": 20, "y": 22})),
            MockStreamEvent::final_response_with_default_usage(),
        ],
        vec![
            MockStreamEvent::text("The answer "),
            MockStreamEvent::text("is 42."),
            MockStreamEvent::final_response_with_total_tokens(4),
        ],
    ]);
    let definition = DurableAgent::builder("streaming-calculator", model)
        .tool(MockAddTool)
        .completion_mode(CompletionMode::Streaming)
        .build()?;
    let orchestrator = AgentOrchestrator::sqlite("sqlite::memory:")
        .await?
        .register(definition)?
        .start()
        .await?;
    let answer = orchestrator
        .agent("streaming-calculator")?
        .prompt("add")
        .await?;
    println!("{answer}");
    println!(
        "Provider tokens become durably visible only after the model activity completes; live at-least-once side channels are out of scope."
    );
    orchestrator.shutdown(None).await;
    Ok(())
}
