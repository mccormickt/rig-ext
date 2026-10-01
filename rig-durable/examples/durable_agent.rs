use std::convert::Infallible;

use rig::{
    providers::openai::{self, OpenAI},
    tool::{Tool, ToolContext},
};
use rig_durable::{AgentOrchestrator, DurableAgent};
use serde::Deserialize;

struct Add;

#[derive(Deserialize)]
struct AddArgs {
    left: i64,
    right: i64,
}

impl Tool for Add {
    const NAME: &'static str = "add";
    type Args = AddArgs;
    type Output = i64;
    type Error = Infallible;

    fn description(&self) -> String {
        "Add two integers".into()
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({"type":"object","properties":{"left":{"type":"integer"},"right":{"type":"integer"}},"required":["left","right"]})
    }

    async fn call(&self, _context: &mut ToolContext, args: AddArgs) -> Result<i64, Infallible> {
        Ok(args.left + args.right)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("OPENAI_API_KEY").is_none() {
        println!("OPENAI_API_KEY is not set; durable_agent is a no-op");
        return Ok(());
    }

    let model = OpenAI::from_env()?.completion(openai::GPT_4O_MINI);
    let definition = DurableAgent::builder("calculator", model)
        .preamble("Use the calculator tool for arithmetic.")
        .tool(Add)
        .build()?;
    let orchestrator = AgentOrchestrator::sqlite("sqlite::memory:")
        .await?
        .register(definition)?
        .start()
        .await?;
    let answer = orchestrator
        .agent("calculator")?
        .prompt("Use add to calculate 20 + 22.")
        .await?;
    println!("{answer}");
    orchestrator.shutdown(None).await;
    Ok(())
}
