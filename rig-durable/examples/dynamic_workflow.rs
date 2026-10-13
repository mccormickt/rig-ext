use rig::test_utils::{MockCompletionModel, MockTurn};
use rig_codemode::Limits;
use rig_durable::{AgentOrchestrator, DurableAgent};

const CODE: &str = r#"
const [facts, risks] = await Promise.all([
    tools.research({ prompt: "Find facts about a database migration." }),
    tools.assess({ prompt: "Find risks in a database migration." })
]);
const review = risks.includes("backup")
    ? await tools.review({ prompt: `Review this plan: ${facts}\nRisks: ${risks}` })
    : "No extra review required.";
text(review);
return { facts, risks, review };
"#;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let researcher = DurableAgent::builder(
        "researcher",
        MockCompletionModel::from_turns([MockTurn::text("Migrate in small batches.")]),
    )
    .description("Research migration facts")
    .build()?;
    let assessor = DurableAgent::builder(
        "assessor",
        MockCompletionModel::from_turns([MockTurn::text("Take a backup before migration.")]),
    )
    .description("Identify migration risks")
    .build()?;
    let reviewer = DurableAgent::builder(
        "reviewer",
        MockCompletionModel::from_turns([MockTurn::text("Approved with a verified backup.")]),
    )
    .description("Review a plan and its risks")
    .build()?;
    let model = MockCompletionModel::from_turns([
        MockTurn::tool_call("workflow", "codemode", serde_json::json!({"code": CODE})),
        MockTurn::text("Approved with a verified backup."),
    ]);
    let coordinator = DurableAgent::builder("coordinator", model.clone())
        .sub_agent("research", researcher)?
        .sub_agent("assess", assessor)?
        .sub_agent("review", reviewer)?
        .code_mode(Limits::default())
        .build()?;
    let orchestrator = AgentOrchestrator::sqlite("sqlite::memory:")
        .await?
        .register(coordinator)?
        .start()
        .await?;
    let answer = orchestrator
        .agent("coordinator")?
        .prompt("Plan a database migration.")
        .await?;
    println!("{answer}");
    let requests = model.requests();
    let completed = requests
        .get(1)
        .ok_or("coordinator did not receive the workflow report")?;
    println!(
        "Workflow report in the coordinator's context:\n{}",
        serde_json::to_string_pretty(&completed.chat_history)?
    );
    orchestrator.shutdown(None).await;
    Ok(())
}
