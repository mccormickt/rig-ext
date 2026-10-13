use std::sync::Arc;

use rig::{
    AgentBuilder,
    test_utils::{MockCompletionModel, MockTurn},
};
use rig_codemode::{Catalog, CatalogEntry, CodeMode, DynamicToolDispatcher, ExecutionRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let researcher = AgentBuilder::new(MockCompletionModel::from_turns([MockTurn::text(
        "Use a staged rollout.",
    )]))
    .name("research")
    .description("Research a release plan")
    .build();
    let reviewer = AgentBuilder::new(MockCompletionModel::from_turns([MockTurn::text(
        "Keep rollback available.",
    )]))
    .name("review")
    .description("Review a release plan")
    .build();
    let dispatcher = DynamicToolDispatcher::new([researcher.into_tool()?, reviewer.into_tool()?])?;
    let catalog = Catalog::new(
        dispatcher
            .definitions()
            .iter()
            .map(CatalogEntry::from_definition),
    )?;
    let codemode = CodeMode::builder(catalog, Arc::new(dispatcher)).build()?;
    let report = codemode
        .execute(ExecutionRequest::new(
            r#"
        const plans = await Promise.all([
            tools.research({ prompt: "Plan a release." }),
            tools.review({ prompt: "Find release risks." })
        ]);
        text(plans.join("\n"));
        return plans;
    "#,
        ))
        .await?;
    println!("{}", report.render_text());
    Ok(())
}
