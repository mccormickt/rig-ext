use std::sync::Arc;

use duroxide::runtime::registry::{ActivityRegistry, OrchestrationRegistry};
use rig::{completion::CompletionModel, tool::ToolSet};

use crate::{
    activities,
    activity_types::{ToolActivityInput, ToolActivityOutput},
    config::DurableAgentConfig,
    names::RuntimeNames,
    types::AgentInput,
};

pub fn activity_registry<M>(model: M, tools: ToolSet) -> ActivityRegistry
where
    M: CompletionModel + Send + Sync + 'static,
{
    activity_registry_with_names(model, tools, &RuntimeNames::legacy())
}

pub(crate) fn activity_registry_with_names<M>(
    model: M,
    tools: ToolSet,
    names: &RuntimeNames,
) -> ActivityRegistry
where
    M: CompletionModel + Send + Sync + 'static,
{
    let model = Arc::new(model);
    let completion_model = Arc::clone(&model);
    let tools = Arc::new(tools);
    let completion_activity = names.completion_activity.clone();
    let streaming_completion_activity = names.streaming_completion_activity.clone();
    let tool_activity = names.tool_activity.clone();
    ActivityRegistry::builder()
        .register_typed(completion_activity, move |_ctx, request| {
            let model = Arc::clone(&completion_model);
            async move { activities::completion::complete(model.as_ref(), request).await }
        })
        .register_typed(streaming_completion_activity, move |_ctx, request| {
            let model = Arc::clone(&model);
            async move { activities::completion::stream(model.as_ref(), request).await }
        })
        .register_typed(tool_activity, move |_ctx, input: ToolActivityInput| {
            let tools = Arc::clone(&tools);
            async move { activities::tool::execute(&tools, input).await }
        })
        .build()
}

pub fn orchestration_registry(config: DurableAgentConfig) -> OrchestrationRegistry {
    orchestration_registry_with_names(config, RuntimeNames::legacy(), None)
}

pub(crate) fn orchestration_registry_with_names(
    config: DurableAgentConfig,
    names: RuntimeNames,
    version: Option<&str>,
) -> OrchestrationRegistry {
    let orchestration = names.orchestration.clone();
    let handler = move |ctx, input: AgentInput| {
        let config = config.clone();
        let names = names.clone();
        async move { crate::orchestration::run_with_names(ctx, input, config, names).await }
    };
    let builder = OrchestrationRegistry::builder();
    if let Some(version) = version {
        builder
            .register_versioned_typed(orchestration, version, handler)
            .build()
    } else {
        builder.register_typed(orchestration, handler).build()
    }
}

// Keep this wire type checked at this boundary.
const _: fn(ToolActivityOutput) = |_| {};
