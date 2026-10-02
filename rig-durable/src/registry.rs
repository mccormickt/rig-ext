use std::sync::Arc;

use duroxide::runtime::registry::{ActivityRegistry, OrchestrationRegistry};
use rig::{DynModel, operation::Completion, tool::ToolSet};

use crate::{
    activities::{self, tool::ToolExecutor},
    activity_types::{ToolActivityInput, ToolActivityOutput},
    config::DurableAgentConfig,
    guard::InvocationGuardStore,
    names::RuntimeNames,
    session::SessionInput,
    tools::ToolCatalog,
    types::AgentInput,
};

pub fn activity_registry(
    model: impl Into<DynModel<Completion>>,
    tools: ToolSet,
) -> ActivityRegistry {
    activity_registry_with_names(
        model.into(),
        ToolExecutor::new(Arc::new(tools)),
        &RuntimeNames::legacy(),
    )
}

/// Like [`activity_registry`], with the catalog's tool policies and an
/// invocation guard store for tools whose policy never repeats an uncertain
/// effect.
pub fn activity_registry_with_guard(
    model: impl Into<DynModel<Completion>>,
    tools: ToolSet,
    catalog: &ToolCatalog,
    guard: Arc<dyn InvocationGuardStore>,
) -> ActivityRegistry {
    let executor = ToolExecutor::new(Arc::new(tools))
        .with_guard(Some(guard))
        .with_registered_policies(catalog.policies());
    activity_registry_with_names(model.into(), executor, &RuntimeNames::legacy())
}

pub(crate) fn activity_registry_with_names(
    model: DynModel<Completion>,
    executor: ToolExecutor,
    names: &RuntimeNames,
) -> ActivityRegistry {
    let model = Arc::new(model);
    let completion_model = Arc::clone(&model);
    let compaction_model = Arc::clone(&model);
    let executor = Arc::new(executor);
    let logical_executor = Arc::clone(&executor);
    let completion_activity = names.completion_activity.clone();
    let streaming_completion_activity = names.streaming_completion_activity.clone();
    let tool_activity = names.tool_activity.clone();
    let logical_tool_activity = names.logical_tool_activity.clone();
    let compaction_activity = names.compaction_activity.clone();
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
            let executor = Arc::clone(&executor);
            async move { executor.execute(input).await }
        })
        .register_typed(
            logical_tool_activity,
            move |_ctx, input: ToolActivityInput| {
                let executor = Arc::clone(&logical_executor);
                async move { executor.execute(input).await }
            },
        )
        .register_typed(compaction_activity, move |_ctx, request| {
            let model = Arc::clone(&compaction_model);
            async move { activities::compaction::summarize(model.as_ref(), request).await }
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
    let session_orchestration = names.session_orchestration.clone();
    let run_config = config.clone();
    let run_names = names.clone();
    let handler = move |ctx, input: AgentInput| {
        let config = run_config.clone();
        let names = run_names.clone();
        async move { crate::orchestration::run_with_names(ctx, input, config, names).await }
    };
    let session_handler = move |ctx, input: SessionInput| {
        let config = config.clone();
        let names = names.clone();
        async move { crate::session::run_session(ctx, input, config, names).await }
    };
    let builder = OrchestrationRegistry::builder();
    if let Some(version) = version {
        builder
            .register_versioned_typed(orchestration, version, handler)
            .register_versioned_typed(session_orchestration, version, session_handler)
            .build()
    } else {
        builder
            .register_typed(orchestration, handler)
            .register_typed(session_orchestration, session_handler)
            .build()
    }
}

// Keep this wire type checked at this boundary.
const _: fn(ToolActivityOutput) = |_| {};
