//! Host-owned tool authority. The script runtime asks a [`HostDispatcher`]
//! for every call; the dispatcher validates, authorizes, runs the tool, and
//! applies result policy before anything reaches the script.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rig_core::tool::{DynamicTool, ToolContext, ToolExecutionError, ToolResult, ToolResultContext};

/// One script-initiated tool call as the host sees it.
#[derive(Debug, Clone)]
pub struct Invocation {
    /// Identity of the outer `codemode` call that started the script.
    pub parent_call_id: String,
    /// Zero-based position of this call within the script.
    pub child_ordinal: u32,
    /// Exact requested tool name. No normalization or aliasing was applied.
    pub name: String,
    /// JSON arguments as the script passed them. A policy may rewrite them;
    /// the dispatcher validates whatever the tool receives.
    pub arguments: serde_json::Value,
    /// The dispatch-scoped context: the parent's inbound values, no results.
    pub context: ToolContext,
}

impl Invocation {
    /// Build an invocation. Applications use this for direct (non-script)
    /// calls that must pass the same policy as nested calls.
    pub fn new(
        parent_call_id: impl Into<String>,
        child_ordinal: u32,
        name: impl Into<String>,
        arguments: serde_json::Value,
        context: ToolContext,
    ) -> Self {
        Self {
            parent_call_id: parent_call_id.into(),
            child_ordinal,
            name: name.into(),
            arguments,
            context,
        }
    }
}

/// What a dispatch produced: the policy-filtered result and the metadata the
/// host explicitly allows the runtime to see. Nothing in `metadata` reaches
/// the script unless a projection chooses it.
#[derive(Debug, Clone)]
pub struct DispatchOutcome {
    /// Tool disposition and model-visible output after result policy.
    pub result: ToolResult,
    /// Host-only metadata published by the tool. `ResultPolicy` filters the
    /// result, not this metadata; projections must not expose its values or
    /// validation details to the script or logs.
    pub metadata: ToolResultContext,
}

impl From<ToolResult> for DispatchOutcome {
    fn from(result: ToolResult) -> Self {
        Self {
            result,
            metadata: ToolResultContext::default(),
        }
    }
}

impl From<Result<rig_core::tool::ToolOutput, ToolExecutionError>> for DispatchOutcome {
    fn from(result: Result<rig_core::tool::ToolOutput, ToolExecutionError>) -> Self {
        Self::from(tool_result(result))
    }
}

/// Convert an ordinary tool `Result` into a [`ToolResult`], keeping refusals.
pub fn tool_result(result: Result<rig_core::tool::ToolOutput, ToolExecutionError>) -> ToolResult {
    match result {
        Ok(output) => ToolResult::success(output),
        Err(error) => ToolResult::failed(error),
    }
}

/// The future a dispatcher returns.
pub type DispatchFuture<'a> = Pin<Box<dyn Future<Output = DispatchOutcome> + Send + 'a>>;

/// Executes script-initiated tool calls with the host's authority.
///
/// Dropping the returned future is the cancellation signal: the runtime drops
/// in-flight dispatches when the script ends. A dropped future does not prove
/// an external effect stopped; implementations that need stronger guarantees
/// must handle cancellation inside the tool.
pub trait HostDispatcher: Send + Sync + 'static {
    /// Run one call. Return a failed or refused [`ToolResult`] for denied or
    /// unknown calls instead of panicking or returning an application error.
    fn dispatch<'a>(&'a self, invocation: Invocation) -> DispatchFuture<'a>;
}

impl<T: HostDispatcher + ?Sized> HostDispatcher for Arc<T> {
    fn dispatch<'a>(&'a self, invocation: Invocation) -> DispatchFuture<'a> {
        (**self).dispatch(invocation)
    }
}

/// Per-call authorization and argument policy. It may rewrite
/// `invocation.arguments`; the dispatcher validates the rewritten value. Return a
/// refusal with [`ToolExecutionError::refused`] to deny the call.
pub type CallPolicy = Arc<dyn Fn(&mut Invocation) -> Result<(), ToolExecutionError> + Send + Sync>;

/// Result policy applied after the tool ran, such as redaction. The returned
/// result is the only thing the script can observe, in both normal and `.raw`
/// mode.
pub type ResultPolicy = Arc<dyn Fn(&Invocation, ToolResult) -> ToolResult + Send + Sync>;

/// A dispatcher over a fixed set of [`DynamicTool`]s with optional call and
/// result policies. Suitable for explicit host mode with unmodified Rig; it
/// does not inherit any agent runner's hooks.
#[derive(Clone)]
pub struct DynamicToolDispatcher {
    tools: Arc<HashMap<String, (DynamicTool, jsonschema::Validator)>>,
    call_policy: Option<CallPolicy>,
    result_policy: Option<ResultPolicy>,
}

/// A registered tool has an invalid input schema.
#[derive(Debug, thiserror::Error)]
#[error("input schema of tool {name:?} is invalid: {reason}")]
pub struct InputSchemaError {
    /// Tool whose schema could not be compiled.
    pub name: String,
    /// Schema compiler diagnostic, for the host only.
    pub reason: String,
}

impl std::fmt::Debug for DynamicToolDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut names: Vec<&str> = self.tools.keys().map(String::as_str).collect();
        names.sort_unstable();
        f.debug_struct("DynamicToolDispatcher")
            .field("tools", &names)
            .field("call_policy", &self.call_policy.is_some())
            .field("result_policy", &self.result_policy.is_some())
            .finish()
    }
}

impl DynamicToolDispatcher {
    /// Build a dispatcher. A later tool with the same name replaces an earlier
    /// one.
    pub fn new(tools: impl IntoIterator<Item = DynamicTool>) -> Result<Self, InputSchemaError> {
        let tools =
            tools
                .into_iter()
                .map(|tool| {
                    let validator = jsonschema::validator_for(&tool.definition().parameters)
                        .map_err(|error| InputSchemaError {
                            name: tool.name().to_string(),
                            reason: error.to_string(),
                        })?;
                    Ok((tool.name().to_string(), (tool, validator)))
                })
                .collect::<Result<_, InputSchemaError>>()?;
        Ok(Self {
            tools: Arc::new(tools),
            call_policy: None,
            result_policy: None,
        })
    }

    /// Install a call policy; see [`CallPolicy`].
    pub fn with_call_policy<F>(mut self, policy: F) -> Self
    where
        F: Fn(&mut Invocation) -> Result<(), ToolExecutionError> + Send + Sync + 'static,
    {
        self.call_policy = Some(Arc::new(policy));
        self
    }

    /// Install a result policy; see [`ResultPolicy`].
    pub fn with_result_policy<F>(mut self, policy: F) -> Self
    where
        F: Fn(&Invocation, ToolResult) -> ToolResult + Send + Sync + 'static,
    {
        self.result_policy = Some(Arc::new(policy));
        self
    }

    /// Tool definitions, sorted by name, for building a catalog.
    pub fn definitions(&self) -> Vec<rig_core::completion::ToolDefinition> {
        let mut definitions: Vec<_> = self
            .tools
            .values()
            .map(|(tool, _)| tool.definition())
            .collect();
        definitions.sort_by(|a, b| a.name.cmp(&b.name));
        definitions
    }

    /// Whether a tool with this exact name is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.tools.contains_key(name)
    }
}

impl HostDispatcher for DynamicToolDispatcher {
    fn dispatch<'a>(&'a self, mut invocation: Invocation) -> DispatchFuture<'a> {
        Box::pin(async move {
            let Some((tool, validator)) = self.tools.get(&invocation.name) else {
                return DispatchOutcome::from(ToolResult::failed(ToolExecutionError::not_found(
                    format!("tool {:?} is not available", invocation.name),
                )));
            };
            if let Some(policy) = &self.call_policy
                && let Err(error) = policy(&mut invocation)
            {
                return DispatchOutcome::from(ToolResult::failed(error));
            }
            if !validator.is_valid(&invocation.arguments) {
                return ToolResult::failed(ToolExecutionError::invalid_args(
                    "tool arguments do not match the declared input schema",
                ))
                .into();
            }
            let mut context = invocation.context.for_dispatch();
            let result = tool_result(
                tool.execute_with(&mut context, invocation.arguments.clone())
                    .await,
            );
            let result = match &self.result_policy {
                Some(policy) => policy(&invocation, result),
                None => result,
            };
            DispatchOutcome {
                result,
                metadata: context.result_context(),
            }
        })
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use rig_core::tool::ToolOutput;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn validates_effective_arguments_before_callback() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let tool = DynamicTool::new(
            "count",
            "",
            json!({
                "type": "object", "properties": {"n": {"type": "integer", "minimum": 0}},
                "required": ["n"]
            }),
            move |args| {
                counter.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { Ok(ToolOutput::json(args)) })
            },
        );
        let dispatcher = DynamicToolDispatcher::new([tool])
            .unwrap()
            .with_call_policy(|call| {
                if call.arguments.get("rewrite").is_some() {
                    call.arguments = json!({"n": -1});
                }
                Ok(())
            });
        for args in [json!({"n": "bad"}), json!({"n": 3, "rewrite": true})] {
            let result = dispatcher
                .dispatch(Invocation::new("p", 0, "count", args, ToolContext::new()))
                .await;
            assert_eq!(
                result.result.error().unwrap().kind(),
                rig_core::tool::ToolErrorKind::InvalidArgs
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
        let result = dispatcher
            .dispatch(Invocation::new(
                "p",
                0,
                "count",
                json!({"n": 3}),
                ToolContext::new(),
            ))
            .await;
        assert!(result.result.is_success());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn invalid_schema_is_a_construction_error() {
        let tool = DynamicTool::new("bad", "", json!({"type": "not-a-type"}), |_| {
            Box::pin(async { Ok(ToolOutput::text("unused")) })
        });
        assert_eq!(DynamicToolDispatcher::new([tool]).unwrap_err().name, "bad");
    }
}
