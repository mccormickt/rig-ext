//! Compose Rig tools with model-written JavaScript in a bounded sandbox.
//!
//! A model writes an async JavaScript function body. The script calls
//! approved tools through `tools["name"](args)`, combines their results, and
//! emits selected output with `text()`. Intermediate tool results stay out of
//! the model transcript unless the script emits them; the host keeps a record
//! of every call for policy and diagnosis.
//!
//! The crate owns execution, discovery, and bounded results. The host owns
//! tool authority through a [`HostDispatcher`]. Tools own their external I/O.
//!
//! ```no_run
//! # #[cfg(feature = "quickjs")]
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use std::sync::Arc;
//! use rig_codemode::{Catalog, CatalogEntry, CodeMode, DynamicToolDispatcher, ExecutionRequest};
//! use rig_core::tool::{DynamicTool, ToolOutput};
//!
//! let echo = DynamicTool::new("echo", "Echo arguments", serde_json::json!({"type": "object"}),
//!     |args| Box::pin(async move { Ok(ToolOutput::json(args)) }));
//! let dispatcher = DynamicToolDispatcher::new([echo.clone()])?;
//! let catalog = Catalog::new([CatalogEntry::from_definition(&echo.definition())])?;
//! let codemode = CodeMode::builder(catalog, Arc::new(dispatcher)).build()?;
//!
//! let report = codemode
//!     .execute(ExecutionRequest::new(r#"text(await tools["echo"]({ a: 1 }));"#))
//!     .await?;
//! assert_eq!(report.output.text, "{\"a\":1}\n");
//! # Ok(()) }
//! ```
//!
//! # Features
//!
//! - `quickjs`: the native QuickJS backend. Without a backend feature the
//!   crate builds catalog and contract code but [`CodeModeBuilder::build`]
//!   fails with [`BuildError::NoBackend`].
//! - `mcp`: catalog entries and output-schema validation for `rig-rmcp` tools.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod analysis;
// Bridge and runtime code is only reachable through a compiled backend.
#[cfg_attr(not(feature = "quickjs"), allow(dead_code))]
mod bridge;
pub mod catalog;
pub mod dispatch;
pub mod limits;
#[cfg(feature = "mcp")]
pub mod mcp;
pub mod policy;
pub mod report;
#[cfg_attr(not(feature = "quickjs"), allow(dead_code))]
mod runtime;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use rig_core::tool::{DynamicTool, ToolContext, ToolExecutionError, ToolOutput};

pub use analysis::{ScriptAnalysis, analyze};
pub use catalog::{Catalog, CatalogEntry, CatalogError, Presentation};
pub use dispatch::{
    CallPolicy, DispatchFuture, DispatchOutcome, DynamicToolDispatcher, HostDispatcher,
    InputSchemaError, Invocation, ResultPolicy,
};
pub use limits::{LimitOverrides, Limits, LimitsError};
pub use policy::{GrantedTools, ScriptGrant, ScriptPolicy, ScriptPolicyFuture, ScriptReview};
pub use report::{
    CallRecord, CallStatus, ExecutionReport, ExecutionStatus, ScriptDelivery, ScriptDiagnostic,
    ScriptOutput,
};
pub use runtime::SpawnError;

/// Default name of the outer tool and the name scripts cannot call.
pub const DEFAULT_TOOL_NAME: &str = "codemode";

static NEXT_PARENT: AtomicU64 = AtomicU64::new(1);

/// Compiled script backends. Enable exactly one feature, or select one here
/// when several are compiled; the builder never picks by feature precedence.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// Native QuickJS on a dedicated thread (`quickjs` feature).
    #[cfg(feature = "quickjs")]
    QuickJs,
}

impl Backend {
    const COMPILED: &'static [Backend] = &[
        #[cfg(feature = "quickjs")]
        Backend::QuickJs,
    ];

    #[cfg_attr(not(feature = "quickjs"), allow(unused_variables))]
    fn spawn(
        self,
        config: bridge::WorkerConfig,
        channels: bridge::WorkerChannels,
    ) -> Result<std::thread::JoinHandle<()>, SpawnError> {
        match self {
            #[cfg(feature = "quickjs")]
            Backend::QuickJs => runtime::quickjs::spawn(config, channels),
        }
    }
}

/// Configuration errors from [`CodeModeBuilder::build`].
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    /// No backend feature is compiled.
    #[error("no script backend is compiled; enable a backend feature such as `quickjs`")]
    NoBackend,
    /// Several backends are compiled and none was selected.
    #[error("several script backends are compiled; select one with `CodeModeBuilder::backend`")]
    AmbiguousBackend,
    /// A catalog entry has the outer tool's name, which would let a script
    /// start another runtime.
    #[error(
        "catalog entry {0:?} has the code-mode tool's own name; recursive code mode is not allowed"
    )]
    RecursiveTool(String),
    /// The host limits are invalid.
    #[error(transparent)]
    Limits(#[from] LimitsError),
}

/// Why a request was rejected before any script ran.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    /// The source exceeds the source byte limit.
    #[error("script source is {size} bytes; the limit is {limit} bytes")]
    SourceTooLarge {
        /// Source size in bytes.
        size: usize,
        /// Configured limit.
        limit: usize,
    },
    /// Per-request limits were invalid or above the host ceiling.
    #[error(transparent)]
    Limits(#[from] LimitsError),
    /// The [`ScriptPolicy`] refused the script.
    #[error("the script policy refused the script: {0}")]
    Refused(ToolExecutionError),
    /// The worker could not be started.
    #[error(transparent)]
    Spawn(#[from] SpawnError),
}

impl From<ExecutionError> for ToolExecutionError {
    fn from(error: ExecutionError) -> Self {
        match error {
            ExecutionError::SourceTooLarge { .. } | ExecutionError::Limits(_) => {
                ToolExecutionError::invalid_args(error.to_string())
            }
            ExecutionError::Refused(error) => error,
            ExecutionError::Spawn(_) => ToolExecutionError::other(error.to_string())
                .with_model_feedback("the script runtime could not be started"),
        }
    }
}

/// One script to run.
#[derive(Debug, Clone)]
pub struct ExecutionRequest {
    /// JavaScript async function body.
    pub code: String,
    /// Identity of the outer call, passed to every child invocation. Defaults
    /// to a process-unique generated id.
    pub parent_call_id: Option<String>,
    /// Inbound context for child calls. Each dispatch receives
    /// [`ToolContext::for_dispatch`] of this value.
    pub context: ToolContext,
    /// Lower limits for this execution only.
    pub limits: LimitOverrides,
}

impl ExecutionRequest {
    /// A request with default context and host limits.
    pub fn new(code: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            parent_call_id: None,
            context: ToolContext::new(),
            limits: LimitOverrides::default(),
        }
    }

    /// Set the parent call id.
    pub fn with_parent_call_id(mut self, id: impl Into<String>) -> Self {
        self.parent_call_id = Some(id.into());
        self
    }

    /// Set the inbound context.
    pub fn with_context(mut self, context: ToolContext) -> Self {
        self.context = context;
        self
    }

    /// Lower limits for this execution.
    pub fn with_limits(mut self, limits: LimitOverrides) -> Self {
        self.limits = limits;
        self
    }
}

/// Builder for [`CodeMode`].
pub struct CodeModeBuilder {
    catalog: Catalog,
    dispatcher: Arc<dyn HostDispatcher>,
    script_policy: Option<Arc<dyn ScriptPolicy>>,
    limits: Limits,
    backend: Option<Backend>,
    tool_name: String,
    declaration_bytes: usize,
}

impl std::fmt::Debug for CodeModeBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodeModeBuilder")
            .field("catalog_len", &self.catalog.len())
            .field("script_policy", &self.script_policy.is_some())
            .field("limits", &self.limits)
            .field("backend", &self.backend)
            .field("tool_name", &self.tool_name)
            .finish_non_exhaustive()
    }
}

impl CodeModeBuilder {
    /// Host limits; see [`Limits`] for defaults.
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// Review every script before it runs; see [`ScriptPolicy`]. Without a
    /// policy every script may call the whole catalog with the request's
    /// context.
    pub fn script_policy<P: ScriptPolicy>(mut self, policy: P) -> Self {
        self.script_policy = Some(Arc::new(policy));
        self
    }

    /// Select the backend when several are compiled.
    pub fn backend(mut self, backend: Backend) -> Self {
        self.backend = Some(backend);
        self
    }

    /// Name of the outer tool. Defaults to [`DEFAULT_TOOL_NAME`].
    pub fn tool_name(mut self, name: impl Into<String>) -> Self {
        self.tool_name = name.into();
        self
    }

    /// Byte budget for inline TypeScript declarations in the tool
    /// description. Defaults to 8 KiB.
    pub fn declaration_bytes(mut self, bytes: usize) -> Self {
        self.declaration_bytes = bytes;
        self
    }

    /// Validate and build.
    pub fn build(self) -> Result<CodeMode, BuildError> {
        self.limits.validate()?;
        if self.catalog.contains(&self.tool_name) {
            return Err(BuildError::RecursiveTool(self.tool_name));
        }
        let backend = match (self.backend, Backend::COMPILED) {
            (Some(backend), _) => backend,
            (None, []) => return Err(BuildError::NoBackend),
            (None, [only]) => *only,
            (None, _) => return Err(BuildError::AmbiguousBackend),
        };
        Ok(CodeMode {
            inner: Arc::new(Inner {
                catalog: Arc::new(self.catalog),
                dispatcher: self.dispatcher,
                script_policy: self.script_policy,
                limits: self.limits,
                backend,
                tool_name: self.tool_name,
                declaration_bytes: self.declaration_bytes,
            }),
        })
    }
}

struct Inner {
    catalog: Arc<Catalog>,
    dispatcher: Arc<dyn HostDispatcher>,
    script_policy: Option<Arc<dyn ScriptPolicy>>,
    limits: Limits,
    backend: Backend,
    tool_name: String,
    declaration_bytes: usize,
}

/// A configured script executor. Cheap to clone; every execution gets a
/// fresh guest.
#[derive(Clone)]
pub struct CodeMode {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for CodeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodeMode")
            .field("catalog_len", &self.inner.catalog.len())
            .field("limits", &self.inner.limits)
            .field("backend", &self.inner.backend)
            .field("tool_name", &self.inner.tool_name)
            .finish()
    }
}

impl CodeMode {
    /// Start building an executor over an approved catalog and the host's
    /// dispatcher.
    pub fn builder(catalog: Catalog, dispatcher: Arc<dyn HostDispatcher>) -> CodeModeBuilder {
        CodeModeBuilder {
            catalog,
            dispatcher,
            script_policy: None,
            limits: Limits::default(),
            backend: None,
            tool_name: DEFAULT_TOOL_NAME.to_string(),
            declaration_bytes: 8 * 1024,
        }
    }

    /// The approved catalog.
    pub fn catalog(&self) -> &Catalog {
        &self.inner.catalog
    }

    /// Host limits.
    pub fn limits(&self) -> &Limits {
        &self.inner.limits
    }

    /// Selected backend.
    pub fn backend(&self) -> Backend {
        self.inner.backend
    }

    /// Name of the outer tool.
    pub fn tool_name(&self) -> &str {
        &self.inner.tool_name
    }

    /// Run one script. `Err` means nothing ran: the source was too large, the
    /// overrides were invalid, the script policy refused the script, or the
    /// worker could not start. Every outcome of a script that did run is an
    /// `Ok` report with a status.
    ///
    /// Dropping the returned future cancels the script: the guest is
    /// interrupted, in-flight dispatches are dropped, and queued calls never
    /// start.
    pub async fn execute(
        &self,
        request: ExecutionRequest,
    ) -> Result<ExecutionReport, ExecutionError> {
        let inner = &self.inner;
        if request.code.len() > inner.limits.source_bytes {
            return Err(ExecutionError::SourceTooLarge {
                size: request.code.len(),
                limit: inner.limits.source_bytes,
            });
        }
        let limits = inner.limits.restrict(&request.limits)?;
        let parent_call_id = request.parent_call_id.unwrap_or_else(|| {
            format!(
                "{}-{}",
                inner.tool_name,
                NEXT_PARENT.fetch_add(1, Ordering::Relaxed)
            )
        });
        let grant = match &inner.script_policy {
            None => ScriptGrant::catalog(request.context),
            Some(policy) => {
                let analysis = analyze(&request.code);
                policy
                    .review(ScriptReview {
                        code: &request.code,
                        analysis: &analysis,
                        catalog: &inner.catalog,
                        parent_call_id: &parent_call_id,
                        context: request.context,
                    })
                    .await
                    .map_err(ExecutionError::Refused)?
            }
        };
        let deadline = Instant::now() + limits.wall_time;
        let (worker, host) = bridge::channels(&limits);
        let config = bridge::WorkerConfig {
            code: request.code,
            catalog: inner.catalog.clone(),
            limits,
            deadline,
        };
        let worker = inner.backend.spawn(config, worker)?;
        let report = bridge::run_host(
            inner.dispatcher.clone(),
            limits,
            deadline,
            parent_call_id,
            grant,
            host,
        )
        .await;
        let _ = worker.join();
        Ok(report)
    }

    /// The model-facing description: usage rules plus inline declarations
    /// within the configured byte budget.
    pub fn description(&self) -> String {
        let declarations = self
            .inner
            .catalog
            .render_declarations(self.inner.declaration_bytes);
        format!(
            "Run a JavaScript async function body that calls approved tools and emits \
             selected output. Call tools with `await tools[\"name\"](args)`; each call \
             returns the tool's JSON value, its literal text, or `{{ content: [...] }}` for \
             mixed content, and rejects with a `CodeModeToolError` (fields `kind`, \
             `status`, `tool`) when the tool fails or is denied. \
             `tools[\"name\"].raw(args)` makes a separate call and returns \
             `{{ status, name, content, error? }}` instead of throwing. Use `text(value)` \
             to emit a string or JSON value to the transcript; nothing else is shown. \
             `searchTools(query, {{ limit }})` and `describeTool(name)` discover tools. \
             There is no network, filesystem, module loading, or timer access. The \
             script's return value is also shown.\n\n{declarations}"
        )
    }

    /// Expose the executor as a Rig tool with `{ "code": string }` input.
    /// Completed scripts return text; other statuses return typed errors whose
    /// model output keeps the partial text and call summary.
    pub fn tool(&self) -> DynamicTool {
        let this = self.clone();
        DynamicTool::new_with_context(
            self.inner.tool_name.clone(),
            self.description(),
            serde_json::json!({
                "type": "object",
                "properties": {
                    "code": {
                        "type": "string",
                        "description": "JavaScript async function body"
                    }
                },
                "required": ["code"],
                "additionalProperties": false
            }),
            move |context: &mut ToolContext, arguments: serde_json::Value| {
                let this = this.clone();
                let context = context.clone();
                Box::pin(async move { this.run_tool(context, arguments).await })
            },
        )
    }

    async fn run_tool(
        &self,
        context: ToolContext,
        arguments: serde_json::Value,
    ) -> Result<ToolOutput, ToolExecutionError> {
        let Some(code) = arguments.get("code").and_then(serde_json::Value::as_str) else {
            return Err(ToolExecutionError::invalid_args(
                "expected an object with a string `code` field",
            ));
        };
        let report = self
            .execute(ExecutionRequest::new(code).with_context(context))
            .await?;
        report.into_tool_result()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    struct Never;

    impl HostDispatcher for Never {
        fn dispatch<'a>(&'a self, _: Invocation) -> DispatchFuture<'a> {
            Box::pin(async { ToolExecutionError::refused("never").into_outcome() })
        }
    }

    trait IntoOutcome {
        fn into_outcome(self) -> DispatchOutcome;
    }

    impl IntoOutcome for ToolExecutionError {
        fn into_outcome(self) -> DispatchOutcome {
            DispatchOutcome::from(rig_core::tool::ToolResult::failed(self))
        }
    }

    #[test]
    fn rejects_recursive_codemode_entry() {
        let catalog = Catalog::new([CatalogEntry::new(
            "codemode",
            "run code",
            serde_json::json!({"type": "object"}),
        )])
        .unwrap();
        let error = CodeMode::builder(catalog, Arc::new(Never))
            .build()
            .unwrap_err();
        assert!(matches!(error, BuildError::RecursiveTool(name) if name == "codemode"));
    }

    #[cfg(not(feature = "quickjs"))]
    #[test]
    fn build_without_backend_fails_closed() {
        let error = CodeMode::builder(Catalog::default(), Arc::new(Never))
            .build()
            .unwrap_err();
        assert!(matches!(error, BuildError::NoBackend));
    }
}
