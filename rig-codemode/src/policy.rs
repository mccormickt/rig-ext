//! Script-level authorization. A [`ScriptPolicy`] reviews the whole script
//! once, before it runs, and returns a [`ScriptGrant`]: the tools the script
//! may call and the context every child call receives. The runtime refuses
//! calls outside the grant, so the grant holds even when the script computes
//! tool names at run time.
//!
//! # Grant an explicit allowlist
//!
//! Use host permissions to select names. Do not treat [`crate::analyze`] as
//! a complete list of possible calls. A fixed grant also applies to computed
//! names and `.raw()` calls.
//!
//! ```
//! # #[cfg(feature = "quickjs")]
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use std::sync::Arc;
//! use rig_codemode::{
//!     Catalog, CatalogEntry, CodeMode, DynamicToolDispatcher, ExecutionRequest,
//!     CallStatus, ScriptGrant, ScriptReview,
//! };
//! use rig_core::tool::{DynamicTool, ToolOutput};
//!
//! let tools = ["lookup", "delete"].map(|name| DynamicTool::new(
//!     name, "Example operation", serde_json::json!({"type": "object"}),
//!     |args| Box::pin(async move { Ok(ToolOutput::json(args)) }),
//! ));
//! let dispatcher = DynamicToolDispatcher::new(tools)?;
//! let catalog = Catalog::new(
//!     dispatcher.definitions().iter().map(CatalogEntry::from_definition),
//! )?;
//! let codemode = CodeMode::builder(catalog, Arc::new(dispatcher))
//!     .script_policy(|review: ScriptReview<'_>| {
//!         Ok(ScriptGrant::only(["lookup"], review.context))
//!     })
//!     .build()?;
//! let report = codemode.execute(ExecutionRequest::new(r#"
//!     const operation = "delete";
//!     try { await tools[operation]({}); }
//!     catch (error) { text(error.status); }
//! "#)).await?;
//! assert!(report.is_completed());
//! assert_eq!(report.output.text, "denied\n");
//! assert_eq!(report.calls.first().map(|call| call.status), Some(CallStatus::Refused));
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "quickjs"))]
//! # fn main() {}
//! ```
//!
//! Return [`ToolExecutionError::refused`] to reject the entire script before
//! execution. To allow only names found by the scanner, return
//! [`ScriptReview::grant_referenced`]; valid calls missed by the scanner will
//! be refused. Approval waits are outside [`crate::Limits::wall_time`]. Bound
//! asynchronous review separately, for example with `tokio::time::timeout`.

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;

use rig_core::tool::{ToolContext, ToolExecutionError};

use crate::analysis::ScriptAnalysis;
use crate::catalog::Catalog;

/// Everything a policy sees about one script before it runs.
#[derive(Debug)]
pub struct ScriptReview<'a> {
    /// The script source.
    pub code: &'a str,
    /// What the source names statically; see [`crate::analyze`].
    pub analysis: &'a ScriptAnalysis,
    /// The approved catalog.
    pub catalog: &'a Catalog,
    /// Identity of the outer call.
    pub parent_call_id: &'a str,
    /// The inbound context from the request. The policy returns it, with any
    /// values it attaches, inside the grant.
    pub context: ToolContext,
}

impl ScriptReview<'_> {
    /// Grant the whole catalog with the review's context.
    pub fn grant_catalog(self) -> ScriptGrant {
        ScriptGrant::catalog(self.context)
    }

    /// Grant exactly the tools the script names statically. A later call to
    /// any other tool, for example through `tools[name]`, is refused.
    pub fn grant_referenced(self) -> ScriptGrant {
        ScriptGrant::only(self.analysis.tools.clone(), self.context)
    }
}

/// Which tools a script may call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantedTools {
    /// Every catalog entry.
    Catalog,
    /// Exactly these names.
    Only(BTreeSet<String>),
}

impl GrantedTools {
    /// True when a call to `name` is allowed.
    pub fn allows(&self, name: &str) -> bool {
        match self {
            GrantedTools::Catalog => true,
            GrantedTools::Only(names) => names.contains(name),
        }
    }
}

/// The policy's decision for one script.
#[derive(Debug, Clone)]
pub struct ScriptGrant {
    /// The callable tools.
    pub tools: GrantedTools,
    /// The inbound context for every child call. Each dispatch receives
    /// [`ToolContext::for_dispatch`] of this value.
    pub context: ToolContext,
}

impl ScriptGrant {
    /// Allow every catalog tool.
    pub fn catalog(context: ToolContext) -> Self {
        Self {
            tools: GrantedTools::Catalog,
            context,
        }
    }

    /// Allow exactly `names`.
    pub fn only<I, S>(names: I, context: ToolContext) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            tools: GrantedTools::Only(names.into_iter().map(Into::into).collect()),
            context,
        }
    }
}

/// The future a policy returns.
pub type ScriptPolicyFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ScriptGrant, ToolExecutionError>> + Send + 'a>>;

/// Reviews a script before it runs.
///
/// Return `Err` to refuse the script; nothing runs and the error is the tool
/// result. Use [`ToolExecutionError::refused`] for a policy refusal so the
/// model learns that the script, not the runtime, was rejected.
///
/// Synchronous closures `Fn(ScriptReview<'_>) -> Result<ScriptGrant,
/// ToolExecutionError>` implement this trait. Implement it directly when the
/// review needs to await an approval. Review runs outside the execution
/// deadline; the host must bound approval waits separately if needed.
pub trait ScriptPolicy: Send + Sync + 'static {
    /// Review one script.
    fn review<'a>(&'a self, review: ScriptReview<'a>) -> ScriptPolicyFuture<'a>;
}

impl<F> ScriptPolicy for F
where
    F: for<'r> Fn(ScriptReview<'r>) -> Result<ScriptGrant, ToolExecutionError>
        + Send
        + Sync
        + 'static,
{
    fn review<'a>(&'a self, review: ScriptReview<'a>) -> ScriptPolicyFuture<'a> {
        let decision = self(review);
        Box::pin(async move { decision })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn only_grant_allows_listed_names() {
        let grant = ScriptGrant::only(["a", "b"], ToolContext::new());
        assert!(grant.tools.allows("a"));
        assert!(!grant.tools.allows("c"));
        assert!(GrantedTools::Catalog.allows("anything"));
    }

    #[tokio::test]
    async fn closure_policy_sees_analysis_and_returns_grant() {
        let catalog = Catalog::default();
        let analysis = crate::analyze("await tools.x({});");
        let policy = |review: ScriptReview<'_>| {
            assert_eq!(review.parent_call_id, "p-1");
            Ok(review.grant_referenced())
        };
        let grant = policy
            .review(ScriptReview {
                code: "await tools.x({});",
                analysis: &analysis,
                catalog: &catalog,
                parent_call_id: "p-1",
                context: ToolContext::new(),
            })
            .await
            .unwrap();
        assert_eq!(grant.tools, GrantedTools::Only(["x".to_owned()].into()));
    }
}
