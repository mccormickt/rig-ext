//! Optional `rig-rmcp` integration: catalog entries that keep MCP output
//! schemas, and a dispatcher wrapper that validates the `structuredContent`
//! an MCP tool answered with against its declared schema.
//!
//! Scripts never see MCP types. Normal calls follow the shared projection in
//! this crate: `rig-rmcp` already turns `isError` results into failed calls
//! whose model output keeps the permitted content, so a normal call rejects
//! and `.raw` mode returns the sanitized envelope with that content.

use std::collections::HashMap;

use rig_core::tool::{ToolExecutionError, ToolResult};
use rig_rmcp::McpStructuredContent;
use rig_rmcp::rmcp::model::Tool as McpToolDefinition;

use crate::catalog::{Catalog, CatalogEntry};
use crate::dispatch::{DispatchFuture, DispatchOutcome, HostDispatcher, Invocation};

impl CatalogEntry {
    /// An entry from an MCP tool definition, keeping its output schema. Take
    /// the definition from [`rig_rmcp::McpTool::definition`] before converting
    /// the tool to a `DynamicTool`; the Rig definition has no output schema.
    pub fn from_mcp_definition(definition: &McpToolDefinition) -> Self {
        let entry = CatalogEntry::new(
            definition.name.to_string(),
            definition.description.as_deref().unwrap_or(""),
            definition.schema_as_json_value(),
        );
        match &definition.output_schema {
            Some(schema) => {
                entry.with_output_schema(serde_json::Value::Object(schema.as_ref().clone()))
            }
            None => entry,
        }
    }
}

/// A declared output schema could not be compiled.
#[derive(Debug, thiserror::Error)]
#[error("output schema of tool {name:?} is invalid: {reason}")]
pub struct SchemaError {
    /// Tool whose schema failed to compile.
    pub name: String,
    /// Compiler message.
    pub reason: String,
}

/// Wraps a dispatcher and checks successful MCP results against the output
/// schema declared in the catalog. A result whose `structuredContent` is
/// missing or does not match becomes a failed, non-retryable call; the tool
/// is never called again to obtain another representation.
pub struct OutputSchemaValidator<D> {
    inner: D,
    validators: HashMap<String, jsonschema::Validator>,
}

impl<D> std::fmt::Debug for OutputSchemaValidator<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut names: Vec<&str> = self.validators.keys().map(String::as_str).collect();
        names.sort_unstable();
        f.debug_struct("OutputSchemaValidator")
            .field("validated_tools", &names)
            .finish_non_exhaustive()
    }
}

impl<D: HostDispatcher> OutputSchemaValidator<D> {
    /// Compile every output schema in `catalog`. Entries without an output
    /// schema pass through unchecked.
    pub fn new(inner: D, catalog: &Catalog) -> Result<Self, SchemaError> {
        let mut validators = HashMap::new();
        for entry in catalog.entries() {
            let Some(schema) = entry.output_schema() else {
                continue;
            };
            let validator = jsonschema::validator_for(schema).map_err(|error| SchemaError {
                name: entry.name().to_string(),
                reason: error.to_string(),
            })?;
            validators.insert(entry.name().to_string(), validator);
        }
        Ok(Self { inner, validators })
    }

    /// The wrapped dispatcher.
    pub fn inner(&self) -> &D {
        &self.inner
    }

    fn check(&self, name: &str, outcome: DispatchOutcome) -> DispatchOutcome {
        let Some(validator) = self.validators.get(name) else {
            return outcome;
        };
        if !outcome.result.is_success() {
            return outcome;
        }
        let problem = match outcome.metadata.get::<McpStructuredContent>() {
            Ok(Some(McpStructuredContent(value))) if validator.is_valid(&value) => return outcome,
            Ok(Some(_)) => "structuredContent does not match",
            Ok(None) => "the result has no structuredContent",
            Err(_) => "the structuredContent could not be read",
        };
        let message = format!(
            "tool {name:?} returned a result that does not match its declared output schema ({problem})"
        );
        tracing::warn!(tool = %name, problem = %problem, "MCP output schema mismatch");
        DispatchOutcome {
            result: ToolResult::failed(
                ToolExecutionError::other(message.clone())
                    .with_retryable(false)
                    .with_model_feedback(message),
            ),
            metadata: outcome.metadata,
        }
    }
}

impl<D: HostDispatcher> HostDispatcher for OutputSchemaValidator<D> {
    fn dispatch<'a>(&'a self, invocation: Invocation) -> DispatchFuture<'a> {
        Box::pin(async move {
            let name = invocation.name.clone();
            let outcome = self.inner.dispatch(invocation).await;
            self.check(&name, outcome)
        })
    }
}
