use std::collections::{BTreeMap, BTreeSet};

use crate::retry::RetryPolicy;
use rig::{
    completion::{CompletionRequest, ToolDefinition},
    message::ToolChoice,
    tool::ToolSet,
};
use serde::{Deserialize, Serialize};

use crate::policy::ToolPolicy;

pub(crate) fn request_tool_names(
    request: &CompletionRequest,
) -> (BTreeSet<String>, BTreeSet<String>) {
    let executable = request
        .tools
        .iter()
        .map(|tool| tool.name.to_string())
        .collect();
    let allowed = allowed_names(&executable, request.tool_choice.as_ref());
    (executable, allowed)
}

fn allowed_names(executable: &BTreeSet<String>, choice: Option<&ToolChoice>) -> BTreeSet<String> {
    match choice {
        Some(ToolChoice::None) => BTreeSet::new(),
        Some(ToolChoice::Specific { function_names }) => function_names
            .iter()
            .filter(|name| executable.contains(name.as_str()))
            .map(ToString::to_string)
            .collect(),
        Some(ToolChoice::Auto | ToolChoice::Required) | None => executable.clone(),
    }
}

/// Retry, routing, approval, and replay settings for one Rig tool.
/// Approval pauses execution; it does not make a tool safe to retry.
#[derive(Clone, Debug, Default)]
pub struct ToolOptions {
    pub retry: RetryPolicy,
    pub tag: Option<String>,
    pub requires_approval: bool,
    pub policy: ToolPolicy,
}

impl ToolOptions {
    pub fn retry(mut self, retry: impl Into<RetryPolicy>) -> Self {
        self.retry = retry.into();
        self
    }

    pub fn policy(mut self, policy: ToolPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn replay_safety(mut self, safety: crate::ReplaySafety) -> Self {
        self.policy = self.policy.replay_safety(safety);
        self
    }

    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = Some(tag.into());
        self
    }

    pub fn require_approval(mut self) -> Self {
        self.requires_approval = true;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ToolRoute {
    RigTool,
    Activity {
        activity_name: String,
    },
    #[cfg(feature = "duroxide")]
    SubOrchestration {
        orchestration_name: String,
        version: Option<String>,
    },
    #[cfg(feature = "duroxide")]
    DurableAgent {
        orchestration_name: String,
        version: String,
    },
}

#[derive(Clone, Debug)]
pub struct ToolEntry {
    pub definition: ToolDefinition,
    pub route: ToolRoute,
    pub retry: RetryPolicy,
    pub tag: Option<String>,
    pub requires_approval: bool,
    /// Replay safety, implementation version, and metadata retention. Applied
    /// only under [`InvocationContract::Logical`](crate::InvocationContract).
    pub policy: ToolPolicy,
}

impl ToolEntry {
    pub fn with_policy(mut self, policy: ToolPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Non-Rig routes receive raw arguments, without invocation metadata.
    #[cfg(feature = "duroxide")]
    pub(crate) fn check_route_policy(&self) -> Result<(), String> {
        use crate::ReplaySafety;

        if matches!(self.route, ToolRoute::RigTool) {
            return Ok(());
        }
        let name = &self.definition.name;
        match self.policy.safety() {
            ReplaySafety::ApplicationManaged | ReplaySafety::ReadOnly => Ok(()),
            ReplaySafety::Idempotent => Err(format!(
                "tool `{name}` declares idempotent replay safety, but its route cannot deliver a \
                 logical call key"
            )),
            ReplaySafety::InterruptOnUncertain => Err(format!(
                "tool `{name}` declares interrupt-on-uncertain replay safety, but its route cannot \
                 use an invocation guard"
            )),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ToolCatalog(pub BTreeMap<String, ToolEntry>);

impl ToolCatalog {
    pub fn insert(&mut self, entry: ToolEntry) -> Option<ToolEntry> {
        self.0.insert(entry.definition.name.to_string(), entry)
    }
    pub fn get(&self, name: &str) -> Option<&ToolEntry> {
        self.0.get(name)
    }
    pub fn definitions(&self) -> Vec<ToolDefinition> {
        self.0.values().map(|e| e.definition.clone()).collect()
    }
    /// Replay policies by tool name, for the worker-side executor.
    pub fn policies(&self) -> Vec<(String, ToolPolicy)> {
        self.0
            .iter()
            .map(|(name, entry)| (name.clone(), entry.policy.clone()))
            .collect()
    }
    pub fn executable_names(&self) -> BTreeSet<String> {
        self.0.keys().cloned().collect()
    }
    pub fn allowed_names(&self, choice: Option<&rig::message::ToolChoice>) -> BTreeSet<String> {
        allowed_names(&self.executable_names(), choice)
    }
}

pub async fn catalog_from_toolset(toolset: &ToolSet, retry: impl Into<RetryPolicy>) -> ToolCatalog {
    let retry = retry.into();
    ToolCatalog(
        toolset
            .tool_definitions()
            .into_iter()
            .map(|definition| {
                let name = definition.name.to_string();
                (
                    name,
                    ToolEntry {
                        definition,
                        route: ToolRoute::RigTool,
                        retry: retry.clone(),
                        tag: None,
                        requires_approval: false,
                        policy: ToolPolicy::default(),
                    },
                )
            })
            .collect(),
    )
}

pub fn activity_tool(
    definition: ToolDefinition,
    activity_name: impl Into<String>,
    retry: impl Into<RetryPolicy>,
) -> ToolEntry {
    ToolEntry {
        definition,
        route: ToolRoute::Activity {
            activity_name: activity_name.into(),
        },
        retry: retry.into(),
        tag: None,
        requires_approval: false,
        policy: ToolPolicy::default(),
    }
}

/// Exposes a Duroxide child orchestration as a model tool.
///
/// The child receives the model arguments as JSON and owns any activity retry
/// policy needed by its work. Duroxide 0.1.30 does not support parent-side
/// retries, timeouts, or worker tags for child orchestration calls.
#[cfg(feature = "duroxide")]
pub fn sub_orchestration_tool(
    definition: ToolDefinition,
    orchestration_name: impl Into<String>,
    version: Option<String>,
) -> ToolEntry {
    ToolEntry {
        definition,
        route: ToolRoute::SubOrchestration {
            orchestration_name: orchestration_name.into(),
            version,
        },
        retry: RetryPolicy::new(1),
        tag: None,
        requires_approval: false,
        policy: ToolPolicy::default(),
    }
}

#[cfg(feature = "duroxide")]
pub(crate) fn durable_agent_tool(
    definition: ToolDefinition,
    orchestration_name: String,
    version: String,
) -> ToolEntry {
    ToolEntry {
        definition,
        route: ToolRoute::DurableAgent {
            orchestration_name,
            version,
        },
        retry: RetryPolicy::new(1),
        tag: None,
        requires_approval: false,
        policy: ToolPolicy::default(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use rig::message::ToolChoice;

    fn entry(name: &str) -> ToolEntry {
        activity_tool(
            ToolDefinition {
                name: rig::message::ToolName::new(name).unwrap(),
                description: String::new(),
                parameters: serde_json::json!({}),
            },
            format!("{name}Activity"),
            RetryPolicy::new(1),
        )
    }

    #[test]
    fn catalog_is_sorted_and_tool_choice_is_fail_closed() {
        let mut catalog = ToolCatalog::default();
        catalog.insert(entry("zeta"));
        catalog.insert(entry("alpha"));
        assert_eq!(
            catalog
                .definitions()
                .into_iter()
                .map(|d| d.name)
                .collect::<Vec<_>>(),
            ["alpha", "zeta"]
        );
        assert!(catalog.allowed_names(Some(&ToolChoice::None)).is_empty());
        assert_eq!(
            catalog.allowed_names(Some(&ToolChoice::Specific {
                function_names: vec![
                    rig::message::ToolName::new("missing").unwrap(),
                    rig::message::ToolName::new("zeta").unwrap()
                ]
            })),
            BTreeSet::from(["zeta".into()])
        );
    }
}
