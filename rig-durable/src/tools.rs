use std::collections::{BTreeMap, BTreeSet};

use duroxide::RetryPolicy;
use rig::{completion::ToolDefinition, tool::ToolSet};

#[derive(Clone, Debug)]
pub enum ToolRoute {
    RigTool,
    Activity {
        activity_name: String,
    },
    SubOrchestration {
        orchestration_name: String,
        version: Option<String>,
    },
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
}

#[derive(Clone, Debug, Default)]
pub struct ToolCatalog(pub BTreeMap<String, ToolEntry>);

impl ToolCatalog {
    pub fn insert(&mut self, entry: ToolEntry) -> Option<ToolEntry> {
        self.0.insert(entry.definition.name.clone(), entry)
    }
    pub fn get(&self, name: &str) -> Option<&ToolEntry> {
        self.0.get(name)
    }
    pub fn definitions(&self) -> Vec<ToolDefinition> {
        self.0.values().map(|e| e.definition.clone()).collect()
    }
    pub fn executable_names(&self) -> BTreeSet<String> {
        self.0.keys().cloned().collect()
    }
    pub fn allowed_names(&self, choice: Option<&rig::message::ToolChoice>) -> BTreeSet<String> {
        use rig::message::ToolChoice;
        match choice {
            Some(ToolChoice::None) => BTreeSet::new(),
            Some(ToolChoice::Specific { function_names }) => function_names
                .iter()
                .filter(|n| self.0.contains_key(*n))
                .cloned()
                .collect(),
            _ => self.executable_names(),
        }
    }
}

pub async fn catalog_from_toolset(toolset: &ToolSet, retry: RetryPolicy) -> ToolCatalog {
    ToolCatalog(
        toolset
            .tool_definitions()
            .into_iter()
            .map(|definition| {
                let name = definition.name.clone();
                (
                    name,
                    ToolEntry {
                        definition,
                        route: ToolRoute::RigTool,
                        retry: retry.clone(),
                        tag: None,
                        requires_approval: false,
                    },
                )
            })
            .collect(),
    )
}

pub fn activity_tool(
    definition: ToolDefinition,
    activity_name: impl Into<String>,
    retry: RetryPolicy,
) -> ToolEntry {
    ToolEntry {
        definition,
        route: ToolRoute::Activity {
            activity_name: activity_name.into(),
        },
        retry,
        tag: None,
        requires_approval: false,
    }
}

/// Exposes a Duroxide child orchestration as a model tool.
///
/// The child receives the model arguments as JSON and owns any activity retry
/// policy needed by its work. Duroxide 0.1.30 does not support parent-side
/// retries, timeouts, or worker tags for child orchestration calls.
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
    }
}

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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rig::message::ToolChoice;

    fn entry(name: &str) -> ToolEntry {
        activity_tool(
            ToolDefinition {
                name: name.into(),
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
                function_names: vec!["missing".into(), "zeta".into()]
            })),
            BTreeSet::from(["zeta".into()])
        );
    }
}
