use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use duroxide::{
    Client, ClientError, OrchestrationStatus, RetryPolicy,
    providers::Provider,
    runtime::{
        Runtime, RuntimeOptions,
        registry::{ActivityRegistry, OrchestrationRegistry},
    },
};
use rig::{
    DynModel,
    agent::PromptResponse,
    completion::{Message, ToolDefinition},
    operation::Completion,
    tool::{Tool, ToolSet},
};
use semver::Version;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    AgentInput, ApprovalDecision, ApprovalRequest, CheckpointConfig, CompletionMode,
    CompletionSettings, DurableAgentConfig,
    names::RuntimeNames,
    orchestration::{STEERING_QUEUE_NAME, SteeringCommand},
    registry::{activity_registry_with_names, orchestration_registry_with_names},
    tools::{ToolCatalog, ToolEntry, durable_agent_tool},
};

const DEFAULT_VERSION: &str = "1.0.0";
const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Debug, Error)]
pub enum AgentOrchestratorError {
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("invalid agent version `{version}`: {source}")]
    InvalidVersion {
        version: String,
        source: semver::Error,
    },
    #[error("agent `{0}` is not registered")]
    AgentNotFound(String),
    #[error("agent `{name}` has no registered version `{version}`")]
    AgentVersionNotFound { name: String, version: String },
    #[error("duplicate agent registration `{name}` at version `{version}`")]
    DuplicateAgent { name: String, version: String },
    #[error("duplicate tool `{0}`")]
    DuplicateTool(String),
    #[error("agent `{name}` versions must use one approval queue; found `{existing}` and `{new}`")]
    ApprovalQueueMismatch {
        name: String,
        existing: String,
        new: String,
    },
    #[error("durable sub-agent `{0}` requires approval, which parent run handles cannot route")]
    SubAgentApprovalUnsupported(String),
    #[error("registry error: {0}")]
    Registry(String),
    #[error("agent run failed: {0}")]
    Run(String),
    #[error("agent run `{0}` completed before requesting approval")]
    ApprovalNotRequested(String),
    #[error("agent run `{0}` does not exist")]
    RunNotFound(String),
    #[error("agent run `{0}` completed before accepting steering")]
    SteeringNotAccepted(String),
    #[cfg(feature = "sqlite")]
    #[error("failed to open SQLite provider: {0}")]
    Sqlite(String),
}

#[derive(Clone, Debug, Default)]
pub struct ToolOptions {
    pub retry: RetryPolicy,
    pub tag: Option<String>,
    pub requires_approval: bool,
}

impl ToolOptions {
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
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

#[derive(Clone)]
struct AgentMetadata {
    name: String,
    version: Version,
    description: String,
    orchestration: String,
    approval_queue: String,
    checkpoint_target: Option<String>,
}

pub struct AgentDefinition {
    metadata: AgentMetadata,
    activities: ActivityRegistry,
    orchestrations: OrchestrationRegistry,
    children: Vec<AgentDefinition>,
    contains_approval: bool,
}

impl AgentDefinition {
    pub fn name(&self) -> &str {
        &self.metadata.name
    }

    pub fn version(&self) -> &Version {
        &self.metadata.version
    }
}

pub struct DurableAgentBuilder {
    name: String,
    version: Version,
    description: Option<String>,
    model: DynModel<Completion>,
    tools: ToolSet,
    tool_options: HashMap<String, ToolOptions>,
    routed_tools: ToolCatalog,
    children: Vec<AgentDefinition>,
    config: DurableAgentConfig,
}

impl DurableAgentBuilder {
    fn new(name: impl Into<String>, model: DynModel<Completion>) -> Self {
        Self {
            name: name.into(),
            version: Version::parse(DEFAULT_VERSION).expect("default version is valid semver"),
            description: None,
            model,
            tools: ToolSet::default(),
            tool_options: HashMap::new(),
            routed_tools: ToolCatalog::default(),
            children: Vec::new(),
            config: DurableAgentConfig::default(),
        }
    }

    pub fn version(mut self, version: impl Into<String>) -> Result<Self, AgentOrchestratorError> {
        let version = version.into();
        self.version =
            Version::parse(&version).map_err(|source| AgentOrchestratorError::InvalidVersion {
                version: version.clone(),
                source,
            })?;
        Ok(self)
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn preamble(mut self, preamble: impl Into<String>) -> Self {
        self.config.preamble = Some(preamble.into());
        self
    }

    pub fn max_turns(mut self, max_turns: usize) -> Self {
        self.config.max_turns = max_turns;
        self
    }

    pub fn completion(mut self, settings: CompletionSettings) -> Self {
        self.config.completion = settings;
        self
    }

    pub fn completion_mode(mut self, mode: CompletionMode) -> Self {
        self.config.completion_mode = mode;
        self
    }

    pub fn completion_retry(mut self, retry: RetryPolicy) -> Self {
        self.config.completion_retry = retry;
        self
    }

    pub fn checkpoint(mut self, checkpoint: CheckpointConfig) -> Self {
        self.config.checkpoint = checkpoint;
        self
    }

    pub fn approval_queue(mut self, queue: impl Into<String>) -> Self {
        self.config.approval.enabled = true;
        self.config.approval.queue_name = queue.into();
        self
    }

    pub fn tool<T>(self, tool: T) -> Self
    where
        T: Tool + 'static,
    {
        self.tool_with(tool, ToolOptions::default())
    }

    pub fn tool_with<T>(mut self, tool: T, options: ToolOptions) -> Self
    where
        T: Tool + 'static,
    {
        let name = self.tools.add_tool(tool);
        if options.requires_approval {
            self.config.approval.enabled = true;
        }
        self.tool_options.insert(name, options);
        self
    }

    /// Add an advanced activity or workflow-backed tool entry.
    pub fn routed_tool(mut self, entry: ToolEntry) -> Result<Self, AgentOrchestratorError> {
        let name = entry.definition.name.clone();
        if self.tools.contains(&name) || self.routed_tools.get(&name).is_some() {
            return Err(AgentOrchestratorError::DuplicateTool(name));
        }
        if entry.requires_approval {
            self.config.approval.enabled = true;
        }
        self.routed_tools.insert(entry);
        Ok(self)
    }

    pub fn sub_agent(
        mut self,
        tool_name: impl Into<String>,
        child: AgentDefinition,
    ) -> Result<Self, AgentOrchestratorError> {
        let tool_name = tool_name.into();
        if self.tools.contains(&tool_name) || self.routed_tools.get(&tool_name).is_some() {
            return Err(AgentOrchestratorError::DuplicateTool(tool_name));
        }
        if child.contains_approval {
            return Err(AgentOrchestratorError::SubAgentApprovalUnsupported(
                child.metadata.name,
            ));
        }
        let child_metadata = child.metadata.clone();
        self.routed_tools.insert(durable_agent_tool(
            ToolDefinition {
                name: tool_name,
                description: child_metadata.description,
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {"prompt": {"type": "string"}},
                    "required": ["prompt"]
                }),
            },
            child_metadata.orchestration,
            child_metadata.version.to_string(),
        ));
        self.children.push(child);
        Ok(self)
    }

    pub fn build(mut self) -> Result<AgentDefinition, AgentOrchestratorError> {
        for definition in self.tools.tool_definitions() {
            if self.routed_tools.get(&definition.name).is_some() {
                return Err(AgentOrchestratorError::DuplicateTool(definition.name));
            }
            let options = self
                .tool_options
                .remove(&definition.name)
                .unwrap_or_default();
            self.routed_tools.insert(ToolEntry {
                definition,
                route: crate::ToolRoute::RigTool,
                retry: options.retry,
                tag: options.tag,
                requires_approval: options.requires_approval,
            });
        }
        self.config.tools = self.routed_tools;

        let version = self.version.to_string();
        let names = RuntimeNames::for_agent(&self.name, &version);
        let activities = activity_registry_with_names(self.model, self.tools, &names);
        let orchestrations =
            orchestration_registry_with_names(self.config.clone(), names.clone(), Some(&version));
        let description = self.description.unwrap_or_else(|| self.name.clone());
        let contains_approval = self.config.approval.enabled
            || self.children.iter().any(|child| child.contains_approval);
        Ok(AgentDefinition {
            metadata: AgentMetadata {
                name: self.name,
                version: self.version,
                description,
                orchestration: names.orchestration,
                approval_queue: self.config.approval.queue_name,
                checkpoint_target: self.config.checkpoint.target_version.clone(),
            },
            activities,
            orchestrations,
            children: self.children,
            contains_approval,
        })
    }
}

pub struct AgentOrchestratorBuilder {
    provider: Arc<dyn Provider>,
    activities: ActivityRegistry,
    orchestrations: OrchestrationRegistry,
    agents: BTreeMap<String, BTreeMap<Version, AgentMetadata>>,
    runtime_options: RuntimeOptions,
}

impl AgentOrchestratorBuilder {
    pub fn register(mut self, definition: AgentDefinition) -> Result<Self, AgentOrchestratorError> {
        self.register_definition(definition)?;
        Ok(self)
    }

    fn register_definition(
        &mut self,
        definition: AgentDefinition,
    ) -> Result<(), AgentOrchestratorError> {
        let metadata = definition.metadata;
        let versions = self.agents.entry(metadata.name.clone()).or_default();
        if versions.contains_key(&metadata.version) {
            return Err(AgentOrchestratorError::DuplicateAgent {
                name: metadata.name,
                version: metadata.version.to_string(),
            });
        }
        if let Some((_, existing)) = versions.first_key_value()
            && existing.approval_queue != metadata.approval_queue
        {
            return Err(AgentOrchestratorError::ApprovalQueueMismatch {
                name: metadata.name,
                existing: existing.approval_queue.clone(),
                new: metadata.approval_queue,
            });
        }
        self.activities = ActivityRegistry::builder_from(&self.activities)
            .merge(definition.activities)
            .build_result()
            .map_err(AgentOrchestratorError::Registry)?;
        self.orchestrations = OrchestrationRegistry::builder_from(&self.orchestrations)
            .merge(definition.orchestrations)
            .build_result()
            .map_err(AgentOrchestratorError::Registry)?;
        versions.insert(metadata.version.clone(), metadata);
        for child in definition.children {
            self.register_definition(child)?;
        }
        Ok(())
    }

    pub fn merge_activities(
        mut self,
        registry: ActivityRegistry,
    ) -> Result<Self, AgentOrchestratorError> {
        self.activities = ActivityRegistry::builder_from(&self.activities)
            .merge(registry)
            .build_result()
            .map_err(AgentOrchestratorError::Registry)?;
        Ok(self)
    }

    pub fn merge_orchestrations(
        mut self,
        registry: OrchestrationRegistry,
    ) -> Result<Self, AgentOrchestratorError> {
        self.orchestrations = OrchestrationRegistry::builder_from(&self.orchestrations)
            .merge(registry)
            .build_result()
            .map_err(AgentOrchestratorError::Registry)?;
        Ok(self)
    }

    pub fn runtime_options(mut self, options: RuntimeOptions) -> Self {
        self.runtime_options = options;
        self
    }

    pub async fn start(self) -> Result<AgentOrchestrator, AgentOrchestratorError> {
        for (name, versions) in &self.agents {
            for metadata in versions.values() {
                if let Some(target) = &metadata.checkpoint_target {
                    let parsed = Version::parse(target).map_err(|source| {
                        AgentOrchestratorError::InvalidVersion {
                            version: target.clone(),
                            source,
                        }
                    })?;
                    if !versions.contains_key(&parsed) {
                        return Err(AgentOrchestratorError::AgentVersionNotFound {
                            name: name.clone(),
                            version: target.clone(),
                        });
                    }
                }
            }
        }
        let client = Client::new(Arc::clone(&self.provider));
        let runtime = Runtime::start_with_options(
            Arc::clone(&self.provider),
            self.activities,
            self.orchestrations,
            self.runtime_options,
        )
        .await;
        Ok(AgentOrchestrator {
            provider: self.provider,
            client,
            runtime,
            agents: Arc::new(self.agents),
        })
    }
}

pub struct AgentOrchestrator {
    provider: Arc<dyn Provider>,
    client: Client,
    runtime: Arc<Runtime>,
    agents: Arc<BTreeMap<String, BTreeMap<Version, AgentMetadata>>>,
}

impl AgentOrchestrator {
    pub fn builder(provider: Arc<dyn Provider>) -> AgentOrchestratorBuilder {
        AgentOrchestratorBuilder {
            provider,
            activities: ActivityRegistry::builder().build(),
            orchestrations: OrchestrationRegistry::builder().build(),
            agents: BTreeMap::new(),
            runtime_options: RuntimeOptions::default(),
        }
    }

    #[cfg(feature = "sqlite")]
    pub async fn sqlite(
        database_url: &str,
    ) -> Result<AgentOrchestratorBuilder, AgentOrchestratorError> {
        let provider = duroxide::providers::sqlite::SqliteProvider::new(database_url, None)
            .await
            .map_err(|error| AgentOrchestratorError::Sqlite(error.to_string()))?;
        Ok(Self::builder(Arc::new(provider)))
    }

    pub fn agent(&self, name: &str) -> Result<DurableAgent, AgentOrchestratorError> {
        let metadata = self
            .agents
            .get(name)
            .and_then(|versions| versions.last_key_value())
            .map(|(_, metadata)| metadata.clone())
            .ok_or_else(|| AgentOrchestratorError::AgentNotFound(name.into()))?;
        Ok(DurableAgent {
            client: self.client.clone(),
            metadata,
        })
    }

    pub fn agent_version(
        &self,
        name: &str,
        version: &str,
    ) -> Result<DurableAgent, AgentOrchestratorError> {
        let parsed =
            Version::parse(version).map_err(|source| AgentOrchestratorError::InvalidVersion {
                version: version.into(),
                source,
            })?;
        let metadata = self
            .agents
            .get(name)
            .and_then(|versions| versions.get(&parsed))
            .cloned()
            .ok_or_else(|| AgentOrchestratorError::AgentVersionNotFound {
                name: name.into(),
                version: version.into(),
            })?;
        Ok(DurableAgent {
            client: self.client.clone(),
            metadata,
        })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn runtime(&self) -> Arc<Runtime> {
        Arc::clone(&self.runtime)
    }

    pub fn provider(&self) -> Arc<dyn Provider> {
        Arc::clone(&self.provider)
    }

    pub async fn shutdown(self, timeout_ms: Option<u64>) {
        self.runtime.shutdown(timeout_ms).await;
    }
}

#[derive(Clone)]
pub struct DurableAgent {
    client: Client,
    metadata: AgentMetadata,
}

impl DurableAgent {
    pub fn builder(
        name: impl Into<String>,
        model: impl Into<DynModel<Completion>>,
    ) -> DurableAgentBuilder {
        DurableAgentBuilder::new(name, model.into())
    }

    pub fn name(&self) -> &str {
        &self.metadata.name
    }

    pub fn version(&self) -> &Version {
        &self.metadata.version
    }

    pub async fn prompt(
        &self,
        prompt: impl Into<Message>,
    ) -> Result<String, AgentOrchestratorError> {
        Ok(self.start(prompt).await?.wait().await?.output)
    }

    pub async fn start(
        &self,
        prompt: impl Into<Message>,
    ) -> Result<DurableRun, AgentOrchestratorError> {
        self.start_with_id(uuid::Uuid::new_v4().to_string(), prompt)
            .await
    }

    pub async fn start_with_id(
        &self,
        run_id: impl Into<String>,
        prompt: impl Into<Message>,
    ) -> Result<DurableRun, AgentOrchestratorError> {
        self.start_with_history(run_id, prompt, Vec::new()).await
    }

    pub async fn start_with_history(
        &self,
        run_id: impl Into<String>,
        prompt: impl Into<Message>,
        history: Vec<Message>,
    ) -> Result<DurableRun, AgentOrchestratorError> {
        let run_id = run_id.into();
        let instance_id = instance_id(&self.metadata, &run_id);
        let mut input = AgentInput::new(prompt);
        input.history = history;
        self.client
            .start_orchestration_versioned_typed(
                &instance_id,
                &self.metadata.orchestration,
                self.metadata.version.to_string(),
                input,
            )
            .await?;
        Ok(self.run(run_id))
    }

    pub fn run(&self, run_id: impl Into<String>) -> DurableRun {
        let run_id = run_id.into();
        DurableRun {
            client: self.client.clone(),
            instance_id: instance_id(&self.metadata, &run_id),
            run_id,
            approval_queue: self.metadata.approval_queue.clone(),
        }
    }
}

fn instance_id(metadata: &AgentMetadata, run_id: &str) -> String {
    let mut digest = Sha256::new();
    let version = metadata.version.to_string();
    for part in [metadata.name.as_str(), version.as_str(), run_id] {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part.as_bytes());
    }
    format!("rig-agent-{:x}", digest.finalize())
}

#[derive(Clone)]
pub struct DurableRun {
    client: Client,
    run_id: String,
    instance_id: String,
    approval_queue: String,
}

impl DurableRun {
    pub fn id(&self) -> &str {
        &self.run_id
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub async fn wait(&self) -> Result<PromptResponse, AgentOrchestratorError> {
        self.wait_timeout(DEFAULT_WAIT_TIMEOUT).await
    }

    pub async fn wait_timeout(
        &self,
        timeout: Duration,
    ) -> Result<PromptResponse, AgentOrchestratorError> {
        self.client
            .wait_for_orchestration_typed(&self.instance_id, timeout)
            .await?
            .map_err(AgentOrchestratorError::Run)
    }

    pub async fn next_approval(&self) -> Result<ApprovalRequest, AgentOrchestratorError> {
        let deadline = tokio::time::Instant::now() + DEFAULT_WAIT_TIMEOUT;
        loop {
            match self
                .client
                .get_orchestration_status(&self.instance_id)
                .await?
            {
                OrchestrationStatus::Running {
                    custom_status: Some(status),
                    ..
                } => {
                    let status: serde_json::Value = match serde_json::from_str(&status) {
                        Ok(status) => status,
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            continue;
                        }
                    };
                    if status["phase"] == "approval" {
                        return serde_json::from_value(status["request"].clone())
                            .map_err(|error| AgentOrchestratorError::Run(error.to_string()));
                    }
                }
                OrchestrationStatus::Running { .. } => {}
                OrchestrationStatus::NotFound => {}
                OrchestrationStatus::Completed { .. } | OrchestrationStatus::Failed { .. } => {
                    return Err(AgentOrchestratorError::ApprovalNotRequested(
                        self.run_id.clone(),
                    ));
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(AgentOrchestratorError::Client(ClientError::Timeout));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub async fn approve(&self, request: &ApprovalRequest) -> Result<(), AgentOrchestratorError> {
        self.send_decision(ApprovalDecision::Approve {
            approval_id: request.approval_id.clone(),
        })
        .await
    }

    pub async fn deny(
        &self,
        request: &ApprovalRequest,
        reason: impl Into<String>,
    ) -> Result<(), AgentOrchestratorError> {
        self.send_decision(ApprovalDecision::Deny {
            approval_id: request.approval_id.clone(),
            reason: Some(reason.into()),
        })
        .await
    }

    /// Queue a message as the next agent turn after the active turn finishes.
    pub async fn steer(&self, message: impl Into<Message>) -> Result<(), AgentOrchestratorError> {
        let command_id = uuid::Uuid::new_v4().to_string();
        self.client
            .enqueue_event_typed(
                &self.instance_id,
                STEERING_QUEUE_NAME,
                &SteeringCommand {
                    command_id: command_id.clone(),
                    message: message.into(),
                },
            )
            .await?;
        let deadline = tokio::time::Instant::now() + DEFAULT_WAIT_TIMEOUT;
        loop {
            let status = self
                .client
                .get_orchestration_status(&self.instance_id)
                .await?;
            if orchestration_status_value_is(&status, "last_steering_id", &command_id) {
                return Ok(());
            }
            match status {
                OrchestrationStatus::Completed { .. } | OrchestrationStatus::Failed { .. } => {
                    return Err(AgentOrchestratorError::SteeringNotAccepted(
                        self.run_id.clone(),
                    ));
                }
                OrchestrationStatus::NotFound | OrchestrationStatus::Running { .. } => {}
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(AgentOrchestratorError::SteeringNotAccepted(
                    self.run_id.clone(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn send_decision(
        &self,
        decision: ApprovalDecision,
    ) -> Result<(), AgentOrchestratorError> {
        self.client
            .enqueue_event_typed(&self.instance_id, &self.approval_queue, &decision)
            .await?;
        Ok(())
    }

    pub async fn cancel(&self, reason: impl Into<String>) -> Result<(), AgentOrchestratorError> {
        self.client
            .cancel_instance(&self.instance_id, reason.into())
            .await?;
        Ok(())
    }

    pub async fn status(&self) -> Result<OrchestrationStatus, AgentOrchestratorError> {
        Ok(self
            .client
            .get_orchestration_status(&self.instance_id)
            .await?)
    }
}

fn orchestration_status_value_is(status: &OrchestrationStatus, key: &str, expected: &str) -> bool {
    let custom_status = match status {
        OrchestrationStatus::Running { custom_status, .. }
        | OrchestrationStatus::Completed { custom_status, .. }
        | OrchestrationStatus::Failed { custom_status, .. } => custom_status.as_deref(),
        OrchestrationStatus::NotFound => None,
    };
    let Some(custom_status) = custom_status else {
        return false;
    };
    serde_json::from_str::<serde_json::Value>(custom_status)
        .ok()
        .and_then(|status| {
            status
                .get(key)
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .is_some_and(|value| value == expected)
}
