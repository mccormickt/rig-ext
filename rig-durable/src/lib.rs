//! Run Rig agents on Duroxide, Temporal, or SQLite Durable Objects.
//!
//! Pass a Rig completion model and Rig [`Tool`](rig::tool::Tool) implementations
//! to a backend's agent builder. The backend records model responses and tool
//! results. Replay uses those records instead of repeating completed I/O.
//! Activities can still be retried: tools that write to external systems must
//! declare and implement an appropriate [`ToolPolicy`].
//!
//! # Choose a backend
//!
//! | Cargo features | Entry point | Storage and worker |
//! |---|---|---|
//! | Default: `duroxide`, `sqlite` | `DurableAgent::builder` | Embedded Duroxide runtime and SQLite |
//! | `duroxide` without defaults | `AgentOrchestrator::builder` | Application-supplied Duroxide provider |
//! | `temporal` without defaults | `temporal::TemporalAgent::new` | Temporal server and activity worker |
//! | `durable-object` without defaults | `durable_object::Builder::new` | Cloudflare or celld SQLite Durable Objects |
//! | No features | Shared configuration, policy, identity, result, and compaction types | Native and WASM; no runtime |
//!
//! Enable `temporal` with the default features to use both native backends. Use Rig
//! 0.43 models and tools directly; an already-built `rig::Agent` cannot be
//! converted because it does not expose all required configuration.
//!
//! # Run a Rig model with SQLite
//!
//! This example requires the default features and `OPENAI_API_KEY`. The SQLite
//! file retains execution history. The model and its credentials stay on the
//! worker; they are not serialized into workflow input.
//!
//! ```no_run
//! # #[cfg(feature = "sqlite")]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use rig::providers::openai::{self, OpenAI};
//! use rig_durable::{AgentOrchestrator, DurableAgent, InvocationContract};
//!
//! let model = OpenAI::from_env()?.completion(openai::GPT_4O_MINI);
//! let definition = DurableAgent::builder("assistant", model)
//!     .invocation_contract(InvocationContract::Logical)
//!     .preamble("Answer briefly and state any uncertainty.")
//!     .max_turns(8)
//!     .build()?;
//! let orchestrator = AgentOrchestrator::sqlite("sqlite://agents.db?mode=rwc")
//!     .await?
//!     .register(definition)?
//!     .start()
//!     .await?;
//! let answer = orchestrator.agent("assistant")?.prompt("Explain durable execution.").await?;
//! println!("{answer}");
//! orchestrator.shutdown(None).await;
//! # Ok(())
//! # }
//! ```
//!
//! Use `start` instead of `prompt` to get a run handle for approvals, steering,
//! cancellation, and detailed results. Use `open_session` for a conversation
//! that retains history across prompts. See `DurableRun`, `DurableSession`, and
//! the `temporal` module for the corresponding examples.
//!
//! # Add a Rig tool
//!
//! A durable tool implements the standard Rig trait. Register this tool with
//! `.tool(Add)` on either backend's builder. Use `.tool_with` on Duroxide or
//! `.tool_with_policy` on Temporal to declare [`ToolPolicy::read_only`].
//!
//! ```
//! use std::convert::Infallible;
//! use rig::tool::{Tool, ToolContext};
//! use serde::Deserialize;
//!
//! struct Add;
//!
//! #[derive(Deserialize)]
//! struct AddArgs { left: i64, right: i64 }
//!
//! impl Tool for Add {
//!     const NAME: &'static str = "add";
//!     type Args = AddArgs;
//!     type Output = i128;
//!     type Error = Infallible;
//!
//!     fn description(&self) -> String { "Add two integers".into() }
//!
//!     fn parameters(&self) -> serde_json::Value {
//!         serde_json::json!({
//!             "type": "object",
//!             "properties": {
//!                 "left": {"type": "integer"},
//!                 "right": {"type": "integer"}
//!             },
//!             "required": ["left", "right"]
//!         })
//!     }
//!
//!     async fn call(&self, _: &mut ToolContext, args: AddArgs) -> Result<i128, Infallible> {
//!         Ok(i128::from(args.left) + i128::from(args.right))
//!     }
//! }
//! ```
//!
//! # Control execution and history
//!
//! - [`ToolPolicy`] selects replay safety and retained result metadata. An
//!   idempotent tool uses [`ToolInvocation::logical_key`] from its Rig context.
//!   [`InvocationGuardStore`] is required for uncertain-effect protection.
//! - [`SubmitInput`] gives a session request a stable client ID. Reuse the ID
//!   only when retrying the same message and mode.
//! - [`DurableResponse`] adds ordered tool dispositions to Rig's unchanged
//!   [`PromptResponse`](rig::agent::PromptResponse).
//! - [`Compaction`] combines a Rig memory policy and compactor. It limits the
//!   active model context between prompts, not the full audit transcript.
//! - Duroxide checkpoints and Temporal continue-as-new limit event-history
//!   windows. They do not remove the transcript or deduplication receipts.
//!
//! # Preserve recorded histories
//!
//! [`InvocationContract::Logical`] records configuration snapshots for new
//! top-level Duroxide runs and is the default for new Temporal inputs.
//! Duroxide defaults to [`InvocationContract::Legacy`] for recorded-history
//! replay. Keep the required agent versions and tool implementations registered
//! while their histories are live. Child and raw Duroxide starts resolve their
//! registered configuration until a checkpoint captures a snapshot; raw callers
//! can also supply one explicitly. The crate is unreleased at version `0.1.0`.

pub mod activities;
pub mod activity_types;
pub mod approval;
pub mod compaction;
pub mod config;
#[cfg(any(feature = "duroxide", feature = "temporal", feature = "durable-object"))]
mod driver;
#[cfg(feature = "durable-object")]
pub mod durable_object;
#[cfg(feature = "duroxide")]
mod facade;
pub mod guard;
pub mod identity;
#[cfg(feature = "duroxide")]
pub mod names;
#[cfg(feature = "duroxide")]
pub mod orchestration;
pub mod outcome;
pub mod policy;
#[cfg(feature = "duroxide")]
pub mod registry;
pub mod result;
pub mod retry;
#[cfg(feature = "duroxide")]
pub mod session;
pub mod streaming;
pub mod submission;
#[cfg(feature = "temporal")]
pub mod temporal;
pub mod tools;
#[cfg(feature = "duroxide")]
pub mod types;

pub use activities::tool::ToolExecutor;
#[cfg(feature = "duroxide")]
pub use activity_types::StreamingCompletionOutput;
pub use activity_types::{
    InvocationContract, ToolActivityInput, ToolActivityOutput, ToolInvocation,
};
pub use approval::{ApprovalDecision, ApprovalRequest};
pub use compaction::{
    Compaction, CompactionArtifact, CompactionConfig, CompactionRecord, ContextState,
    ModelCompactor, ModelSummary,
};
pub use config::{
    ApprovalConfig, CheckpointConfig, CheckpointPolicy, CompletionMode, CompletionSettings,
    DEFAULT_APPROVAL_QUEUE, DurableAgentConfig,
};
#[cfg(feature = "duroxide")]
pub use facade::{
    AgentDefinition, AgentOrchestrator, AgentOrchestratorBuilder, AgentOrchestratorError,
    DurableAgent, DurableAgentBuilder, DurableRun, DurableSession,
};
pub use guard::{InMemoryGuardStore, InvocationGuardStore};
pub use identity::{AttemptMetadata, LogicalCallKey};
pub use outcome::{DurableResponse, OutcomeSource, ToolOutcome};
pub use policy::{MetadataRetention, ReplaySafety, ToolPolicy};
#[cfg(feature = "duroxide")]
pub use registry::{activity_registry, orchestration_registry};
pub use result::{DurableToolResult, InterruptionReason, ToolDisposition};
pub use retry::RetryPolicy;
#[cfg(feature = "duroxide")]
pub use session::{SessionInput, SessionResult};
pub use streaming::{StreamItem, StreamTranscript};
pub use submission::{
    Submission, SubmissionError, SubmissionLedger, SubmissionMode, SubmissionState, SubmitInput,
};
pub use tools::ToolOptions;
#[cfg(feature = "duroxide")]
pub use tools::sub_orchestration_tool;
pub use tools::{ToolCatalog, ToolEntry, ToolRoute, activity_tool, catalog_from_toolset};
#[cfg(feature = "duroxide")]
pub use types::AgentInput;

#[cfg(all(test, feature = "duroxide"))]
mod tests {
    use std::{
        convert::Infallible,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use duroxide::{
        Client, OrchestrationStatus, RetryPolicy, providers::sqlite::SqliteProvider, runtime,
    };
    use rig::{
        agent::PromptResponse,
        test_utils::{MockAddTool, MockCompletionModel, MockTurn},
        tool::{Tool, ToolContext, ToolSet},
    };
    use serde::Deserialize;
    use sha2::{Digest, Sha256};

    use crate::{
        AgentInput, ApprovalConfig, ApprovalDecision, ApprovalRequest, DurableAgentConfig,
        activity_registry, catalog_from_toolset, names::ORCHESTRATION, orchestration_registry,
    };

    #[derive(Clone)]
    struct CountingAdd(Arc<AtomicUsize>);

    #[derive(Deserialize)]
    struct AddArgs {
        x: i64,
        y: i64,
    }

    impl Tool for CountingAdd {
        const NAME: &'static str = "add";
        type Args = AddArgs;
        type Output = i64;
        type Error = Infallible;

        fn description(&self) -> String {
            "Add two integers".into()
        }
        fn parameters(&self) -> serde_json::Value {
            serde_json::json!({"type":"object","properties":{"x":{"type":"integer"},"y":{"type":"integer"}}})
        }
        async fn call(
            &self,
            _context: &mut ToolContext,
            args: AddArgs,
        ) -> Result<i64, Self::Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(args.x + args.y)
        }
    }

    async fn wait_for_approval(client: &Client, instance: &str) -> ApprovalRequest {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let OrchestrationStatus::Running {
                    custom_status: Some(status),
                    ..
                } = client.get_orchestration_status(instance).await.unwrap()
                {
                    let status: serde_json::Value = serde_json::from_str(&status).unwrap();
                    if status["phase"] == "approval" {
                        return serde_json::from_value(status["request"].clone()).unwrap();
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap()
    }

    fn approval_config() -> ApprovalConfig {
        ApprovalConfig {
            enabled: true,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn agent_run_executes_model_and_tool_as_activities() {
        let model = MockCompletionModel::from_turns([
            MockTurn::tool_call("call-1", "add", serde_json::json!({"x": 20, "y": 22})),
            MockTurn::text("The answer is 42."),
        ]);
        let tools = ToolSet::from_tools(vec![MockAddTool]);
        let catalog = catalog_from_toolset(&tools, RetryPolicy::new(1)).await;
        let activities = activity_registry(model, tools);
        let orchestrations = orchestration_registry(DurableAgentConfig {
            tools: catalog,
            ..Default::default()
        });
        let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
        let runtime =
            runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
        let client = Client::new(store);

        client
            .start_orchestration_typed(
                "rig-tool-run",
                ORCHESTRATION,
                AgentInput::new("What is 20 + 22?"),
            )
            .await
            .unwrap();
        let response = client
            .wait_for_orchestration_typed::<PromptResponse>("rig-tool-run", Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(response.output, "The answer is 42.");
        runtime.shutdown(None).await;
    }

    #[tokio::test]
    async fn approved_tool_executes_exactly_once() {
        let count = Arc::new(AtomicUsize::new(0));
        let tools = ToolSet::from_tools(vec![CountingAdd(count.clone())]);
        let mut catalog = catalog_from_toolset(&tools, RetryPolicy::new(1)).await;
        catalog.0.get_mut("add").unwrap().requires_approval = true;
        let config = DurableAgentConfig {
            tools: catalog,
            approval: approval_config(),
            ..Default::default()
        };
        let model = MockCompletionModel::from_turns([
            MockTurn::tool_call("call-approved", "add", serde_json::json!({"x": 2, "y": 3})),
            MockTurn::text("done"),
        ]);
        let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
        let runtime = runtime::Runtime::start_with_store(
            store.clone(),
            activity_registry(model, tools),
            orchestration_registry(config.clone()),
        )
        .await;
        let client = Client::new(store);
        client
            .start_orchestration_typed("approved", ORCHESTRATION, AgentInput::new("add"))
            .await
            .unwrap();
        let request = wait_for_approval(&client, "approved").await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
        client
            .enqueue_event_typed(
                "approved",
                &config.approval.queue_name,
                &ApprovalDecision::Approve {
                    approval_id: request.approval_id,
                },
            )
            .await
            .unwrap();
        let response = client
            .wait_for_orchestration_typed::<PromptResponse>("approved", Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.output, "done");
        assert_eq!(count.load(Ordering::SeqCst), 1);
        runtime.shutdown(None).await;
    }

    #[tokio::test]
    async fn denied_tool_is_not_executed_and_model_continues() {
        let count = Arc::new(AtomicUsize::new(0));
        let tools = ToolSet::from_tools(vec![CountingAdd(count.clone())]);
        let mut catalog = catalog_from_toolset(&tools, RetryPolicy::new(1)).await;
        catalog.0.get_mut("add").unwrap().requires_approval = true;
        let config = DurableAgentConfig {
            tools: catalog,
            approval: approval_config(),
            ..Default::default()
        };
        let model = MockCompletionModel::from_turns([
            MockTurn::tool_call("call-denied", "add", serde_json::json!({"x": 2, "y": 3})),
            MockTurn::text("recovered from denial"),
        ]);
        let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
        let runtime = runtime::Runtime::start_with_store(
            store.clone(),
            activity_registry(model, tools),
            orchestration_registry(config.clone()),
        )
        .await;
        let client = Client::new(store);
        client
            .start_orchestration_typed("denied", ORCHESTRATION, AgentInput::new("add"))
            .await
            .unwrap();
        let request = wait_for_approval(&client, "denied").await;
        client
            .enqueue_event_typed(
                "denied",
                &config.approval.queue_name,
                &ApprovalDecision::Deny {
                    approval_id: request.approval_id,
                    reason: Some("not permitted".into()),
                },
            )
            .await
            .unwrap();
        let response = client
            .wait_for_orchestration_typed::<PromptResponse>("denied", Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.output, "recovered from denial");
        assert_eq!(count.load(Ordering::SeqCst), 0);
        runtime.shutdown(None).await;
    }

    #[tokio::test]
    async fn malformed_and_stale_decisions_are_ignored_and_early_match_is_consumed() {
        let count = Arc::new(AtomicUsize::new(0));
        let arguments = serde_json::json!({"x": 8, "y": 9});
        let tools = ToolSet::from_tools(vec![CountingAdd(count.clone())]);
        let mut catalog = catalog_from_toolset(&tools, RetryPolicy::new(1)).await;
        catalog.0.get_mut("add").unwrap().requires_approval = true;
        let config = DurableAgentConfig {
            tools: catalog,
            approval: approval_config(),
            ..Default::default()
        };
        let model = MockCompletionModel::from_turns([
            MockTurn::tool_call("call-early", "add", arguments.clone()),
            MockTurn::text("early approved"),
        ]);
        let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap());
        let runtime = runtime::Runtime::start_with_store(
            store.clone(),
            activity_registry(model, tools),
            orchestration_registry(config.clone()),
        )
        .await;
        let client = Client::new(store);
        client
            .start_orchestration_typed("early", ORCHESTRATION, AgentInput::new("add"))
            .await
            .unwrap();
        client
            .enqueue_event("early", &config.approval.queue_name, "not-json")
            .await
            .unwrap();
        client
            .enqueue_event_typed(
                "early",
                &config.approval.queue_name,
                &ApprovalDecision::Approve {
                    approval_id: "stale".into(),
                },
            )
            .await
            .unwrap();
        let digest = Sha256::digest(serde_json::to_vec(&arguments).unwrap());
        let approval_id = format!("prompt-0-turn-1-call-0-call-early-{digest:x}");
        client
            .enqueue_event_typed(
                "early",
                &config.approval.queue_name,
                &ApprovalDecision::Approve { approval_id },
            )
            .await
            .unwrap();
        let response = client
            .wait_for_orchestration_typed::<PromptResponse>("early", Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.output, "early approved");
        assert_eq!(count.load(Ordering::SeqCst), 1);
        runtime.shutdown(None).await;
    }
}
