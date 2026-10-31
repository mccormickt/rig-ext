//! The tool activity. It runs a Rig tool once per attempt and decides, from
//! the error's retryability and the tool policy, whether to fail the attempt
//! so the backend can schedule another one or to record the outcome.

use std::{collections::BTreeMap, sync::Arc};

use rig::tool::{ToolContext, ToolSet};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    activity_types::{ToolActivityInput, ToolActivityOutput},
    guard::{
        ClaimOutcome, ClaimRecord, ClaimRequest, ClaimState, GuardError, InvocationGuardStore,
        SettleOutcome,
    },
    identity::arguments_digest,
    policy::{ErrorHandling, ToolPolicy, error_handling},
    result::{DurableToolResult, InterruptionReason},
};

/// A declaration or identity mismatch. No new tool execution is permitted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("incompatible tool invocation: {0}")]
pub struct InvocationConfigurationError(pub String);

#[derive(Debug, thiserror::Error)]
pub enum ToolExecutionFailure {
    #[error(transparent)]
    Configuration(#[from] InvocationConfigurationError),
    #[error(transparent)]
    Guard(#[from] GuardError),
    #[error("{0}")]
    Retryable(String),
}

/// Duroxide records configuration failures as data, without retrying them.
pub type ToolActivityResult = Result<ToolActivityOutput, InvocationConfigurationError>;

#[cfg(feature = "duroxide")]
impl ToolExecutionFailure {
    pub(crate) fn activity_result(
        result: Result<ToolActivityOutput, Self>,
    ) -> Result<ToolActivityResult, String> {
        match result {
            Ok(output) => Ok(Ok(output)),
            Err(Self::Configuration(error)) => Ok(Err(error)),
            Err(error) => Err(error.to_string()),
        }
    }
}

/// Worker-side tool execution shared by both backends.
pub struct ToolExecutor {
    tools: Arc<ToolSet>,
    guard: Option<Arc<dyn InvocationGuardStore>>,
    registered: BTreeMap<String, ToolPolicy>,
}

impl ToolExecutor {
    pub fn new(tools: Arc<ToolSet>) -> Self {
        Self {
            tools,
            guard: None,
            registered: BTreeMap::new(),
        }
    }

    pub fn with_guard(mut self, guard: Option<Arc<dyn InvocationGuardStore>>) -> Self {
        self.guard = guard;
        self
    }

    /// Policies this worker registered, by tool name. New execution requires
    /// exact safety and version matches. Recorded result recovery does not.
    pub fn with_registered_policies(
        mut self,
        policies: impl IntoIterator<Item = (String, ToolPolicy)>,
    ) -> Self {
        self.registered = policies.into_iter().collect();
        self
    }

    pub fn tools(&self) -> &ToolSet {
        &self.tools
    }

    /// Recover a retained result before checking permission for new execution.
    pub async fn execute(
        &self,
        input: ToolActivityInput,
    ) -> Result<ToolActivityOutput, ToolExecutionFailure> {
        self.validate_invocation(&input)?;
        let policy = input.policy.clone();
        if policy.safety().requires_guard() {
            return self.execute_guarded(input, &policy).await;
        }
        self.execute_committed(input).await
    }

    pub(crate) fn validate_invocation(
        &self,
        input: &ToolActivityInput,
    ) -> Result<(), InvocationConfigurationError> {
        let invocation = &input.invocation;
        let key = &invocation.logical_key;
        if key.logical_execution_id.is_empty()
            || key.submission_id.is_empty()
            || key.model_turn != invocation.turn
            || key.call_index != invocation.call_index
            || invocation.execution_id.is_empty()
            || invocation.attempt.backend_execution_id.is_empty()
            || invocation.attempt.activity_attempt == Some(0)
        {
            return Err(InvocationConfigurationError(
                "invalid logical identity or attempt metadata".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_execution(
        &self,
        input: &ToolActivityInput,
    ) -> Result<(), InvocationConfigurationError> {
        if let Some(recorded) = &input.runtime_identity {
            #[cfg(all(feature = "codemode", not(target_family = "wasm")))]
            let current = Some(crate::codemode::CodeModeConfig::runtime_identity(
                &rig_codemode::replay::NativeReplayPlanner,
            ));
            #[cfg(not(all(feature = "codemode", not(target_family = "wasm"))))]
            let current: Option<String> = None;
            if current.as_ref() != Some(recorded) {
                return Err(InvocationConfigurationError(
                    "code-mode runtime identity mismatch".into(),
                ));
            }
        }
        let registered = self
            .registered
            .get(&input.name)
            .filter(|_| self.tools.contains(&input.name))
            .ok_or_else(|| {
                InvocationConfigurationError(format!(
                    "tool `{}` has no executable registration and policy",
                    input.name
                ))
            })?;
        if registered.version() != input.policy.version()
            || registered.safety() != input.policy.safety()
        {
            return Err(InvocationConfigurationError(format!(
                "tool `{}` requires recorded safety {:?} and implementation version `{}`; worker declares {:?} and `{}`",
                input.name,
                input.policy.safety(),
                input.policy.version(),
                registered.safety(),
                registered.version()
            )));
        }
        Ok(())
    }

    /// Execute an intent already claimed by the backend's transaction.
    #[cfg_attr(not(feature = "durable-object"), allow(dead_code))]
    pub(crate) async fn execute_committed(
        &self,
        input: ToolActivityInput,
    ) -> Result<ToolActivityOutput, ToolExecutionFailure> {
        self.validate_invocation(&input)?;
        self.validate_execution(&input)?;
        let policy = input.policy.clone();
        let result = self.run_tool(&input, &policy).await?;
        if let Some(error) = result.result.as_ref().and_then(|result| result.error())
            && error_handling(error, policy.safety()) == ErrorHandling::Retry
        {
            return Err(ToolExecutionFailure::Retryable(error.to_string()));
        }
        Ok(ToolActivityOutput::from_result(&input.name, result))
    }

    async fn run_tool(
        &self,
        input: &ToolActivityInput,
        policy: &ToolPolicy,
    ) -> Result<DurableToolResult, ToolExecutionFailure> {
        let mut context = ToolContext::new();
        context
            .insert(input.invocation.clone())
            .map_err(|error| InvocationConfigurationError(error.to_string()))?;
        let result = self
            .tools
            .execute(&input.name, input.arguments.clone(), &mut context)
            .await;
        let (result, _dropped) =
            DurableToolResult::from_rig(result, &context.result_context(), policy.metadata());
        Ok(result)
    }

    async fn execute_guarded(
        &self,
        input: ToolActivityInput,
        policy: &ToolPolicy,
    ) -> Result<ToolActivityOutput, ToolExecutionFailure> {
        let Some(guard) = &self.guard else {
            return Err(InvocationConfigurationError(format!(
                "tool `{}` requires an invocation guard store, but the worker has none",
                input.name
            ))
            .into());
        };
        let key = input.invocation.logical_key.clone();
        if let Some(record) = guard.get(&key).await? {
            return Ok(recover(&input, record));
        }
        self.validate_execution(&input)?;
        let claim_token = uuid::Uuid::new_v4().to_string();
        let claim = ClaimRequest {
            key: key.clone(),
            tool_name: input.name.clone(),
            arguments_digest: argument_digest(&input.arguments),
            policy: policy.clone(),
            attempt: input.invocation.attempt.clone(),
            claim_token: claim_token.clone(),
        };
        let result = match guard.claim(claim).await? {
            ClaimOutcome::Created => {
                let result = self.run_tool(&input, policy).await?;
                match guard.settle(&key, &claim_token, result.clone()).await? {
                    SettleOutcome::Settled => result,
                    SettleOutcome::Rejected => DurableToolResult::interrupted(
                        InterruptionReason::ClaimMismatch,
                        "the invocation claim changed before the result was recorded",
                    ),
                }
            }
            ClaimOutcome::Existing(record) => return Ok(recover(&input, *record)),
        };
        Ok(ToolActivityOutput::from_result(&input.name, result))
    }
}

fn recover(input: &ToolActivityInput, record: ClaimRecord) -> ToolActivityOutput {
    let result = if record.claim.key != input.invocation.logical_key
        || record.claim.tool_name != input.name
        || record.claim.arguments_digest != argument_digest(&input.arguments)
        || record.claim.policy != input.policy
    {
        DurableToolResult::interrupted(
            InterruptionReason::ClaimMismatch,
            "an earlier attempt claimed this call with a different tool, arguments, or policy",
        )
    } else {
        match record.state {
            ClaimState::Settled { result } => result,
            ClaimState::Claimed => DurableToolResult::interrupted(
                InterruptionReason::ClaimHeld,
                format!(
                    "attempt `{}` claimed this call and has not recorded a result",
                    record.claim.attempt.backend_execution_id
                ),
            ),
        }
    };
    ToolActivityOutput::from_result(&input.name, result)
}

fn argument_digest(arguments: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(arguments) {
        Ok(value) => arguments_digest(&value)
            .unwrap_or_else(|_| format!("{:x}", Sha256::digest(arguments.as_bytes()))),
        Err(_) => format!("{:x}", Sha256::digest(arguments.as_bytes())),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::{
        convert::Infallible,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use rig::tool::{ContextValue, Tool, ToolExecutionError};
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{
        activity_types::ToolInvocation,
        guard::InMemoryGuardStore,
        identity::{AttemptMetadata, LogicalCallKey},
        policy::MetadataRetention,
        result::ToolDisposition,
    };

    struct ReadInvocation;

    #[derive(Deserialize)]
    struct NoArgs {}

    impl Tool for ReadInvocation {
        const NAME: &'static str = "invocation";
        type Args = NoArgs;
        type Output = String;
        type Error = Infallible;

        fn description(&self) -> String {
            "Return the durable invocation identity".into()
        }

        fn parameters(&self) -> serde_json::Value {
            serde_json::json!({"type":"object"})
        }

        async fn call(
            &self,
            context: &mut ToolContext,
            _args: NoArgs,
        ) -> Result<Self::Output, Self::Error> {
            let invocation = context.require::<ToolInvocation>().unwrap();
            Ok(format!(
                "{}:{}:{}:{}:{}",
                invocation.execution_id,
                invocation.prompt_index,
                invocation.turn,
                invocation.call_index,
                invocation.logical_key.canonical()
            ))
        }
    }

    /// Returns a result whose disposition is chosen by the `mode` argument.
    struct Disposition;

    #[derive(Deserialize)]
    struct DispositionArgs {
        mode: String,
    }

    impl Tool for Disposition {
        const NAME: &'static str = "disposition";
        type Args = DispositionArgs;
        type Output = String;
        type Error = ToolExecutionError;

        fn description(&self) -> String {
            "Return a chosen disposition".into()
        }

        fn parameters(&self) -> serde_json::Value {
            serde_json::json!({"type":"object","properties":{"mode":{"type":"string"}}})
        }

        async fn call(
            &self,
            _context: &mut ToolContext,
            args: DispositionArgs,
        ) -> Result<Self::Output, Self::Error> {
            let text = "same words";
            match args.mode.as_str() {
                "success" => Ok(text.into()),
                "error" => Err(ToolExecutionError::other(text)),
                "refused" => Err(ToolExecutionError::refused(text)),
                "network" => Err(ToolExecutionError::network(text)),
                "provider_final" => Err(ToolExecutionError::provider(text).with_retryable(false)),
                "provider" => Err(ToolExecutionError::provider(text)),
                other => Err(ToolExecutionError::invalid_args(other)),
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Receipt(String);
    impl ContextValue for Receipt {
        const KEY: &'static str = "receipt";
    }

    #[derive(Serialize, Deserialize)]
    struct Secret(String);
    impl ContextValue for Secret {
        const KEY: &'static str = "secret";
    }

    /// Writes an inbound secret and publishes a receipt.
    struct Publisher;

    impl Tool for Publisher {
        const NAME: &'static str = "publisher";
        type Args = NoArgs;
        type Output = String;
        type Error = Infallible;

        fn description(&self) -> String {
            "Publish result metadata".into()
        }

        fn parameters(&self) -> serde_json::Value {
            serde_json::json!({"type":"object"})
        }

        async fn call(
            &self,
            context: &mut ToolContext,
            _args: NoArgs,
        ) -> Result<Self::Output, Self::Error> {
            context.insert(Secret("inbound-credential".into())).unwrap();
            context.insert_result(Receipt("receipt-77".into())).unwrap();
            context
                .insert_result(Secret("published-credential".into()))
                .unwrap();
            Ok("published".into())
        }
    }

    struct Counting(Arc<AtomicUsize>);

    impl Tool for Counting {
        const NAME: &'static str = "counting";
        type Args = NoArgs;
        type Output = usize;
        type Error = Infallible;

        fn description(&self) -> String {
            "Count calls".into()
        }

        fn parameters(&self) -> serde_json::Value {
            serde_json::json!({"type":"object"})
        }

        async fn call(
            &self,
            _context: &mut ToolContext,
            _args: NoArgs,
        ) -> Result<Self::Output, Self::Error> {
            Ok(self.0.fetch_add(1, Ordering::SeqCst) + 1)
        }
    }

    fn key() -> LogicalCallKey {
        LogicalCallKey {
            logical_execution_id: "run-1".into(),
            submission_id: "prompt-4".into(),
            model_turn: 2,
            call_index: 3,
        }
    }

    fn input(name: &str, arguments: &str, policy: ToolPolicy) -> ToolActivityInput {
        ToolActivityInput {
            name: name.into(),
            arguments: arguments.into(),
            invocation: ToolInvocation {
                execution_id: "run-1:exec-1".into(),
                prompt_index: 4,
                turn: 2,
                call_index: 3,
                logical_key: key(),
                attempt: AttemptMetadata {
                    backend_execution_id: "exec-1".into(),
                    activity_attempt: Some(1),
                },
            },
            policy,
            runtime_identity: None,
        }
    }

    fn text(output: &ToolActivityOutput) -> String {
        match &output.content[0] {
            rig::message::ToolResultContent::Text(text) => text.text.clone(),
            rig::message::ToolResultContent::Json { value } => value.to_string(),
            other => panic!("unexpected content {other:?}"),
        }
    }

    #[tokio::test]
    async fn exposes_the_stable_invocation_to_rig_tools() {
        let executor = ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![ReadInvocation])))
            .with_registered_policies([("invocation".into(), ToolPolicy::idempotent())]);

        let output = executor
            .execute(input("invocation", "{}", ToolPolicy::idempotent()))
            .await
            .unwrap();
        assert_eq!(
            text(&output),
            "run-1:exec-1:4:2:3:v1;5:run-1;8:prompt-4;2;3"
        );
    }

    #[tokio::test]
    async fn equal_content_keeps_distinct_dispositions_through_execution() {
        let executor = ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![Disposition])))
            .with_registered_policies([("disposition".into(), ToolPolicy::read_only())]);
        let cases = [
            ("success", ToolDisposition::Success, false),
            ("error", ToolDisposition::Error, true),
            ("refused", ToolDisposition::Refused, true),
        ];
        for (mode, disposition, is_error) in cases {
            let arguments = format!(r#"{{"mode":"{mode}"}}"#);
            let output = executor
                .execute(input("disposition", &arguments, ToolPolicy::read_only()))
                .await
                .unwrap();
            let json = serde_json::to_string(&output).unwrap();
            let decoded: ToolActivityOutput = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded.is_error, is_error, "{mode}");
            assert_eq!(text(&decoded), "same words", "{mode}");
            assert_eq!(decoded.result.disposition, disposition, "{mode}");
        }
    }

    #[tokio::test]
    async fn retry_decision_separates_retryability_from_replay_safety() {
        let network = r#"{"mode":"network"}"#;
        let provider_final = r#"{"mode":"provider_final"}"#;
        let provider = r#"{"mode":"provider"}"#;

        // Retryable error, repeat permitted: the attempt fails.
        for policy in [
            ToolPolicy::application_managed(),
            ToolPolicy::read_only(),
            ToolPolicy::idempotent(),
        ] {
            let executor = ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![Disposition])))
                .with_registered_policies([("disposition".into(), policy.clone())]);
            assert!(matches!(
                executor
                    .execute(input("disposition", network, policy.clone()))
                    .await,
                Err(ToolExecutionFailure::Retryable(_))
            ));
            assert!(matches!(
                executor
                    .execute(input("disposition", provider, policy))
                    .await,
                Err(ToolExecutionFailure::Retryable(_))
            ));
        }
        // Explicit `retryable = false` is recorded even for a replay-safe tool.
        let executor = ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![Disposition])))
            .with_registered_policies([("disposition".into(), ToolPolicy::read_only())]);
        let output = executor
            .execute(input(
                "disposition",
                provider_final,
                ToolPolicy::read_only(),
            ))
            .await
            .unwrap();
        assert_eq!(output.result.disposition, ToolDisposition::Error);
        // A retryable error under a policy that never repeats is recorded.
        let guarded = ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![Disposition])))
            .with_registered_policies([(
                "disposition".into(),
                ToolPolicy::interrupt_on_uncertain(),
            )])
            .with_guard(Some(Arc::new(InMemoryGuardStore::new())));
        let output = guarded
            .execute(input(
                "disposition",
                network,
                ToolPolicy::interrupt_on_uncertain(),
            ))
            .await
            .unwrap();
        assert_eq!(output.result.disposition, ToolDisposition::Error);
    }

    #[tokio::test]
    async fn only_approved_result_metadata_is_retained() {
        let executor = ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![Publisher])))
            .with_registered_policies([("publisher".into(), ToolPolicy::read_only())]);
        let output = executor
            .execute(input(
                "publisher",
                "{}",
                ToolPolicy::read_only()
                    .retain_metadata(MetadataRetention::none().key(Receipt::KEY)),
            ))
            .await
            .unwrap();
        let json = serde_json::to_string(&output).unwrap();
        assert!(json.contains("receipt-77"));
        assert!(!json.contains("credential"));

        let output = executor
            .execute(input("publisher", "{}", ToolPolicy::read_only()))
            .await
            .unwrap();
        let json = serde_json::to_string(&output).unwrap();
        assert!(!json.contains("receipt-77"));
    }

    #[tokio::test]
    async fn guarded_tool_runs_once_and_redelivery_returns_the_stored_result() {
        let calls = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(InMemoryGuardStore::new());
        let executor =
            ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![Counting(calls.clone())])))
                .with_registered_policies([(
                    "counting".into(),
                    ToolPolicy::interrupt_on_uncertain(),
                )])
                .with_guard(Some(store.clone()));
        let policy = ToolPolicy::interrupt_on_uncertain();
        let first = executor
            .execute(input(
                "counting",
                r#"{"a":1,"b":{"x":2,"y":[{"c":3,"d":4}]}}"#,
                policy.clone(),
            ))
            .await
            .unwrap();
        let second = executor
            .execute(input(
                "counting",
                r#"{"b":{"y":[{"d":4,"c":3}],"x":2},"a":1}"#,
                policy.clone(),
            ))
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(text(&first), "1");
        assert_eq!(text(&second), "1");
        assert_eq!(second.result.disposition, ToolDisposition::Success);

        for upgraded in [
            ToolExecutor::new(Arc::new(ToolSet::default())),
            ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![Counting(calls.clone())])))
                .with_registered_policies([(
                    "counting".into(),
                    ToolPolicy::read_only().implementation_version("2"),
                )]),
        ] {
            let recovered = upgraded
                .with_guard(Some(store.clone()))
                .execute(input(
                    "counting",
                    r#"{"a":1,"b":{"x":2,"y":[{"c":3,"d":4}]}}"#,
                    policy.clone(),
                ))
                .await
                .unwrap();
            assert_eq!(text(&recovered), "1");
            assert_eq!(recovered.result.disposition, ToolDisposition::Success);
        }

        // Different arguments under the same logical key cannot reuse the claim.
        let mismatch = executor
            .execute(input(
                "counting",
                r#"{"a":1,"b":{"x":2,"y":[{"c":3,"d":5}]}}"#,
                policy.clone(),
            ))
            .await
            .unwrap();
        let result = mismatch.result;
        assert_eq!(result.disposition, ToolDisposition::Interrupted);
        assert_eq!(
            result.interruption.unwrap().reason,
            InterruptionReason::ClaimMismatch
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn guard_rejects_a_different_tool_or_retained_policy() {
        for change_tool in [true, false] {
            let store = Arc::new(InMemoryGuardStore::new());
            let policy = ToolPolicy::interrupt_on_uncertain();
            let stored_policy = if change_tool {
                policy.clone()
            } else {
                policy
                    .clone()
                    .retain_metadata(MetadataRetention::none().key("receipt"))
            };
            store
                .claim(ClaimRequest {
                    key: key(),
                    tool_name: if change_tool { "other" } else { "counting" }.into(),
                    arguments_digest: argument_digest("{}"),
                    policy: stored_policy,
                    attempt: AttemptMetadata::default(),
                    claim_token: "owner".into(),
                })
                .await
                .unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let executor =
                ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![Counting(calls.clone())])))
                    .with_guard(Some(store));
            let result = executor
                .execute(input("counting", "{}", policy))
                .await
                .unwrap()
                .result;
            assert_eq!(
                result.interruption.unwrap().reason,
                InterruptionReason::ClaimMismatch
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn held_claim_is_reported_as_interrupted_without_running_the_tool() {
        let calls = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(InMemoryGuardStore::new());
        let policy = ToolPolicy::interrupt_on_uncertain();
        store
            .claim(ClaimRequest {
                key: key(),
                tool_name: "counting".into(),
                arguments_digest: argument_digest("{}"),
                policy: policy.clone(),
                attempt: AttemptMetadata {
                    backend_execution_id: "dead-worker".into(),
                    activity_attempt: Some(1),
                },
                claim_token: "dead".into(),
            })
            .await
            .unwrap();
        let executor =
            ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![Counting(calls.clone())])))
                .with_guard(Some(store));
        let output = executor
            .execute(input("counting", "{}", policy))
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(output.is_error);
        assert!(text(&output).contains("may have happened"));
        let result = output.result;
        assert_eq!(result.disposition, ToolDisposition::Interrupted);
        assert_eq!(
            result.interruption.unwrap().reason,
            InterruptionReason::ClaimHeld
        );
    }

    #[tokio::test]
    async fn new_execution_requires_exact_safety_version_and_registration() {
        let calls = Arc::new(AtomicUsize::new(0));
        let policies = [
            ToolPolicy::read_only(),
            ToolPolicy::idempotent(),
            ToolPolicy::interrupt_on_uncertain(),
            ToolPolicy::application_managed(),
        ];
        for stored in &policies {
            for current in &policies {
                if stored.safety() == current.safety() {
                    continue;
                }
                let executor =
                    ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![Counting(calls.clone())])))
                        .with_registered_policies([("counting".into(), current.clone())])
                        .with_guard(Some(Arc::new(InMemoryGuardStore::new())));
                assert!(matches!(
                    executor
                        .execute(input("counting", "{}", stored.clone()))
                        .await,
                    Err(ToolExecutionFailure::Configuration(_))
                ));
            }
        }
        for registration in [
            None,
            Some(ToolPolicy::read_only().implementation_version("2")),
        ] {
            let executor =
                ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![Counting(calls.clone())])))
                    .with_registered_policies(
                        registration.map(|policy| ("counting".into(), policy)),
                    );
            assert!(matches!(
                executor
                    .execute(input("counting", "{}", ToolPolicy::read_only()))
                    .await,
                Err(ToolExecutionFailure::Configuration(_))
            ));
        }
        let executor =
            ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![Counting(calls.clone())])))
                .with_registered_policies([("counting".into(), ToolPolicy::read_only())]);
        let mut changed_runtime = input("counting", "{}", ToolPolicy::read_only());
        changed_runtime.runtime_identity = Some("another-planner/projection".into());
        assert!(matches!(
            executor.execute(changed_runtime).await,
            Err(ToolExecutionFailure::Configuration(_))
        ));
        let mut missing_policy =
            serde_json::to_value(input("counting", "{}", ToolPolicy::read_only())).unwrap();
        missing_policy.as_object_mut().unwrap().remove("policy");
        assert!(serde_json::from_value::<ToolActivityInput>(missing_policy).is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn ambiguous_claim_or_settlement_never_authorizes_another_effect() {
        use rig::wasm_compat::WasmBoxedFuture;
        struct LostAck {
            inner: InMemoryGuardStore,
            claim: bool,
        }
        impl InvocationGuardStore for LostAck {
            fn claim(
                &self,
                request: ClaimRequest,
            ) -> WasmBoxedFuture<'_, Result<ClaimOutcome, GuardError>> {
                Box::pin(async move {
                    let outcome = self.inner.claim(request).await?;
                    if self.claim {
                        Err(GuardError::Store("claim acknowledgement lost".into()))
                    } else {
                        Ok(outcome)
                    }
                })
            }
            fn settle(
                &self,
                key: &LogicalCallKey,
                token: &str,
                result: DurableToolResult,
            ) -> WasmBoxedFuture<'_, Result<SettleOutcome, GuardError>> {
                let (key, token) = (key.clone(), token.to_owned());
                Box::pin(async move {
                    self.inner.settle(&key, &token, result).await?;
                    Err(GuardError::Store("settlement acknowledgement lost".into()))
                })
            }
            fn get(
                &self,
                key: &LogicalCallKey,
            ) -> WasmBoxedFuture<'_, Result<Option<ClaimRecord>, GuardError>> {
                self.inner.get(key)
            }
        }
        for lost_claim in [true, false] {
            let calls = Arc::new(AtomicUsize::new(0));
            let store = Arc::new(LostAck {
                inner: InMemoryGuardStore::new(),
                claim: lost_claim,
            });
            let policy = ToolPolicy::interrupt_on_uncertain();
            let executor =
                ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![Counting(calls.clone())])))
                    .with_registered_policies([("counting".into(), policy.clone())])
                    .with_guard(Some(store.clone()));
            assert!(matches!(
                executor
                    .execute(input("counting", "{}", policy.clone()))
                    .await,
                Err(ToolExecutionFailure::Guard(_))
            ));
            let recovered = ToolExecutor::new(Arc::new(ToolSet::default()))
                .with_guard(Some(store))
                .execute(input("counting", "{}", policy))
                .await
                .unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), usize::from(!lost_claim));
            assert_eq!(
                recovered.result.disposition,
                if lost_claim {
                    ToolDisposition::Interrupted
                } else {
                    ToolDisposition::Success
                }
            );
        }
    }

    #[tokio::test]
    async fn guarded_policy_without_a_store_fails_closed() {
        let executor = ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![Counting(Arc::new(
            AtomicUsize::new(0),
        ))])));
        let error = executor
            .execute(input(
                "counting",
                "{}",
                ToolPolicy::interrupt_on_uncertain(),
            ))
            .await
            .unwrap_err();
        assert!(matches!(error, ToolExecutionFailure::Configuration(_)));
        assert!(error.to_string().contains("guard store"));
    }
}
