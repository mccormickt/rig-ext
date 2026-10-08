//! The tool activity. It runs a Rig tool once per attempt and decides, from
//! the error's retryability and the tool policy, whether to fail the attempt
//! so the backend can schedule another one or to record the outcome.

use std::{collections::BTreeMap, sync::Arc};

use rig::tool::{ToolContext, ToolSet};
use sha2::{Digest, Sha256};

use crate::{
    activity_types::{ToolActivityInput, ToolActivityOutput},
    guard::{ClaimOutcome, ClaimRequest, ClaimState, InvocationGuardStore, SettleOutcome},
    identity::arguments_digest,
    policy::{ErrorHandling, ToolPolicy, error_handling},
    result::{DurableToolResult, InterruptionReason},
};

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

    /// Policies this worker registered, by tool name. An input whose policy
    /// names another implementation version is refused instead of run, so an
    /// upgraded worker cannot execute a call recorded for an older version.
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

    /// Run one attempt. `Err` fails the attempt so the backend may retry it;
    /// `Ok` records a terminal result, including recorded tool errors.
    pub async fn execute(&self, input: ToolActivityInput) -> Result<ToolActivityOutput, String> {
        let policy = input.policy.clone().unwrap_or_default();
        if let (Some(payload), Some(registered)) =
            (input.policy.as_ref(), self.registered.get(&input.name))
            && payload.version() != registered.version()
        {
            return Err(format!(
                "tool `{}` was recorded for implementation version `{}`, but this worker \
                 registered version `{}`",
                input.name,
                payload.version(),
                registered.version()
            ));
        }
        if policy.safety().requires_guard() {
            return self.execute_guarded(input, &policy).await;
        }
        self.execute_committed(input).await
    }

    /// Execute an intent already claimed by the backend's transaction.
    #[cfg_attr(not(feature = "durable-object"), allow(dead_code))]
    pub(crate) async fn execute_committed(
        &self,
        input: ToolActivityInput,
    ) -> Result<ToolActivityOutput, String> {
        let policy = input.policy.clone().unwrap_or_default();
        let result = self.run_tool(&input, &policy).await?;
        if let Some(error) = result.result.as_ref().and_then(|result| result.error())
            && error_handling(error, policy.safety()) == ErrorHandling::Retry
        {
            return Err(error.to_string());
        }
        Ok(ToolActivityOutput::from_result(&input.name, result))
    }

    async fn run_tool(
        &self,
        input: &ToolActivityInput,
        policy: &ToolPolicy,
    ) -> Result<DurableToolResult, String> {
        let mut context = ToolContext::new();
        context
            .insert(input.invocation.clone())
            .map_err(|error| error.to_string())?;
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
    ) -> Result<ToolActivityOutput, String> {
        let Some(guard) = &self.guard else {
            return Err(format!(
                "tool `{}` requires an invocation guard store, but the worker has none",
                input.name
            ));
        };
        let Some(key) = input.invocation.logical_key.clone() else {
            return Err(format!(
                "tool `{}` requires a logical call key, but the invocation has none",
                input.name
            ));
        };
        let digest = argument_digest(&input.arguments);
        let claim_token = uuid::Uuid::new_v4().to_string();
        let claim = ClaimRequest {
            key: key.clone(),
            tool_name: input.name.clone(),
            arguments_digest: digest.clone(),
            policy: policy.clone(),
            attempt: input.invocation.attempt.clone().unwrap_or_default(),
            claim_token: claim_token.clone(),
        };
        let result = match guard
            .claim(claim)
            .await
            .map_err(|error| error.to_string())?
        {
            ClaimOutcome::Created => {
                let result = self.run_tool(&input, policy).await?;
                match guard
                    .settle(&key, &claim_token, result.clone())
                    .await
                    .map_err(|error| error.to_string())?
                {
                    SettleOutcome::Settled => result,
                    SettleOutcome::Rejected => DurableToolResult::interrupted(
                        InterruptionReason::ClaimMismatch,
                        "the invocation claim changed before the result was recorded",
                    ),
                }
            }
            ClaimOutcome::Existing(record) => {
                if record.claim.key != key
                    || record.claim.tool_name != input.name
                    || record.claim.arguments_digest != digest
                    || record.claim.policy != *policy
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
                }
            }
        };
        Ok(ToolActivityOutput::from_result(&input.name, result))
    }
}

fn argument_digest(arguments: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(arguments) {
        Ok(value) => arguments_digest(&value)
            .unwrap_or_else(|_| format!("{:x}", Sha256::digest(arguments.as_bytes()))),
        Err(_) => format!("{:x}", Sha256::digest(arguments.as_bytes())),
    }
}

#[cfg(test)]
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
                invocation
                    .logical_key
                    .as_ref()
                    .map(LogicalCallKey::canonical)
                    .unwrap_or_default()
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

    fn input(name: &str, arguments: &str, policy: Option<ToolPolicy>) -> ToolActivityInput {
        ToolActivityInput {
            name: name.into(),
            arguments: arguments.into(),
            invocation: ToolInvocation {
                execution_id: "run-1:exec-1".into(),
                prompt_index: 4,
                turn: 2,
                call_index: 3,
                logical_key: Some(key()),
                attempt: Some(AttemptMetadata {
                    backend_execution_id: "exec-1".into(),
                    activity_attempt: Some(1),
                }),
            },
            policy,
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
        let legacy = ToolActivityInput {
            name: "invocation".into(),
            arguments: "{}".into(),
            invocation: ToolInvocation::legacy("run-1".into(), 4, 2, 3),
            policy: None,
        };
        let executor = ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![ReadInvocation])));
        let output = executor.execute(legacy).await.unwrap();
        assert_eq!(text(&output), "run-1:4:2:3:");

        let output = executor
            .execute(input("invocation", "{}", Some(ToolPolicy::idempotent())))
            .await
            .unwrap();
        assert_eq!(
            text(&output),
            "run-1:exec-1:4:2:3:v1;5:run-1;8:prompt-4;2;3"
        );
    }

    #[tokio::test]
    async fn legacy_payloads_are_byte_identical() {
        let legacy = ToolActivityInput {
            name: "add".into(),
            arguments: "{}".into(),
            invocation: ToolInvocation::legacy("run-1".into(), 0, 1, 0),
            policy: None,
        };
        assert_eq!(
            serde_json::to_string(&legacy).unwrap(),
            r#"{"name":"add","arguments":"{}","invocation":{"execution_id":"run-1","prompt_index":0,"turn":1,"call_index":0}}"#
        );
        let old_output: ToolActivityOutput =
            serde_json::from_str(r#"{"content":[{"type":"text","text":"3"}],"is_error":false}"#)
                .unwrap();
        assert!(old_output.result.is_none());
    }

    #[tokio::test]
    async fn equal_content_keeps_distinct_dispositions_through_execution() {
        let executor = ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![Disposition])));
        let cases = [
            ("success", ToolDisposition::Success, false),
            ("error", ToolDisposition::Error, true),
            ("refused", ToolDisposition::Refused, true),
        ];
        for (mode, disposition, is_error) in cases {
            let arguments = format!(r#"{{"mode":"{mode}"}}"#);
            let output = executor
                .execute(input(
                    "disposition",
                    &arguments,
                    Some(ToolPolicy::read_only()),
                ))
                .await
                .unwrap();
            let json = serde_json::to_string(&output).unwrap();
            let decoded: ToolActivityOutput = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded.is_error, is_error, "{mode}");
            assert_eq!(text(&decoded), "same words", "{mode}");
            assert_eq!(decoded.result.unwrap().disposition, disposition, "{mode}");
        }
    }

    #[tokio::test]
    async fn retry_decision_separates_retryability_from_replay_safety() {
        let executor = ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![Disposition])));
        let network = r#"{"mode":"network"}"#;
        let provider_final = r#"{"mode":"provider_final"}"#;
        let provider = r#"{"mode":"provider"}"#;

        // Retryable error, repeat permitted: the attempt fails.
        for policy in [
            ToolPolicy::default(),
            ToolPolicy::read_only(),
            ToolPolicy::idempotent(),
        ] {
            assert!(
                executor
                    .execute(input("disposition", network, Some(policy.clone())))
                    .await
                    .is_err()
            );
            assert!(
                executor
                    .execute(input("disposition", provider, Some(policy)))
                    .await
                    .is_err()
            );
        }
        // Explicit `retryable = false` is recorded even for a replay-safe tool.
        let output = executor
            .execute(input(
                "disposition",
                provider_final,
                Some(ToolPolicy::read_only()),
            ))
            .await
            .unwrap();
        assert_eq!(output.result.unwrap().disposition, ToolDisposition::Error);
        // Legacy payloads keep the old provider-error rule.
        assert!(
            executor
                .execute(input("disposition", provider, None))
                .await
                .is_err()
        );
        let output = executor
            .execute(input("disposition", provider_final, None))
            .await
            .unwrap();
        assert!(output.is_error);
        // A retryable error under a policy that never repeats is recorded.
        let guarded = ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![Disposition])))
            .with_guard(Some(Arc::new(InMemoryGuardStore::new())));
        let output = guarded
            .execute(input(
                "disposition",
                network,
                Some(ToolPolicy::interrupt_on_uncertain()),
            ))
            .await
            .unwrap();
        assert_eq!(output.result.unwrap().disposition, ToolDisposition::Error);
    }

    #[tokio::test]
    async fn only_approved_result_metadata_is_retained() {
        let executor = ToolExecutor::new(Arc::new(ToolSet::from_tools(vec![Publisher])));
        let output = executor
            .execute(input(
                "publisher",
                "{}",
                Some(
                    ToolPolicy::read_only()
                        .retain_metadata(MetadataRetention::none().key(Receipt::KEY)),
                ),
            ))
            .await
            .unwrap();
        let json = serde_json::to_string(&output).unwrap();
        assert!(json.contains("receipt-77"));
        assert!(!json.contains("credential"));

        let output = executor
            .execute(input("publisher", "{}", Some(ToolPolicy::read_only())))
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
                .with_guard(Some(store.clone()));
        let policy = Some(ToolPolicy::interrupt_on_uncertain());
        let first = executor
            .execute(input("counting", "{}", policy.clone()))
            .await
            .unwrap();
        let second = executor
            .execute(input("counting", "{}", policy.clone()))
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(text(&first), "1");
        assert_eq!(text(&second), "1");
        assert_eq!(second.result.unwrap().disposition, ToolDisposition::Success);

        // Different arguments under the same logical key cannot reuse the claim.
        let mismatch = executor
            .execute(input("counting", r#"{"extra":true}"#, policy.clone()))
            .await
            .unwrap();
        let result = mismatch.result.unwrap();
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
                .execute(input("counting", "{}", Some(policy)))
                .await
                .unwrap()
                .result
                .unwrap();
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
            .execute(input("counting", "{}", Some(policy)))
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(output.is_error);
        assert!(text(&output).contains("may have happened"));
        let result = output.result.unwrap();
        assert_eq!(result.disposition, ToolDisposition::Interrupted);
        assert_eq!(
            result.interruption.unwrap().reason,
            InterruptionReason::ClaimHeld
        );
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
                Some(ToolPolicy::interrupt_on_uncertain()),
            ))
            .await
            .unwrap_err();
        assert!(error.contains("guard store"));
    }
}
