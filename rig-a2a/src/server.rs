//! In-process A2A executor for an ordinary Rig agent.
//!
//! The host supplies authentication, task ownership, admission control, and the
//! A2A task store. Context history is process-local and serialized per tenant
//! and context. Cancellation stops dispatch at the next Rig effect boundary;
//! it waits for an in-flight effect and its cleanup, rather than aborting it.
//! Effects must finish and must not leave untracked background work. A terminal
//! A2A task alone is not a suspension barrier. Close admission outside this
//! executor, drain the protocol handler, then check [`RigExecutor::active`].

use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use a2a::{A2AError, Artifact, Message, Part, Role, StreamResponse, Task, TaskState, TaskStatus};
use a2a_server::{AgentExecutor, ExecutorContext};
use futures::{StreamExt, stream::BoxStream};
use rig_agent::agent::{
    Agent, AgentHook, DispatchAction, DispatchEvent, HookContext, RunSettled, StepEventKind,
};
use rig_core::message::Message as RigMessage;
use tokio::sync::{Mutex as AsyncMutex, watch};

type ContextKey = (Option<String>, String);
type History = Arc<AsyncMutex<Vec<RigMessage>>>;

#[derive(Clone)]
pub struct RigExecutor {
    agent: Agent,
    contexts: Arc<Mutex<HashMap<ContextKey, History>>>,
    executions: Arc<Mutex<HashMap<ContextKey, Arc<Execution>>>>,
}

struct Execution {
    cancel: AtomicBool,
    done: watch::Sender<bool>,
    messages: Mutex<Vec<RigMessage>>,
}

struct ExecutionHook(Arc<Execution>);

struct ExecutionGuard {
    executor: RigExecutor,
    key: ContextKey,
    execution: Arc<Execution>,
}

impl Drop for ExecutionGuard {
    fn drop(&mut self) {
        self.executor
            .executions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.key);
        self.execution.done.send_replace(true);
    }
}

impl AgentHook for ExecutionHook {
    fn observes(&self, _: StepEventKind) -> bool {
        true
    }

    async fn on_dispatch(&self, _: &HookContext, _: DispatchEvent<'_>) -> DispatchAction {
        if self.0.cancel.load(Ordering::SeqCst) {
            DispatchAction::stop("A2A cancellation")
        } else {
            DispatchAction::proceed()
        }
    }

    async fn on_run_settled(&self, _: &HookContext, event: RunSettled<'_>) {
        if let Some(messages) = event.messages {
            *self.0.messages.lock().unwrap_or_else(|e| e.into_inner()) = messages.to_vec();
        }
    }
}

impl RigExecutor {
    pub fn new(agent: Agent) -> Self {
        Self {
            agent,
            contexts: Arc::default(),
            executions: Arc::default(),
        }
    }

    /// Number of accepted executions, including those waiting for a context.
    /// This is an observation, not an atomic admission/suspension fence.
    pub fn active(&self) -> usize {
        self.executions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// Full canonical Rig history, including tool calls and committed results.
    pub async fn history(&self, tenant: Option<String>, context: &str) -> Vec<RigMessage> {
        let history = self
            .contexts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(tenant, context.to_owned()))
            .cloned();
        match history {
            Some(history) => history.lock().await.clone(),
            None => Vec::new(),
        }
    }
}

impl AgentExecutor for RigExecutor {
    fn execute(
        &self,
        ctx: ExecutorContext,
    ) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        let this = self.clone();
        async_stream::stream! {
            let message = match ctx.message.as_ref() {
                Some(message) if message.parts.iter().all(|p| matches!(p.content, a2a::PartContent::Text(_))) => message,
                _ => {
                    yield Err(A2AError::invalid_params("only text messages are supported"));
                    return;
                }
            };
            let prompt = message.parts.iter().filter_map(|p| match &p.content {
                a2a::PartContent::Text(text) => Some(text.as_str()),
                _ => None,
            }).collect::<Vec<_>>().join("\n");
            let key = (ctx.tenant.clone(), ctx.task_id.clone());
            let execution = Arc::new(Execution {
                cancel: AtomicBool::new(false),
                done: watch::channel(false).0,
                messages: Mutex::new(Vec::new()),
            });
            this.executions.lock().unwrap_or_else(|e| e.into_inner()).insert(key.clone(), execution.clone());
            let guard = ExecutionGuard { executor: this.clone(), key, execution: execution.clone() };
            let history = this.contexts.lock().unwrap_or_else(|e| e.into_inner())
                .entry((ctx.tenant.clone(), ctx.context_id.clone())).or_default().clone();
            tracing::info!(task = %ctx.task_id, context = %ctx.context_id,
                message = %message.message_id, correlation = ?ctx.service_params.get("x-request-id"),
                "Rig execution admitted");
            yield Ok(status(&ctx, TaskState::Working, None));
            let mut history = history.lock().await;
            let result = if execution.cancel.load(Ordering::SeqCst) {
                None
            } else {
                Some(this.agent.prompt(prompt).history(history.clone()).max_turns(8)
                    .add_hook(ExecutionHook(execution.clone())).await)
            };
            history.extend(execution.messages.lock().unwrap_or_else(|e| e.into_inner()).iter().cloned());
            drop(history);
            let canceled = execution.cancel.load(Ordering::SeqCst);
            drop(guard);
            let (state, text) = match result {
                _ if canceled => (TaskState::Canceled, None),
                Some(Ok(response)) => (TaskState::Completed, Some(response.to_string())),
                Some(Err(error)) => (TaskState::Failed, Some(error.to_string())),
                None => (TaskState::Canceled, None),
            };
            tracing::info!(task = %ctx.task_id, context = %ctx.context_id, ?state, "Rig execution settled");
            yield Ok(status(&ctx, state, text));
        }.boxed()
    }

    fn cancel(&self, ctx: ExecutorContext) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        let execution = self
            .executions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(ctx.tenant.clone(), ctx.task_id.clone()))
            .cloned();
        async_stream::stream! {
            if let Some(execution) = execution {
                let mut done = execution.done.subscribe();
                execution.cancel.store(true, Ordering::SeqCst);
                while !*done.borrow_and_update() {
                    if done.changed().await.is_err() { break; }
                }
            }
            yield Ok(status(&ctx, TaskState::Canceled, None));
        }
        .boxed()
    }
}

fn status(ctx: &ExecutorContext, state: TaskState, text: Option<String>) -> StreamResponse {
    StreamResponse::Task(Task {
        id: ctx.task_id.clone(),
        context_id: ctx.context_id.clone(),
        status: TaskStatus {
            state,
            message: None,
            timestamp: None,
        },
        artifacts: text.map(|text| {
            vec![Artifact {
                artifact_id: a2a::new_artifact_id(),
                name: Some("result".into()),
                description: None,
                parts: Message::new(Role::Agent, vec![Part::text(text)]).parts,
                metadata: None,
                extensions: None,
            }]
        }),
        history: None,
        metadata: None,
    })
}
