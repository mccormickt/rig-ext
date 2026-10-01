# rig-durable

`rig-durable` supports two independent durable execution backends:

| Cargo feature | Backend | Default |
|---|---|---|
| `duroxide` | Duroxide orchestration and activities | yes |
| `sqlite` | Duroxide SQLite provider | yes |
| `temporal` | Temporal workflows and activities | no |

Use only Temporal with `default-features = false, features = ["temporal"]`.
Use both backends with `features = ["temporal"]`.

## Temporal

The Temporal integration runs Rig's `AgentRun` state machine as a workflow.
Each model request and each tool call is a Temporal activity. Tool calls from
one model turn are scheduled concurrently and their results retain model
emission order. Activity retries and timeouts use Temporal policies. Human
approval uses the generated `TemporalAgentWorkflow::approval` signal, and
current state is available from the `TemporalAgentWorkflow::status` query.

For a long-lived agent, start `TemporalAgentSessionWorkflow`. Each `prompt`
update runs a durable agent turn against workflow-owned conversation history.
The workflow rejects concurrent prompt updates. A `steer` signal received while
a turn is active is queued as the next prompt and runs before that update
returns. A steer signal received while the session is idle starts a new turn.
Steering does not cancel a model or tool activity that is already in flight.
Send `close` when the session is no longer needed; the workflow drains messages
accepted before the close and waits for active handlers before it completes.
An activity or agent failure closes the session. Completed turns commit history
before the next queued turn starts, so a later failure does not discard them.

Temporal sessions continue as new at an idle boundary when the service suggests
it. Conversation history, queued steering, and the next prompt identity are
carried into the new run. `session_history_max_bytes` limits the serialized
conversation payload (1 MB by default); prompts that exceed it are rejected and
a completed turn that crosses it fails the session.

```rust,ignore
use rig_durable::temporal::{TemporalAgent, TemporalAgentSessionWorkflow};
use temporalio_client::{
    WorkflowExecuteUpdateOptions, WorkflowSignalOptions, WorkflowStartOptions,
};

let input = agent.session_input(Vec::new());
let handle = client.start_workflow(
    TemporalAgentSessionWorkflow::run,
    input,
    WorkflowStartOptions::new("rig-agents", "calculator-session-1").build(),
).await?;
let response = handle.execute_update(
    TemporalAgentSessionWorkflow::prompt,
    "What is 20 + 22?".into(),
    WorkflowExecuteUpdateOptions::default(),
).await?;
handle.signal(
    TemporalAgentSessionWorkflow::close,
    (),
    WorkflowSignalOptions::default(),
).await?;
```

```rust,ignore
use rig_durable::temporal::{TemporalAgent, TemporalAgentWorkflow};
use temporalio_client::{
    WorkflowGetResultOptions, WorkflowStartOptions,
};
use temporalio_sdk::WorkerOptions;

let agent = TemporalAgent::new(model)
    .preamble("Use the calculator tool for arithmetic.")
    .tool(Add);
let input = agent.input("What is 20 + 22?");

let mut worker_options = WorkerOptions::new("rig-agents").build();
agent.register(&mut worker_options)?;

// Create and run a Temporal Worker with worker_options, then start the agent:
let handle = client.start_workflow(
    TemporalAgentWorkflow::run,
    input,
    WorkflowStartOptions::new("rig-agents", "calculator-run-1").build(),
).await?;
let response = handle
    .get_result(WorkflowGetResultOptions::default())
    .await?;
```

Register one `TemporalAgent` per worker task queue. The model and `ToolSet` stay
inside the activity worker and are not serialized into workflow history. The
serializable agent configuration is part of workflow input, so replay sees the
same preamble, tool schemas, model settings, timeout, and retry limit after a
deployment. Activities are at-least-once. Rig tools can read
`ToolInvocation` from `ToolContext` and use its stable execution, turn, and
call identity as an idempotency key. `prompt_index` distinguishes tool calls
from separate prompts in one long-lived session while retries retain the same
complete identity.

`temporal_retrying_tool` demonstrates a tool activity that reports a retryable
provider failure on its first attempt and succeeds on its second:

```bash
cargo run -p rig-durable --no-default-features --features temporal \
  --example temporal_retrying_tool
```

The ignored live suite covers happy-path model and tool activities, retries,
stable invocation identity, exhausted retries, approval and denial, long-lived
history, idle steering, closing, and payload limits. Configure a server with
the standard Temporal environment variables before running it:

```bash
TEMPORAL_ADDRESS=temporal.example.com:443 TEMPORAL_TLS=true \
  cargo test -p rig-durable --no-default-features --features temporal \
  --test temporal -- --ignored --test-threads=1
```

An approval tool pauses before any tool activity is scheduled:

```rust,ignore
let agent = TemporalAgent::new(model).approval_tool(DeleteAccount);

let request = match handle.query(
    TemporalAgentWorkflow::status,
    (),
    Default::default(),
).await? {
    TemporalAgentStatus::Approval { request } => request,
    status => panic!("expected approval, got {status:?}"),
};
handle.signal(
    TemporalAgentWorkflow::approval,
    ApprovalDecision::Approve { approval_id: request.approval_id },
    Default::default(),
).await?;
```

## Duroxide

Drive Rig 0.43's serializable `AgentRun` state machine inside a Duroxide 0.1
orchestration. The orchestration performs no provider or tool I/O; all such work
is registered as activities.

The normal API is Rig-first: define an agent with Rig models and tools, register
it with an `AgentOrchestrator`, and call `prompt`.

```rust,ignore
let calculator = DurableAgent::builder("calculator", model)
    .version("1.0.0")?
    .preamble("Use the calculator tool for arithmetic.")
    .tool(Add)
    .build()?;

let orchestrator = AgentOrchestrator::sqlite("sqlite://agents.db?mode=rwc")
    .await?
    .register(calculator)?
    .start()
    .await?;

let answer = orchestrator
    .agent("calculator")?
    .prompt("What is 20 + 22?")
    .await?;
```

`start` returns a `DurableRun` handle for approvals, status, cancellation, and
steering. `run.steer(message)` queues a follow-up turn. As with Temporal, it
does not cancel model or tool work that is already in flight; the queued turn
runs before the orchestration completes. The call returns only after the
orchestration durably acknowledges that exact steering command. It returns
`SteeringNotAccepted` if the run completes first. The acknowledgement survives
checkpoints in custom status, and checkpoint policy also applies before a
steered text-only turn starts.

```text
Orchestration (deterministic, zero IO)          Activities (all IO)
──────────────────────────────────────          ───────────────────────────
AgentRun::next_step()
  ├─ CallModel{prompt, history, turn}
  │    build CompletionRequest ────────────────► completion activity
  │                                               model completion or stream-to-EOF
  │    run.model_response/streamed_turn ◄──────── durable JSON result
  │
  ├─ CallTools{calls}
  │    ctx.join([...]) ────────────────────────► one activity per tool call
  │                                               ToolSet::execute / user activity
  │    run.tool_results(results) ◄─────────────── ToolActivityOutput (JSON)
  │
  └─ Done(PromptResponse) ──► orchestration output
```

`DurableAgentBuilder` turns each Rig tool into activity-backed durable execution.
`AgentOrchestrator` owns the Duroxide runtime, registries, provider, and client;
named agent and run handles hide orchestration names and wire inputs. Explicit
run IDs are scoped to the selected agent version and support idempotent starts
and reconnection; for one scoped ID, the first submitted input wins. The provider, runtime,
client, raw registry merge methods, `ToolCatalog`, and route constructors remain
available for advanced integration with existing Duroxide applications.

Rig 0.43 does not expose enough state to convert an already-built `rig::Agent`.
The durable builder therefore captures the model and tools before Rig makes
them private. It currently covers preambles, completion settings, history,
tools, tool choice, maximum turns, streaming, approvals, checkpoints, and child
agents. Rig memory, retrieval, hooks, structured output, and arbitrary per-run
`ToolContext` values are not yet mirrored. Activity-backed Rig tools receive a
durable `ToolInvocation` in their context.

Configuration and retry policies are captured at registration time rather than
serialized. Tool calls are checked against the exact advertised and
`ToolChoice`-allowed names, and unknown calls fail closed. Treat changes to tool
definitions, routing, prompts, retry policy, or control flow as orchestration
version changes. Keep every version needed by live histories registered; replay
must observe the same configuration and activity names. Activities themselves
must be idempotent because retries can repeat I/O. Use `ToolInvocation` as the
idempotency key for side effects.

Timeouts add durable timer events, and worker tags are part of an activity's
event identity. Adding or changing either for a live orchestration version will
break replay determinism. Each completion request also contains the complete
conversation and is recorded in history, so stored history grows quadratically
with the number of turns.

## Streaming completion

`CompletionMode::Streaming` selects a separate stable streaming activity;
blocking completion remains the compatibility default. The activity consumes
the model's `stream` to EOF and returns a provider-neutral, serde-tagged
transcript. The transcript records each Rig 0.43 part event (start, text,
reasoning, arguments, end, and unknown items) and ends with the
`CompletionResponse` that Rig folds from the stream. Orchestration applies that
response deterministically through `AgentRun::streamed_turn`. Calls to tools
that are not registered or not allowed by `ToolChoice` fail closed. Rig drops a
streamed call that never receives a name. A streamed completion counts as one
checkpoint operation.

Duroxide 0.1.30 activities return one final result. Tokens become durably
visible to orchestration only after the model activity completes; this does not
provide exactly-once live token delivery. Live at-least-once side channels are
out of scope. See `examples/streaming_agent.rs` for an API-key-free SQLite
example.

## Architecture

The backend-neutral driver owns Rig policy: completion request construction,
`AgentRun` transitions, prompt-scoped approval identities and denials, and
tool-result correlation. Both adapters use the same follow-up-turn steering
semantics. Backend adapters only schedule activities, wait for durable control
messages, expose status, and apply backend retry and checkpoint rules.

```text
Rig model + ToolSet
        │
        ▼
shared durable driver ── AgentRun + pure policy
        │
        ├── Duroxide adapter ── orchestration, events, timers, continue-as-new
        │
        └── Temporal adapter ── workflow, signals, queries, activity policies
```

This boundary keeps the public API Rig-first and avoids a generic async runtime
trait. Hooks that perform durable control belong at this driver boundary. Rig's
current high-level `AgentRunner` hooks are not replayed because both adapters
drive `AgentRun` directly. A future Rig effect API can replace this narrow seam
without changing backend scheduling.

## Checkpointing

`CheckpointConfig` can continue an active run as a new Duroxide execution after
a deterministic number of completed durable operations. A completed model
activity and a complete tool batch each count as one operation. Checkpointing is
disabled by default; use `CheckpointPolicy::Every(NonZeroU32)` to enable it.
The continuation contains the complete serialized Rig `AgentRun`, generation,
and model-turn metadata. The per-history-window operation count resets after a
checkpoint. This preserves global turn limits,
usage, pending tool calls, approval IDs, and queued approval events. An optional
target orchestration version may be selected for the next execution.

Continue-as-new bounds each Duroxide event-history window; it does not bound the
serialized `AgentRun` or full conversation payload, which continue to grow.
Checkpoint configuration and threshold changes are orchestration versioning
boundaries. The envelope has a crate-owned format version, but Rig's `AgentRun`
serde representation is also a persistence compatibility boundary. Keep the
required crate and orchestration versions available while continuations live.
See `examples/checkpointed_agent.rs` for an API-key-free SQLite example.

Each multi-tool turn fans out through `ctx.join`. Every tool keeps its own retry
policy and optional worker tag, while results remain in model emission order.
Sub-orchestration calls participate in the same fan-out and use Duroxide's
replay-safe automatic child IDs. Their raw JSON arguments are the child input;
JSON child output becomes structured tool content and other output becomes
text. A child may itself use `continue_as_new`, and the parent waits for its
logical completion. Duroxide 0.1.30 does not support a parent-side retry policy,
timeout, or worker tag on these calls, so such configuration is rejected before
scheduling. Child workflows own the activity retries for their work. See
`examples/sub_orchestration_tool.rs` for an API-key-free example.

A durable agent can be exposed directly as a parent agent's tool. The public
tool schema is `{ "prompt": string }`; private `AgentInput` and
`PromptResponse` conversion stays inside the orchestration:

```rust,ignore
let researcher = DurableAgent::builder("researcher", research_model)
    .description("Research a question and return a concise report.")
    .tool(WebSearch)
    .build()?;
let assistant = DurableAgent::builder("assistant", main_model)
    .sub_agent("research", researcher)?
    .build()?;
```

## Human approval

Approval is enabled automatically when a builder tool requires it:

```rust,ignore
let ops = DurableAgent::builder("ops", model)
    .tool_with(DeleteAccount, ToolOptions::default().require_approval())
    .build()?;
let run = orchestrator.agent("ops")?.start("Delete account 123").await?;
let request = run.next_approval().await?;
run.approve(&request).await?;
let response = run.wait().await?;
```

Approval queue identity is fixed across all registered versions of one logical
agent. Approval-enabled agents cannot currently be nested as sub-agents because
Duroxide's automatic child instance ID is not exposed to the parent run handle;
the builder rejects that composition instead of allowing an unreachable wait.

While waiting, custom status is JSON with `phase: "approval"` and a complete
`ApprovalRequest`. `DurableRun::approve` and `deny` hide queue and event
handling. The stable default queue is
`rig-duroxide-approvals`. Decisions are FIFO and persistent: malformed or
non-matching IDs are consumed and ignored, including stale decisions ahead of
the matching one. Approval IDs are deterministic but opaque; copy the exact ID
from the request rather than constructing it.

All flagged calls in one model turn are resolved in emission order before any
tool activity in that turn is scheduled. After that barrier, approved and
unflagged calls fan out together. A denial is returned to the model as a
correlated synthetic tool result, so it can recover. Flagged tools fail closed
if approval is disabled, and preresolved Rig calls bypass both approval and
execution. See `examples/human_approval.rs` for a local, API-key-free flow.
