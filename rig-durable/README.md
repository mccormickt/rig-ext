# rig-duroxide

Drive Rig 0.41's serializable `AgentRun` state machine inside a Duroxide 0.1
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

Rig 0.41 does not expose enough state to convert an already-built `rig::Agent`.
The durable builder therefore captures the model and tools before Rig makes
them private. It currently covers preambles, completion settings, history,
tools, tool choice, maximum turns, streaming, approvals, checkpoints, and child
agents. Rig memory, retrieval, hooks, structured output, and custom per-run
`ToolContext` values are not yet mirrored. Activity-backed Rig tools currently
receive an empty `ToolContext`; context-dependent tools need an explicit
activity route.

Configuration and retry policies are captured at registration time rather than
serialized. Tool calls are checked against the exact advertised and
`ToolChoice`-allowed names, and unknown calls fail closed. Treat changes to tool
definitions, routing, prompts, retry policy, or control flow as orchestration
version changes. Keep every version needed by live histories registered; replay
must observe the same configuration and activity names. Activities themselves
must be idempotent because retries can repeat I/O.

Timeouts add durable timer events, and worker tags are part of an activity's
event identity. Adding or changing either for a live orchestration version will
break replay determinism. Each completion request also contains the complete
conversation and is recorded in history, so stored history grows quadratically
with the number of turns.

## Streaming completion

`CompletionMode::Streaming` selects a separate stable streaming activity;
blocking completion remains the compatibility default. The activity consumes
`CompletionModel::stream` to EOF and returns a provider-neutral, serde-tagged
transcript. Orchestration deterministically replays it through Rig 0.41's
`StreamedTurnAssembler` and `AgentRun::streamed_turn`. The transcript preserves
text, reasoning, complete and delta tool calls, internal and provider IDs,
unknown items, final usage, message ID, and Rig's final aggregate. Invalid tools
fail closed, malformed deltas fail, absent usage uses Rig's zero sentinel, and
a streamed completion counts as one checkpoint operation.

Duroxide 0.1.30 activities return one final result. Tokens become durably
visible to orchestration only after the model activity completes; this does not
provide exactly-once live token delivery. Live at-least-once side channels are
out of scope. See `examples/streaming_agent.rs` for an API-key-free SQLite
example.

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
