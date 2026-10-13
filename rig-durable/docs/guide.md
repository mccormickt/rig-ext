# Durable agent guide

See the [crate overview](../README.md) for backend selection and checks.

## Durable Objects

One SQLite Durable Object owns one agent session. The application owns the
`#[durable_object]` class, routes, authentication, and model credentials. Keep
one `durable_object::Engine` per object and pass its ID as the session ID:

```rust,ignore
let storage = rig_celld::CellStorage::from(state);
let engine = rig_durable::durable_object::Builder::new(model)
    .preamble("Use the calculator for arithmetic.")
    .tool_with(Add, ToolOptions::default().policy(ToolPolicy::read_only()))
    .build(storage.clone(), storage, object_id)?;

let receipt = engine.submit(SubmitInput::new("req-1", "What is 20 + 22?")).await?;
let response = engine.wait(&receipt.request_id).await?;
```

`submit` commits admission and arms an alarm. It does not wait for an answer.
`wait` drives the session and returns `Option<DurableResponse>`; `None` means
the request is not answered yet. `result` reads a committed answer without
driving. `status` exposes receipts, pending approvals, and retry deadlines.
Call `approve` or `deny` with the exact approval ID. Delegate the object's
alarm handler to `engine.alarm()`. `close` stops admission and drains work
already accepted. A failed prompt closes the session and cancels queued work.

The engine commits model intent before provider I/O, then commits the model
turn. It commits all tool intents before it starts the concurrent tool batch.
Each result commits separately; final outcomes retain model emission order.
After a restart, an incomplete model call runs again. An incomplete tool call
runs again only when its stored and registered replay policies agree and permit
repetition. Otherwise the model receives an `Interrupted` result. An
implementation-version change fails the session closed. The SQL tool-call
table is the invocation guard; no separate guard store is needed.

The object uses the `Logical` contract. Approval resolves the whole tool batch
before execution. Submissions received during I/O only commit to the queue; one local
driver owns the active turn. There is no post-tool steering.

Schema-versioned SQL tables hold the append-only transcript, chunked run
checkpoints, tool intents, and submission ledger. Checkpoints replace the
active `AgentRun`; past completion requests are not retained. Model context
comes from the transcript and compaction cutoff. Compaction runs between
prompts and uses the same validation as the native backends. Mutations reload
SQL inside a synchronous transaction, so an uncertain write cannot leave an
in-memory candidate visible. Use a new object ID for a new session.

Retry deadlines are persisted. Short waits use `worker::Delay`; the single
alarm tracks the earliest deadline or a 30-second recovery heartbeat. The
alarm handler explicitly re-arms after errors. Idle alarms do not recur.
The driver yields between operations after 60 seconds. Tools and model calls
must still fit the platform's invocation and CPU limits. Tool worker tags,
sub-orchestrations, and explicit per-call timeouts are not supported.
Streaming completion commits after EOF; it is not a live token feed.

Checkpoints and results use 512 KiB chunks. Individual transcript messages and
tool records are capped at 1 MiB. `session_history_max_bytes` caps each full
checkpoint, result, and transcript (8 MiB by default), not only model context.
The submission ledger retains receipts within its 48 KiB limit. Results remain
available for the life of the object. Start another object when a limit is
reached; compaction does not delete the audit transcript.

Build the core, backend, and API-key-free example:

```sh
rustup target add wasm32-unknown-unknown
cargo check -p rig-durable --no-default-features --target wasm32-unknown-unknown
cargo check -p rig-durable --no-default-features --features durable-object --target wasm32-unknown-unknown
cargo build -p durable-object-example --target wasm32-unknown-unknown
cargo test -p rig-durable --no-default-features --features durable-object --lib durable_object
```

The [example](../examples/durable-object/README.md) runs on celld or Wrangler
without `sqlite-vec`. Native conformance tests use the same engine with
`rusqlite`; production uses `rig-celld/storage` and `transactionSync()`.

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

The `submit` update admits a prompt under a client `request_id` (see
[Sessions](#sessions)) and returns `TemporalSubmission` once the prompt
answers: the receipt and, while the session retains it, the `DurableResponse`
with ordered tool dispositions. A rejected request fails the update from its
validator, without a history event, with a message that starts with the
`SubmissionError` reason (`conflict`, `busy`, `closed`, `ledger_full`). The
`receipt`, `ledger`, `result`, and `tool_outcomes` queries read the same
state; `snapshot` reports the compaction cutoff, queued submissions, and the
last compaction error.

Temporal sessions continue as new at an idle boundary when the service suggests
it. Conversation history, the compaction record, the submission ledger, queued
submissions, retained results, and queued steering are carried into the new
run. `session_history_max_bytes` limits the serialized transcript payload
(1 MB by default); prompts that exceed it are rejected and a completed turn
that crosses it fails the session.

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

`TemporalAgent` inputs use the `Logical` invocation contract (see
[Tool replay safety](#tool-replay-safety)). `register` returns `TemporalAgentError`
when a tool policy needs an invocation guard that was not supplied.

The ignored live suite covers happy-path model and tool activities, retries,
stable invocation identity, exhausted retries, approval and denial, long-lived
history, idle steering, closing, payload limits, registration validation,
guarded and idempotent writes across an activity timeout, submission
deduplication, follow-up and reject-if-busy admission during a tool round,
completed-prompt compaction, a failed summary, ordered tool dispositions, and
a failed submission closing the session. Configure a server
with the standard Temporal environment variables before running it. The live
continuation test also needs a disposable server with a low continuation
threshold (default address `http://localhost:7234`, overridden by
`TEMPORAL_CONTINUATION_ADDRESS`):

```bash
temporal server start-dev --port 7234 --headless \
  --dynamic-config-value limit.historyCount.suggestContinueAsNew=12
```

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

Drive Rig's serializable `AgentRun` state machine inside a Duroxide
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

Rig does not expose enough state to convert an already-built `rig::Agent`.
The durable builder therefore captures the model and tools before Rig makes
them private. It currently covers preambles, completion settings, history,
tools, tool choice, maximum turns, streaming, approvals, checkpoints, and child
agents. Rig memory, retrieval, hooks, structured output, and arbitrary per-run
`ToolContext` values are not yet mirrored. Activity-backed Rig tools receive a
durable `ToolInvocation` in their context.

Under the default `Legacy` contract, configuration and retry policies are
captured at registration time rather than serialized. Under the `Logical`
contract, each new run records a configuration snapshot in its input and
carries it across continue-as-new; see
[Tool replay safety](#tool-replay-safety). Tool calls are checked against the
exact advertised and `ToolChoice`-allowed names, and unknown calls fail closed.
Treat changes to tool definitions, routing, prompts, retry policy, or control
flow as orchestration version changes. Keep every version needed by live
histories registered; replay must observe the same configuration and activity
names. Activities themselves must be idempotent because retries can repeat
I/O. Use `ToolInvocation` as the idempotency key for side effects.

Timeouts add durable timer events, and worker tags are part of an activity's
event identity. Adding or changing either for a live orchestration version will
break replay determinism. Each completion request also contains the complete
conversation and is recorded in history, so stored history grows quadratically
with the number of turns.

## Sessions

All backends run a long-lived session as one durable execution that admits
prompts one at a time. `SubmitInput` carries the client's `request_id`, the
message, and a `SubmissionMode`:

| Mode | Session idle | Session busy |
|---|---|---|
| `FollowUp` (default) | runs now | queued; runs after the active prompt answers |
| `RejectIfBusy` | runs now | rejected with `SubmissionError::Busy` |

The session keeps a `SubmissionLedger` of receipts keyed by `request_id`.
A retried request receives its original `Submission` in every state, including
after the answer; the same `request_id` with a different message or mode is
rejected with `Conflict`. Deduplication runs before the busy and closed checks.
Each admitted submission gets the next `prompt_index`, which is the
`submission_id` that tool calls carry in their `LogicalCallKey`. The ledger is
capped at 48 KiB by default (`submission::DEFAULT_LEDGER_MAX_BYTES`); a session whose ledger
is full rejects new requests with `LedgerFull` instead of evicting receipts.
Request IDs contain 1–256 UTF-8 bytes. Failure text is capped at 256 bytes;
admission reserves the largest JSON encoding for every pending receipt's
terminal transition. Duroxide rejection retention is capped by count and bytes.
Oversized rejection identifiers use a SHA-256 representation. Receipts are
never evicted, and terminal receipt states do not change.

On the native backends, busy is evaluated at operation boundaries: a request
that arrives during a tool round is admitted or rejected after its results return. A queued
follow-up never enters the active prompt's tool round. Post-tool steering of an
active prompt and compaction of an active prompt stay out of scope.
Admitted queued work counts as busy, even before execution starts.

A failed prompt closes the session and cancels queued submissions. `close`
stops admission; admitted submissions and queued steering still run before the
session completes.

Sessions retain recent per-submission results for the `wait` and result
surfaces: 32 on Duroxide (`session::SESSION_RESULT_RETENTION`) and 16 on
Temporal (`temporal::SESSION_RESULT_RETENTION`). An older result is gone, but
its receipt stays `Answered`.
On Duroxide, a detailed result that cannot fit in the 60 KiB retention budget
is not written to KV. Its receipt stays `Answered`, `wait` returns
`ResultNotRetained`, and the full answer stays in the audit transcript.
Single-run `wait` and `wait_detailed` return the full orchestration response;
only the bounded tool outcomes are stored separately in KV.

```rust,ignore
let session = orchestrator.agent("calculator")?.open_session("support-42").await?;
let receipt = session.submit(SubmitInput::new("req-1", "What is 20 + 22?")).await?;
let detailed = session.wait(&receipt.request_id).await?;
let rejected = session
    .submit(SubmitInput::new("req-2", "urgent").mode(SubmissionMode::RejectIfBusy))
    .await;
```

The Duroxide `DurableSession::submit` returns on admission and `wait` returns
the answer; the Temporal `submit` update returns when the prompt answers.

### Compaction

`compaction(Compaction::new(policy, compactor))` bounds the active context a
session sends to the model with Rig's memory abstractions: a
`rig_memory::MemoryPolicy` (`SlidingWindowMemory`, `TokenWindowMemory`, or
your own) decides which transcript prefix leaves the active window, and a
`rig::memory::Compactor` folds that prefix, with the prior artifact as
`carry_over`, into a new artifact. This is the durable form of
`rig_memory::CompactingMemory`: the policy and compactor run in one activity
on the worker, and the workflow keeps only the watermark, so the
`CompactionRecord` (cutoff, policy version, encoded artifact, artifact
message, input count) survives replay and continue-as-new.

After every completed prompt the session runs one compaction activity over
the full transcript. When the policy demotes nothing beyond the applied
cutoff the round is a no-op. Otherwise the workflow checks that the artifact
message followed by the kept transcript is canonical (`validate_canonical`)
before it applies the new cutoff; a cutoff that splits a tool exchange is
rejected. Later rounds compact only the newly demoted messages. The audit
transcript keeps every message, and `SessionResult`/
`TemporalAgentSessionResult` carry both the transcript and the record.

The compactor's artifact must implement `Serialize` and `DeserializeOwned`,
because the workflow retains it between rounds. `ModelCompactor` is the
crate-owned compactor: one model call summarizes the evicted window and its
`ModelSummary` artifact records the text and usage. `rig_memory::TemplateCompactor`
does not qualify, because its `TextSummary` artifact has no serde
support and no public constructor.

```rust,ignore
use rig_durable::{Compaction, ModelCompactor};
use rig_memory::SlidingWindowMemory;

let agent = DurableAgent::builder("support", model.clone())
    .compaction(
        Compaction::new(
            SlidingWindowMemory::last_messages(40),
            ModelCompactor::new(model.clone()),
        )
        .version("summary-v1"),
    )
    .build()?;
```

A failed round leaves the context in place and does not fail the session; the
session runs again after the next prompt grows the transcript. Compaction
runs between prompts: a Duroxide session processes it as the next command,
and a Temporal session treats it as busy for submissions while the
`prompt` update waits for it. A worker without a registered `Compaction`
fails the activity closed. The recorded expected version must match the worker
and any carry-over artifact before policy or model execution. A mismatch is a
nonfatal compaction failure. Applied outputs also require that version.
Temporal carries the last attempted transcript length and error across
continue-as-new, so unchanged input does not retry a failed round.
Compaction is a mandatory gate before the next queued or steering prompt.
Compaction bounds the model context;
continue-as-new and checkpoints bound event history. Both backends serialize
the full transcript. Temporal's `session_history_max_bytes` applies to that
transcript, not only the compacted active context.

### Detailed results

`PromptResponse` stays the result of `prompt`, `DurableRun::wait`, and the
Temporal `TemporalAgentWorkflow::run` result. The crate-owned
`DurableResponse` wraps it with ordered `ToolOutcome`s: one per tool call, in
dispatch order, with `prompt_index`, 1-based model `turn`, `call_index`, the
`ToolDisposition`, and whether it was `Retained` from the activity output or
`Derived` from the legacy error flag. Read it from
`DurableRun::wait_detailed`/`result_detailed`, `DurableSession::wait`, the
Temporal `submit` update, or the Temporal `tool_outcomes` and `result`
queries. Tool results with equal content and different dispositions stay
distinct.

## Streaming completion

Streaming completion is supported by Duroxide and Durable Objects. Temporal
uses blocking completion only.

`CompletionMode::Streaming` selects a separate stable streaming activity;
blocking completion is the default. The activity consumes
the model's `stream` to EOF and returns a provider-neutral, serde-tagged
transcript. The transcript records each Rig part event (start, text,
reasoning, arguments, end, and unknown items) and ends with the
`CompletionResponse` that Rig folds from the stream. Orchestration applies that
response deterministically through `AgentRun::streamed_turn`. Calls to tools
that are not registered or not allowed by `ToolChoice` fail closed. Rig drops a
streamed call that never receives a name. A streamed completion counts as one
checkpoint operation.

Duroxide activities return one final result. Tokens become durably
visible to orchestration only after the model activity completes; this does not
provide exactly-once live token delivery. Live at-least-once side channels are
out of scope. See `examples/streaming_agent.rs` for an API-key-free SQLite
example.

## Architecture

The backend-neutral driver plans model, tool, and done effects. It owns
completion requests, `AgentRun` transitions, prompt-scoped approval identities
and denials, and tool-result correlation. The native adapters use the same
follow-up-turn steering semantics. Backend adapters schedule activities, wait
for durable control messages, expose status, and apply backend retry and
checkpoint rules.

```text
Rig model + ToolSet
        │
        ▼
shared durable driver ── AgentRun + pure policy
        │
        ├── Duroxide adapter ── orchestration, events, timers, continue-as-new
        │
        ├── Temporal adapter ── workflow, signals, queries, activity policies
        │
        └── Durable Object engine ── SQL checkpoints, tool intents, alarms
```

This boundary keeps the public API Rig-first and avoids a generic async runtime
trait. Hooks that perform durable control belong at this driver boundary. Rig's
current high-level `AgentRunner` hooks are not replayed because the backends
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
logical completion. Duroxide does not support a parent-side retry policy,
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

## Durable code-mode workflows

Enable the optional native `codemode` feature, then use
`DurableAgentBuilder::code_mode(...)` or `TemporalAgent::code_mode(...)`. Registered
tools and `.sub_agent(...)` children remain callable from JavaScript, but the
coordinator model sees only `codemode({ code })`. This prevents direct calls
from bypassing the script boundary.
Use `.code_mode(Default::default())` for default limits without naming the
dependency. Add `rig-codemode` as a direct dependency to configure its `Limits`.

```rust,ignore
let coordinator = DurableAgent::builder("coordinator", main_model)
    .sub_agent("research", researcher)?
    .sub_agent("assess", assessor)?
    .sub_agent("review", reviewer)?
    .code_mode(rig_codemode::Limits::default())
    .build()?;
```

A script can use `Promise.all` to run researchers concurrently, inspect their
answers, then call a reviewer only when those answers require review. Every
nested Rig tool is a separate durable activity; every durable sub-agent is a
child orchestration. Script planning runs in a pure QuickJS activity. After
each selected result, the next planning activity persists its delivery position
and reconstructs the VM from the ordered journal. The shared scheduler retains
pending operations, so a fast answer can start a follow-up before a slow sibling
finishes. Ready-result ties use stable admission order. `Promise.all` retains
input ordering; `Promise.race` uses recorded host delivery order, not physical
timestamps. A restart reuses completed activities and children rather than
rerunning the whole script as one tool activity. Interrupted activities still
have at-least-once delivery and need the existing replay-safety policies.
Under `Logical`, nested Rig tools receive a key scoped to the script's child
workflow instance and call ordinal. Keys differ across script calls and remain
stable across activity attempts. Temporal scopes keys to the execution chain,
parent prompt, model turn, outer script call, and nested ordinal.
Guard stores and per-tool retries still apply.

Race losers are not cancelled. The host completes all admitted calls, including
queued calls, before returning the final report. Late replies are `Discarded`.
This prevents returning a successful script while its external effects remain
untracked, but a slow loser can delay the outer tool's return.

The outer tool returns a JSON `ExecutionReport`. Script exceptions are reports
with `status: "script_error"`, not rollback. Exhausted nested activity or child
failures reject their JavaScript promises; scripts can catch them or use `.raw`.
Schema errors cannot start an effect. Replay divergence fails the workflow
closed. Duroxide status records include admitted/completed counts. Both backends
retain separate child histories for inspection.

Planning failures stop admission but do not cancel admitted work. The scheduler
drains active and queued calls, then persists their final journal in one last
planning activity. That activity cannot admit new effects. The original error
fails the workflow on both backends; it is not coordinator tool feedback.

The [`dynamic_workflow`](../examples/dynamic_workflow.rs) example runs without
an API key and prints the actual report received by the coordinator:

```sh
cargo run -p rig-durable --features codemode --example dynamic_workflow
cargo test -p rig-durable --features codemode --test facade code_mode
```

### Temporal

Enable only Temporal and code mode without pulling in Duroxide:

```toml
rig-durable = { path = "../rig-durable", default-features = false, features = ["temporal", "codemode"] }
```

Temporal child agents are registered independently on their own worker queues.
Calling `.sub_agent(...)` captures the child's configuration and queue:

```rust,ignore
let researcher = TemporalAgent::new(research_model);
let coordinator = TemporalAgent::new(main_model)
    .sub_agent("research", &researcher, "research-workers")?
    .tool(lookup)
    .code_mode(Default::default());
let input = coordinator.input("Research and check the result.");
researcher.register(&mut research_worker_options)?;
coordinator.register(&mut coordinator_worker_options)?;
// Start both workers, then start TemporalAgentWorkflow::run with input.
```

The live regression test uses mocked models, a blocked child, out-of-order
results, worker restart, and recorded-history replay. Start a local Temporal
development server and run:

```sh
cargo test -p rig-durable --no-default-features --features temporal,codemode \
  --test temporal temporal_code_mode -- --include-ignored
```

The public `rig_durable::codemode` module owns configuration, step inputs, and
the shared scheduler. Backend adapters supply durable planning and tool
operations. VM execution never runs inside backend workflow code.

### Boundaries

This integration supports native Duroxide and Temporal. `Date`, `Math.random`,
weak references, and finalizers are unavailable.
`wall_time` bounds each VM step, not the complete workflow. `max_in_flight`
bounds active nested operations across steps; sub-agents own limits for their
internal work. Await new calls or compose them in awaited promises before
returning. Nested human approval and child
conversation steering are not supported. Parent steering keeps its existing
follow-up-turn behavior; it does not modify a running script or child.

Treat enabling code mode, changing the nested catalog or policies, changing
limits, or upgrading the VM as an agent-version boundary. Keep each registered
version fixed while its histories live. Duroxide's parent configuration snapshot
does not snapshot nested script configuration; Temporal inputs record it.
Step inputs retain prior results,
so history grows with both the number of calls and their result sizes. This
feature does not add continue-as-new or bounded transcript storage to scripts.

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

## Tool replay safety

Both backends deliver tool activities at least once. A worker can lose its
lease or die after the external service accepted an effect and before the
activity recorded a result; the backend then redelivers the same attempt.
Retry policy decides how often a *returned* failure is retried. Replay safety
decides whether a redelivered attempt may run the tool again:

| `ReplaySafety` | Redelivered attempt | Requirement |
|---|---|---|
| `ApplicationManaged` (default) | Runs the tool again | The application owns idempotency. |
| `ReadOnly` | Runs the tool again | No external write. A durably recorded result is never replaced by a new read. |
| `Idempotent` | Runs the tool again with the same `LogicalCallKey` | The tool keys its effect on `ToolInvocation::logical_key`, so attempts have one effect. |
| `InterruptOnUncertain` | Does not run the tool | An `InvocationGuardStore` shared by every worker that can receive the activity. |

Declare a policy per tool. An explicit `retryable = false` on a returned
`ToolExecutionError` is always honored and records the error without a retry,
whatever the replay safety:

```rust,ignore
let agent = DurableAgent::builder("payments", model)
    .invocation_contract(InvocationContract::Logical)
    .invocation_guard(Arc::new(InMemoryGuardStore::new()))
    .tool_with(LookupBalance, ToolOptions::default().policy(ToolPolicy::read_only()))
    .tool_with(
        Transfer,
        ToolOptions::default()
            .retry(RetryPolicy::new(3))
            .policy(ToolPolicy::interrupt_on_uncertain().implementation_version("2")),
    )
    .build()?;

let agent = TemporalAgent::new(model)
    .invocation_guard(guard)
    .tool_with_policy(Transfer, ToolPolicy::interrupt_on_uncertain());
```

### Invocation contracts

`InvocationContract` selects the activity payloads an agent version produces:

- `Legacy` uses minimal invocation payloads. Every tool runs with
  `ApplicationManaged` safety; the builder rejects any other policy.
- `Logical` adds the `LogicalCallKey`, attempt metadata, and the tool policy to
  each payload, and records results as `DurableToolResult` with a
  `ToolDisposition`.

The Duroxide builder defaults to `Legacy`; `TemporalAgent` inputs default to
`Logical`. Choose the contract before starting an execution and keep it fixed
for that execution's histories.

### Logical call identity

Under `Logical`, `ToolInvocation::logical_key` identifies one tool call across
retries, redelivery, and continue-as-new: the logical execution ID (the
Duroxide instance ID or the Temporal namespace and first execution run ID), the submission ID
(`prompt-{index}`), the model turn, and the call index. The same tool called
from two prompts gets two keys; a retry keeps its key. `ToolInvocation::attempt`
carries the physical execution ID and activity attempt for diagnosis only.
Provider tool-call IDs can repeat across prompts and are not an idempotency key.
Independent Temporal starts that reuse a closed workflow ID get different keys.
Continue-as-new retains the execution-chain identity and ledger identity.

### Invocation guard

`InterruptOnUncertain` tools claim their logical key in the guard store before
any I/O, and settle the claim with the result before returning it. A
redelivered attempt that finds a `Claimed` record returns an `Interrupted`
result with `InterruptionReason::ClaimHeld` instead of running the tool. A
record whose arguments, policy, or implementation version differ returns
`ClaimMismatch`. Settlement is conditional on the claim token, so a worker
that resumes after its lease expired cannot overwrite a newer result.

The guard gives up automatic progress after a crash between claim and I/O:
the call stays interrupted until an operator or a tool-specific status lookup
resolves it. Registration fails with `GuardStoreRequired` when a tool needs a
guard and none was supplied. `InMemoryGuardStore` is process-local and only
suitable for tests and single-worker deployments. Run
`guard::conformance::run` against any other store before using it.

### Result dispositions

The model always receives canonical `ToolResultContent`. An interrupted call is
reported to the model as text that states the effect may have happened and was
not repeated, so the model does not treat it as a definite failure. The
activity output retains the exact `ToolDisposition` (`Success`, `Error`,
`Refused`, `Skipped`, `Interrupted`) and only the `ToolResultContext` keys the
tool's `MetadataRetention` approved, within its size limit. Inbound
`ToolContext` values are never persisted.

### Configuration snapshots

Under `Logical`, a new top-level Duroxide run started through the facade stores
a `ConfigSnapshot` in its input:
ordered tool definitions, routes, retry settings, approval flags, tool
policies, completion settings, and checkpoint policy. Continuations carry it
forward. On replay, the worker resolves the snapshot against its registration
and fails the run closed when a snapshot tool is missing, routed differently,
or registered with another implementation version. The tool executor refuses a
payload recorded for another implementation version for the same reason. A
worker whose live settings drifted in other ways replays the retained
configuration.

**Child agents and raw orchestration starts do not have this automatic input
snapshot guarantee.** Child starts retain the versioned `AgentInput` payload
and resolve configuration from that registered child version until their first
checkpoint. That checkpoint carries the resolved snapshot. Keep each child
registration unchanged for its live histories; deploy changed configuration
under a new child orchestration version. Raw callers can attach an explicit
snapshot with `AgentInput::with_snapshot`.

Approval requests bind to the logical call, the final argument digest, and the
tool implementation version. A decision recorded in history is reused after a
restart without a new request, and a decision recorded for other arguments does
not release a rewritten call.
