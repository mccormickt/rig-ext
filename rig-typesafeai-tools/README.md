# rig-typesafeai-tools

Expose a host-configured `rig-typesafeai` 0.44 evaluation model as the Rig
`DynamicTool` named `jev_evaluate`. There is no production dependency on
a script engine, an agent runner, or a specific HTTP transport.

```rust,ignore
use rig_typesafeai::JevConfig;
use rig_typesafeai_tools::evaluation_tool;

// The host supplies its credential and HTTP transport.
let model = JevConfig::new(token).connect(http).evaluation();
let tool = evaluation_tool(model);
```

Register the tool with a Rig agent or a `rig-codemode::DynamicToolDispatcher`.
Native code-mode scripts can then compose it with other approved tools:

```js
const result = await tools.jev_evaluate({
  state: { message: "The service is down for every customer." },
  questions: {
    urgent: { type: "noul", instructions: "Does this need immediate review?" }
  }
});
return { needsReview: result.answers.urgent.noul >= 0.8 };
```

## Contract

- Input is `{state, questions}`. State is a string, object, or array. Question
  IDs are nonempty; question types are native `choice`, `score`, and `noul`.
  Instructions and criteria support the structured values
  accepted by `rig-typesafeai`.
- Both direct and dispatcher calls validate the input schema and construct a
  `DynamicQuery` before evaluation. The configured model is responsible for the
  `Evaluate` contract, including answer validation; the Jev client implements it.
- Output is exactly `{answers, model, usage}`. Answers retain distributions,
  confidence, and score legends. Missing usage is `null`. Request IDs and raw
  provider responses are excluded. Usage is what a successful evaluation
  reported, not a complete billing ledger for failed or cancelled requests.
- Invalid arguments produce `invalid_args`. Evaluation failures produce a fixed,
  non-retryable `other` error without provider bodies or error-chain details.
- Native models require `Send + Sync`. `wasm32-unknown-unknown` accepts local
  state and futures. This does not add a Wasm JavaScript executor.

## Host responsibilities

Keep credentials, endpoint/model configuration, authorization, timeouts, response
size limits, call/concurrency budgets, and result policies in the host. Direct
`DynamicTool::execute` validates arguments but does not apply dispatcher policy.
Nested code-mode calls use the supplied dispatcher's policies, not agent hooks.
The adapter adds no retries; a configured transport may have its own retry policy.
Dropping a future does not prove provider work stopped.

The adapter has no application-specific question-count or question-ID-length
ceiling. Hosts that need these limits must check them in call policy, along with
argument size and other budgets. Confidence is not authorization.

An external executor must supply its own safe guest-to-host bridge,
projection, accounting, and engine limits. Public report constructors do not
provide those guarantees. No runtime-accounting abstraction is included here.

## Verification

```sh
cargo test --locked -p rig-typesafeai-tools
cargo check --locked -p rig-typesafeai-tools --target wasm32-unknown-unknown
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
  cargo test --locked -p rig-typesafeai-tools --target wasm32-unknown-unknown --test tool
```

Tests use controlled HTTP responses through the real Jev client. Native tests
also compose dependent evaluations through QuickJS and check host policy. Wasm
tests include a model holding `Rc` across an asynchronous wait. These tests do
not measure live model accuracy, calibration, latency, or billing.
