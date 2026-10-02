# celld durable Rig agent

This example maps each agent name to one SQLite-backed Durable Object. The object:

1. loads the exact Rig chat history from SQLite;
2. retrieves five relevant prior turns through `SqliteVecIndex`;
3. runs a Rig OpenAI agent and embeds the completed turn;
4. persists the turn, its embedding, and the messages of the turn in one `transactionSync()` call.

If a step of the write fails, celld rolls back all of it. The next request then sees the state before the turn.

Each message is one row in the `messages` table, in commit order. A turn appends only its own messages. Requests to one agent can overlap while they wait for the provider, so two turns can run on the same prior history. Both turns are kept, and each turn's messages stay together. A turn does not see a concurrent turn in its own prompt.

This is an architecture example, not a public API. Add authentication and authorization before deployment so callers cannot read or modify another agent's conversation. Also add history compaction and request idempotency before using long-running agents in production.

## Prerequisites

```sh
rustup target add wasm32-unknown-unknown
cargo install worker-build
curl -fsSL https://celld.dev/install.sh | sh
npm install --global esbuild
```

Use celld 0.6.0 or later. The turn transaction contains the nested transaction of the index upsert, and earlier releases reject that nesting.

The workspace pins workers-rs 0.8.3 because it shares the `wasm-streams` 0.5 ABI used by Rig 0.43 and reqwest 0.13. See the [workspace constraints](../../../README.md#constraints).

## Build

Run this command from this directory:

```sh
worker-build --release
```

Do not deploy the raw Cargo WASM artifact. `worker-build` creates `build/worker/shim.mjs`, which is the configured Worker entry point.

## Run locally

Put the OpenAI key in a `.dev.vars` file beside `wrangler.jsonc` instead of adding it to `wrangler.jsonc`. The repository `.gitignore` excludes this file:

```sh
printf '%s\n' 'OPENAI_API_KEY=sk-...' > .dev.vars
chmod 600 .dev.vars
celld dev
```

celld 0.5.0 and later read `.dev.vars` in `celld dev` only, and reload the application when the file changes. `celld deploy` does not send the file to a fleet. celld removed `CELLD_VAR_<NAME>` and `CELLD_VARS_FILE`, and a node that has one of them set does not start.

celld treats these values as normal Worker variables, not as a dedicated encrypted secret binding. It also writes them into the local deployment record under `.celld/dev`. Protect the file and that directory. On Cloudflare, use `wrangler secret put OPENAI_API_KEY`.

Send a message:

```sh
curl --fail-with-body \
  --request POST \
  --header 'content-type: application/json' \
  --data '{"prompt":"My preferred editor is Helix."}' \
  http://127.0.0.1:9876/agents/demo/messages
```

The response contains the answer and durable turn number:

```json
{"answer":"...","turn":1}
```

Requests that use another agent name are routed to another cell and do not share memory. Local state remains in `.celld/dev` between normal restarts. Use `celld dev --clean` to start from an empty local state.

## Configure models

The Wrangler variables select the default models and embedding width:

- `OPENAI_COMPLETION_MODEL` defaults to `gpt-4o-mini`.
- `OPENAI_EMBEDDING_MODEL` defaults to `text-embedding-3-small`.
- `OPENAI_EMBEDDING_DIMENSIONS` defaults to `512`.
- `OPENAI_BASE_URL` selects an OpenAI-compatible endpoint. The default is the OpenAI API.

The configured embedding width is part of the vec0 table schema. Do not change it for an existing agent database without rebuilding or migrating that vector table.

Use a new Durable Object class and migration tag for an incompatible vector width or storage backend. Copy logical documents through the library's `(id, generation)` cursor; do not copy vec0 row IDs.
