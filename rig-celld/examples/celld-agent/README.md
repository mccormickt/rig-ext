# celld durable Rig agent

This example maps each agent name to one SQLite-backed Durable Object. The object:

1. loads the exact Rig chat history from SQLite;
2. retrieves five relevant prior turns through `SqliteVecIndex`;
3. runs a Rig OpenAI agent;
4. persists the completed turn, its embedding, and the updated history.

This is an architecture example, not a public API. Add authentication and authorization before deployment so callers cannot read or modify another agent's conversation. Also add history compaction and request idempotency before using long-running agents in production.

## Prerequisites

```sh
rustup target add wasm32-unknown-unknown
cargo install worker-build
curl -fsSL https://celld.dev/install.sh | sh
```

The workspace pins workers-rs 0.8.3 because it shares the `wasm-streams` 0.5 ABI used by Rig 0.43 and reqwest 0.13. See the [workspace constraints](../../../README.md#constraints).

## Build

Run this command from this directory:

```sh
worker-build --release
```

Do not deploy the raw Cargo WASM artifact. `worker-build` creates `build/worker/shim.mjs`, which is the configured Worker entry point.

## Run locally

Pass the OpenAI key as a celld variable override instead of adding it to `wrangler.jsonc`:

```sh
CELLD_VAR_OPENAI_API_KEY='sk-...' celld dev
```

For a file-based local setup, create a file outside the repository and restrict its permissions:

```sh
printf '%s\n' 'OPENAI_API_KEY=sk-...' > "$HOME/.config/rig-celld.env"
chmod 600 "$HOME/.config/rig-celld.env"
CELLD_VARS_FILE="$HOME/.config/rig-celld.env" celld dev
```

celld treats these values as normal Worker variables, not as a dedicated encrypted secret binding. Protect the process environment or variables file. On Cloudflare, use `wrangler secret put OPENAI_API_KEY`.

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

Requests that use another agent name are routed to another cell and do not share memory. Local state remains in `.celld/dev` between normal restarts.

## Configure models

The Wrangler variables select the default models and embedding width:

- `OPENAI_COMPLETION_MODEL` defaults to `gpt-4o-mini`.
- `OPENAI_EMBEDDING_MODEL` defaults to `text-embedding-3-small`.
- `OPENAI_EMBEDDING_DIMENSIONS` defaults to `512`.

The configured embedding width is part of the vec0 table schema. Do not change it for an existing agent database without rebuilding or migrating that vector table.

Use a new Durable Object class and migration tag for an incompatible vector width or storage backend. Copy logical documents through the library's `(id, generation)` cursor; do not copy vec0 row IDs.
