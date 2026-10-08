# Durable Object agent

This Worker uses a deterministic mock model and an approval-gated addition
tool. It needs no API key. The application owns the Durable Object class;
`rig-durable` owns SQL checkpoints, submissions, approvals, and recovery.
The Wrangler migration uses `new_sqlite_classes`. No `sqlite-vec` extension
or compatibility flag is needed.

Build from this directory:

```sh
rustup target add wasm32-unknown-unknown
cargo install worker-build
npm install --global esbuild
worker-build --release
```

Run with celld 0.6.0 or later (tested with 0.6.1):

```sh
curl -fsSL https://celld.dev/install.sh | sh
celld dev --clean
```

In another shell, test alarm-driven execution, admission, and approvals:

```sh
python3 conformance.py
```

Stop and restart `celld dev` without `--clean`. Test retained results and a
pending approval across that restart:

```sh
python3 conformance.py --verify-reopen
```

The same bundle can run with `npx wrangler dev --port 9876`. Each first path
segment selects an object: `/<session>/submit`, `/status`, `/approval`,
`/result?request_id=<id>`, `/transcript`, and `/close`. Mutations use POST;
reads use GET. Submit bodies use the serde format of `SubmitInput`; approval
bodies use `ApprovalDecision`. The conformance script contains examples.

The example does not authenticate requests. Add authentication and access
control before deployment. Keep one engine per object. Initialization is
retried after storage errors; request handlers read only committed state.
