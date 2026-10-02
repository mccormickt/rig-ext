# rig-celld conformance fixture

This Worker runs the storage contract against a real Durable Object SQL database on celld 0.6.0 or later. It is tested with celld 0.6.1. It uses a deterministic two-dimensional embedding model and no external provider.

Build and start it. The [`rig-celld` README](../../README.md#examples) lists the prerequisites:

```sh
worker-build --release
celld dev --clean
```

In another shell, run CRUD, upsert, multi-embedding retrieval, ranking, logical cursor, orphan isolation, idempotent garbage collection, forced catalog-switch and upsert rollback, nested commit and rollback, inner rollback inside a committed transaction, batch rollback, search during a concurrent write, and concurrent request checks:

```sh
python3 conformance.py
```

Stop and restart `celld dev` without removing `.celld/dev`, then verify that the catalog and vectors reopen:

```sh
python3 conformance.py --verify-reopen
```

The fixture is an application crate because Durable Object classes and routes do not belong in `rig-celld`.
