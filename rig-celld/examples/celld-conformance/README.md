# rig-celld conformance fixture

This Worker runs the storage contract against a real celld 0.4 Durable Object SQL database. It uses a deterministic two-dimensional embedding model and no external provider.

Build and start it:

```sh
worker-build --release
celld dev
```

In another shell, run CRUD, upsert, multi-embedding retrieval, ranking, logical cursor, orphan isolation, idempotent garbage collection, forced catalog-switch rollback, and concurrent request checks:

```sh
python3 conformance.py
```

Stop and restart `celld dev` without removing `.celld/dev`, then verify that the catalog and vectors reopen:

```sh
python3 conformance.py --verify-reopen
```

The fixture is an application crate because Durable Object classes and routes do not belong in `rig-celld`.
