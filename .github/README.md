# CI and releases

## CI coverage

`ci.yml` runs on pull requests, pushes to `main`, merge queues, and manual
requests. Jobs use Linux and stable Rust. They install Protobuf, clang/lld,
OpenSSL development files, and the `wasm32-unknown-unknown` target. Replay jobs
use Node 22 and the tools pinned by `scripts/workers/install.sh`. No provider,
Cloudflare, or crates.io credentials are needed for CI.

| Check | Coverage |
| --- | --- |
| Native workspace | All 11 workspace members: default and all-feature tests, examples, doctests, Clippy on all targets, and formatting. |
| Native feature isolation | Each public library without unrelated workspace features. A2A TLS choices; MCP; contracts-only and QuickJS code mode; Duroxide, SQLite guards, Temporal, and Durable Objects. Tests also run with Temporal/code mode and Durable Object/code mode independently of the default backend. |
| WASM | All applicable public libraries and four Worker examples. Separate storage, MCP, dynamic-worker, replay, and Durable Object feature sets. Dependency-tree checks reject unrelated backends. TypeSafe tools and local code-mode dispatch/policy tests run in Node through wasm-bindgen. |
| Replay runtime | Build both Cargo-generated replay artifacts; check hashes, Cargo file selection, pins, and notices; run the ignored Dynamic Worker and WASM memory-bound tests on local workerd; build the Durable Object Worker. |
| Standalone examples | Test and lint the separately locked Rig-native prototype. Compile the independent Loader fixture for WASM and the replay fixture for native and WASM. These two fixtures have no committed lockfile, so their first check resolves dependencies; subsequent checks use that lock. |
| Packaging | Package and compile the six public crate archives together. Cargo uses a temporary local registry for unpublished workspace dependencies. This checks archive creation, dependency versions, file selection, and default-feature archive builds without an upload. Publishing also retains Cargo's archive build verification. |

`rig-a2a` is native-only. Native QuickJS, Duroxide, Temporal, and the code-mode
`rig-rmcp` adapter are not WASM backends. The separate `rig-celld` MCP adapter
supports WASM. Do not replace the target-specific checks with a WASM workspace
`--all-features` build. The guest runtime manifest is generated from
`rig-codemode/runtime/guest/Cargo.toml.in` and is built by the replay asset build.

CI does not run the ignored live Temporal server tests, the ignored MCP runtime
test, or the orb-managed Durable Object crash/restart suite. Run
`scripts/workers/conformance.sh` in an orb for the latter. The MCP runtime test
currently copies its fingerprint from `runtime/fingerprint.json`, but the
asset build writes `runtime/build/fingerprint.json`; that test needs a separate
fix before it can run from a clean checkout. Local workerd tests do not prove
Cloudflare production CPU or subrequest enforcement.

For the workspace checks locally, install the pinned tools, then run
`scripts/workers/check.sh`. The optional `native` and `wasm` arguments select
the two CI portions. `RUST_TEST_THREADS=1` matches CI.

## Publication eligibility and order

As checked on 2026-11-09, none of the six public crate names exists on
crates.io. Confirm the names again before the first release; an available name
does not reserve it. The external Rig 0.45 dependencies are published.

All six libraries are enabled explicitly in `release-plz.toml`. Examples,
conformance fixtures, the generated guest, and test tooling are not release
products. Their manifests retain `publish = false`. Newly added crates are not
released until added to the allowlist.

Normal and build dependencies determine this order:

1. `rig-codemode-core` and `rig-celld` can publish independently.
2. `rig-codemode` requires both crates, including `rig-celld` for its optional
   WASM backend. Optional and target-specific dependencies still need registry
   versions.
3. `rig-durable` requires `rig-codemode` and `rig-celld`.

`rig-a2a` and `rig-typesafeai-tools` can publish independently of that chain.
Release-plz determines the dependency order and waits for registry availability.
The configuration file's package order is not a publish script.

The path-only code-mode **dev-dependencies** in `rig-celld` and
`rig-typesafeai-tools` are intentional. Cargo removes them from the published
manifest. They are not registry prerequisites and do not create a publication
cycle. Keep running integration tests from this workspace checkout.

The public libraries inherit `workspace.package.version`. Review shared version
and internal dependency changes in the release PR together. If the core version
changes, refresh the generated guest's lockfile using the manifest template and
the new core path/version, then copy that lock back to
`rig-codemode/runtime/guest/Cargo.lock`. The nested build uses `--locked`; the
replay CI job must pass on the release PR before it is merged.

## Owner setup

These are repository and registry owner actions. Do not paste credentials into
an issue, PR, or Amp transcript. Use GitHub **Settings → Secrets and variables →
Actions**, and the environment settings described below.

1. Enable Actions and permit the actions used by these workflows. Keep the
   default `GITHUB_TOKEN` permissions read-only; the publish job requests
   Contents write access. The separate token below creates release PRs.
   Add `Native workspace`, `Native feature isolation`, `WASM and replay runtime`,
   and `Standalone examples` as required checks on `main` once GitHub has
   registered their first run. Require review of release PRs; do not auto-merge
   them before their checks pass.
2. Create `RELEASE_PLZ_TOKEN` as a repository Actions secret. Use a fine-grained
   GitHub PAT limited to `mccormickt/rig-ext`, with **Contents: read/write** and
   **Pull requests: read/write**, from an account that can push a PR branch.
   An organization may require approval of this token. A GitHub App installation
   token with these permissions is an alternative, but needs an App token step
   in the workflow. Do not substitute `GITHUB_TOKEN`: its PR pushes do not start
   the normal PR CI workflow.
3. Create a GitHub environment named `crates-io`. Limit it to `main` and set
   required reviewers before the workflow is merged. The publishing job uses
   this environment; the release-PR job does not. A read-only job checks for a
   merged release PR before requesting environment approval. Normal main pushes
   prepare the release PR without opening the publishing environment.
4. Create a crates.io API token from the intended crate owner account. It must
   permit **publish-new** for the first publication and **publish-update** for
   later versions, restricted to the six crate names where the token UI permits
   this. Add it as the `CARGO_REGISTRY_TOKEN` secret in the `crates-io`
   environment. Use an expiry and rotate it. No owner-management or yank scope
   is needed. Confirm the account has a verified email, has accepted the registry
   terms, and can own the names. Add other maintainers after the initial
   publication if needed.
5. Review and merge the release PR. It prepares versions and changelogs. Only
   after CI passes on the main push does the publishing workflow run. The
   built-in `GITHUB_TOKEN` has Contents write and PR read access in this job for
   tags, GitHub releases, and release-PR detection. No extra GitHub publishing
   token is required.

`release_always = false` means the workflow setup commit does not publish the
initial versions. Release-plz publishes only a commit associated with a merged
`release-plz-…` PR. Do not use that branch prefix for unrelated work. The release
job checks out the exact successful CI head, not a later `main` commit. Fork and
PR workflow completions cannot enter the privileged jobs. No CI artifacts are
downloaded into them. Release-PR updates are serialized separately; publishing
runs and main-branch CI runs are not canceled by a newer main push. Older PR
checks are canceled when a new commit is pushed to that PR.

For a failed partial release, inspect crates.io and GitHub tags first. Published
versions cannot be overwritten. Re-run the failed publishing job only after
the cause is fixed; release-plz skips already published versions. Do not delete
tags or publish a different archive under an existing version.

After the first publication, an owner can switch to
[crates.io trusted publishing](https://crates.io/docs/trusted-publishing).
Configure each crate for repository `mccormickt/rig-ext`, workflow
`release-plz.yml`, and environment `crates-io`; grant `id-token: write` to the
release job and remove its `CARGO_REGISTRY_TOKEN` environment entry. Release-plz
handles the OIDC exchange itself. New crate names cannot use trusted publishing
for their first release, so this workflow uses a registry token for bootstrap.

References: [release-plz setup](https://release-plz.dev/docs/github/quickstart)
and [configuration](https://release-plz.dev/docs/config).
