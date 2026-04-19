# Codex Integration Running Log

**Plan:** `docs/specs/2026-04-18-codex-integration-plan.md`

---

## Frozen Header

| field | value |
| --- | --- |
| Base branch | `codex-integration` |
| Base worktree | `/home/ketan/project/gnomon/.worktrees/codex-integration` |
| Merge target | `main` |
| Final integration policy | `git merge --no-ff codex-integration` into `main` |
| GitHub issues | `#119` through `#125` |
| Cross-cutting fixture issue | `#121` lands incrementally with the protected work |

---

## Session Log

### 2026-04-18 — Orchestration Setup

Status: KEPT

Summary:

- Created the long-lived integration branch `codex-integration`.
- Created the linked base worktree at
  `/home/ketan/project/gnomon/.worktrees/codex-integration`.
- Added the long-lived orchestration plan and this running log to the base
  branch.
- Established the issue order, child branch naming scheme, and session-close
  handoff rules for future work.

Landed on base branch:

- `docs/specs/2026-04-18-codex-integration-plan.md`
- `docs/specs/2026-04-18-codex-integration-log.md`

Remaining high-level work:

- `#119` provider-aware source modeling and configuration
- `#120` explicit shared session identity
- `#121` Codex fixture and regression coverage, landed incrementally
- `#122` Codex rollout raw import
- `#123` Codex rollout normalization into the shared model
- `#124` Codex auxiliary source import
- `#125` provider-aware query, report, UI, and docs support

Notes:

- The authoritative base for all future implementation slices is
  `codex-integration`, not `main`.
- End every future session back on the base branch and update this log before
  handoff.

### 2026-04-19 — `#119` Provider-Aware Source Model

Status: MERGED into `codex-integration`

Summary:

- Created child branch/worktree
  `codex-integration-provider-aware-source-model` at
  `.worktrees/codex-integration-provider-aware-source-model` and merged it back
  into `codex-integration`.
- Added provider-aware source modeling and runtime config support while keeping
  Claude import defaults and the legacy Claude `--source-root` override
  working.
- Split `source_provider` from `source_kind` in the import/cache schema,
  updated importer entry points and normalization, and landed migration
  `0015_provider_aware_sources.sql`.
- Landed the initial `#121` redacted Codex fixture slice plus regression tests
  that cover provider-aware scanning/import planning.
- Updated operator docs to describe `[sources.claude]`, optional
  `[sources.codex]`, and the required rebuild after this importer schema bump.

Verification:

- `cargo test --workspace`

Notes:

- The original slash-style child branch name from the plan
  (`codex-integration/provider-aware-source-model`) was not usable because the
  flat branch `codex-integration` already exists. Future child branches should
  use the dashed form `codex-integration-...`.

### 2026-04-19 — `#120` Shared Session Spine

Status: MERGED into `codex-integration`

Summary:

- Created child branch/worktree `codex-integration-shared-session-spine` at
  `.worktrees/codex-integration-shared-session-spine` and merged it back into
  `codex-integration`.
- Added explicit `conversation.shared_session_id` via migration
  `0016_shared_session_spine.sql` and bumped the initial schema version.
- Updated transcript normalization to persist the raw session id directly on
  each conversation while preserving the existing opaque per-source-file
  `external_id`.
- Replaced the remaining skill/session query joins that substring-parsed
  `conversation.external_id` with direct joins on `shared_session_id`.
- Landed the next `#121` regression slice by updating skill/session query tests
  to prove they still join when `external_id` is opaque and only
  `shared_session_id` carries the session identity.

Verification:

- `cargo test -p gnomon-core normalization_allows_duplicate_session_ids_across_source_files -- --nocapture`
- `cargo test -p gnomon-core skill_invocations_join_to_sessions_and_preserve_unmatched_rows -- --nocapture`
- `cargo test -p gnomon-core skills_report_aggregates_session_associated_metrics_by_skill_project_and_session -- --nocapture`
- `cargo test -p gnomon-core`
- `cargo test --workspace`

### 2026-04-19 — `#122` Codex Rollout Raw Import

Status: MERGED into `codex-integration`

Summary:

- Created child branch/worktree `codex-integration-codex-rollout-raw-import`
  at `.worktrees/codex-integration-codex-rollout-raw-import` and merged it
  back into `codex-integration`.
- Added migration `0017_codex_rollout_raw.sql`, bumped the initial schema
  version, and bumped the import schema version for the raw rollout contract.
- Introduced Codex-specific raw tables `codex_rollout_session` and
  `codex_rollout_event` so rollout data stays physically separate from Claude
  raw data.
- Implemented Codex rollout raw import, including session metadata capture,
  per-line raw event persistence, purge/reimport behavior, and chunk record
  counting for Codex rollout rows.
- Updated rollout scan attribution to read Codex-native `cwd` metadata,
  including `session_meta.payload.cwd`, so rollout-backed projects resolve from
  Codex session metadata instead of the rollout root path.
- Landed the next `#121` regression slice with:
  - a scan test that proves payload-based rollout `cwd` attribution
  - an end-to-end import test that proves raw Codex rollout rows import into
    Codex-specific tables and remain stable across reimport

Verification:

- `cargo test -p gnomon-core scan_codex_rollout_uses_payload_cwd_for_project_attribution -- --nocapture`
- `cargo test -p gnomon-core import_all_imports_codex_rollout_raw_sessions_without_blocking_claude_imports -- --nocapture`
- `cargo test -p gnomon-core`
- `cargo test --workspace`

---

## RESUME HERE

Phase: `#122` merged; ready to start `#123`
Base branch: `codex-integration`
Base worktree: `/home/ketan/project/gnomon/.worktrees/codex-integration`
Last completed: Merged `#122` Codex rollout raw import and the next `#121` regression slice from `codex-integration-codex-rollout-raw-import` into the base branch
Next action: Start `#123` by creating child branch `codex-integration-codex-rollout-normalization` and worktree `.worktrees/codex-integration-codex-rollout-normalization`, then normalize rollout-backed Codex sessions into the shared conversation, message, turn, action, and usage model using the raw rollout tables landed in `#122`
Open issue sequence: `#123`, `#121` incremental, `#124`, `#125`
In-flight uncommitted state on base branch: none expected after the merge and log-update commits
Child-branch naming note: Because the flat branch `codex-integration` exists, `codex-integration/...` refs are invalid here; use dashed child branch names like `codex-integration-shared-session-spine`

---

## FRESH AGENT PROMPT

Continue Codex integration work from the long-lived base branch and worktree:

- Branch: `codex-integration`
- Worktree: `/home/ketan/project/gnomon/.worktrees/codex-integration`

Before doing anything else:

1. Read `docs/specs/2026-04-18-codex-integration-plan.md`.
2. Read this log file and treat the **RESUME HERE** block as authoritative.
3. Confirm the current branch is `codex-integration` in the base worktree.

Then:

1. Create the child branch and worktree for `#123` from `codex-integration`.
   Use a dashed branch name such as
   `codex-integration-codex-rollout-normalization`; do not use
   `codex-integration/...` because that ref layout conflicts with the existing
   flat base branch.
2. Implement only the Codex rollout normalization slice in that child worktree.
   Normalize rollout-backed Codex sessions into the shared conversation,
   message, turn, action, and usage model without collapsing provider-specific
   raw metadata back into Claude-shaped assumptions.
3. Land any relevant `#121` fixture and regression-test slice with the `#123`
   work.
4. Verify the resulting change.
5. Merge the child branch back into `codex-integration`.
6. Return to the base worktree.
7. Update this log with:
   - what landed
   - what remains
   - the next recommended action
   - a refreshed fresh-agent prompt

End the session on the base branch and base worktree, not on the child branch.
