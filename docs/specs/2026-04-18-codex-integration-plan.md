# Codex Integration Plan

> **For agentic workers:** Read the **RESUME HERE** block at the bottom of
> `2026-04-18-codex-integration-log.md` first. It is the authoritative handoff
> for the current base branch, worktree, landed work, and next action. Do not
> re-derive state from `git status` alone.

**Goal:** Add Codex session-log support to `gnomon` while keeping Codex raw data
physically separate from Claude raw data and sharing a common normalized model
for sessions, messages, turns, actions, and usage.

**Architecture:** One long-lived integration branch (`codex-integration`) with a
linked worktree at `.worktrees/codex-integration/`. Individual implementation
slices land on short-lived child branches forked from that base branch, each in
its own linked worktree. Kept child branches merge back into
`codex-integration`. At the end of the full effort, `codex-integration` merges
into `main` with `git merge --no-ff`, and the associated GitHub issues close.

---

## 0. Tracking Context

GitHub issue set:

| issue | title | role |
| --- | --- | --- |
| #119 | Add provider-aware source modeling and configuration for multi-provider import | foundation |
| #120 | Introduce explicit shared session identity for Claude and Codex imports | foundation |
| #121 | Add a redacted Codex fixture corpus and multi-provider import test coverage | cross-cutting |
| #122 | Import Codex rollout session logs as a first-class raw source | raw Codex import |
| #123 | Normalize Codex rollout events into the shared message, turn, action, and usage model | shared-model integration |
| #124 | Import Codex `history.jsonl` and `session_index.jsonl` as auxiliary Codex sources | Codex enrichment |
| #125 | Add provider-aware query, report, UI, and docs support for Claude and Codex data | user-facing completion |

Execution order:

1. `#119`
2. `#120`
3. `#121` fixture and test slices land incrementally with the relevant work
4. `#122`
5. `#123`
6. `#124`
7. `#125`

---

## 1. Branch And Worktree Layout

Long-lived integration branch and worktree:

- Branch: `codex-integration`
- Worktree: `.worktrees/codex-integration`

Planned child branches and worktrees:

| issue | child branch | worktree |
| --- | --- | --- |
| #119 | `codex-integration/provider-aware-source-model` | `.worktrees/codex-integration-provider-aware-source-model` |
| #120 | `codex-integration/shared-session-spine` | `.worktrees/codex-integration-shared-session-spine` |
| #122 | `codex-integration/codex-rollout-raw-import` | `.worktrees/codex-integration-codex-rollout-raw-import` |
| #123 | `codex-integration/codex-rollout-normalization` | `.worktrees/codex-integration-codex-rollout-normalization` |
| #124 | `codex-integration/codex-aux-sources` | `.worktrees/codex-integration-codex-aux-sources` |
| #125 | `codex-integration/provider-aware-surfaces` | `.worktrees/codex-integration-provider-aware-surfaces` |

Rules:

- Child branches always fork from `codex-integration`, not from `main`.
- Each child branch gets a dedicated linked worktree.
- Completed child branches merge back into `codex-integration`.
- The base branch is the orchestration and handoff point for every session.
- End each session on `codex-integration`, not on a child branch.

---

## 2. Session Workflow

Every session should follow this loop:

1. Start in `.worktrees/codex-integration` on branch `codex-integration`.
2. Read the running log and the **RESUME HERE** block.
3. Choose the next issue-sized slice and create the corresponding child branch
   and worktree from `codex-integration`.
4. Implement and verify in the child worktree.
5. Merge the completed child branch back into `codex-integration`.
6. Return to `.worktrees/codex-integration`.
7. Update the running log with:
   - what landed
   - what remains
   - current base branch and worktree
   - the next recommended action
   - a fresh-agent prompt for the next session
8. End the session on the base branch and base worktree.

Merge policy:

- Merge child branches into `codex-integration` with `git merge --no-ff`.
- Rebase child branches onto `codex-integration` before merging when they have
  drifted.
- Leave abandoned child worktrees alone unless explicit cleanup is requested.

---

## 3. Scope Boundaries Per Issue

### `#119` Provider-Aware Source Model

- Split provider identity from source format in the import model.
- Generalize config and source discovery away from the current Claude-only
  assumptions.
- Keep existing Claude imports working.

### `#120` Shared Session Spine

- Add explicit shared session identity for Claude and Codex imports.
- Remove query paths that parse `conversation.external_id` for joins.

### `#121` Fixtures And Tests

- Land fixture and regression-test slices alongside the issues they protect.
- Do not hold `#121` until the end as a cleanup-only issue.

### `#122` Codex Rollout Raw Import

- Treat `~/.codex/sessions/**/rollout-*.jsonl` as the first-class Codex source.
- Import raw Codex rows into Codex-specific tables.
- Attribute project identity from Codex `cwd`.

### `#123` Codex Shared-Model Normalization

- Normalize rollout sessions into the common session, message, turn, action,
  and usage model.
- Extend classification only where Codex event shapes require it.

### `#124` Codex Auxiliary Sources

- Import Codex `history.jsonl` and `session_index.jsonl` as separate auxiliary
  Codex sources.
- Preserve unmatched rows rather than inventing project attribution.

### `#125` Provider-Aware User Surfaces

- Add provider-aware query filters, report output, TUI surfaces, web surfaces,
  and docs.
- Keep mixed-provider views explicit rather than implicit.

---

## 4. Definition Of Done For The Integration Branch

The long-lived `codex-integration` branch is ready to merge into `main` when:

- `#119` through `#125` are implemented or intentionally descoped with updated
  issue notes.
- `#121` is satisfied by committed fixtures and regression coverage across the
  implemented slices.
- Codex raw data is physically separate from Claude raw data.
- Claude and Codex share the intended normalized model.
- Provider-aware filtering works across query, report, and user-facing surfaces.
- The running log ends with a final merge summary and closure checklist.

