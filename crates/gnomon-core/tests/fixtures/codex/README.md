Codex fixture corpus for provider-aware import tests.

Preserved:
- provider-specific file layout for rollout sessions, `history.jsonl`, and `session_index.jsonl`
- representative event names for rollout discovery and future normalization work
- stable relative paths that provider-aware scan tests can assert against

Redacted:
- all real user, repo, host, and task identifiers
- file-system paths replaced with `/tmp/redacted/...`
- message text shortened to compact synthetic examples

Represented CLI lineage:
- synthetic fixture for the `#119` provider-aware source-model/config slice
- sufficient for scan/config/regression coverage without requiring a live `~/.codex`
