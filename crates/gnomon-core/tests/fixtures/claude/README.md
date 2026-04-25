Claude fixture corpus for provider-aware import tests.

Preserved:
- provider-specific file layout for Claude transcripts under `projects/<encoded-project>/*.jsonl`
- sibling `history.jsonl` at the Claude root
- stable relative paths that provider-aware scan tests can assert against
- representative user + assistant message shapes including `usage` token counts

Redacted:
- all real user, repo, host, and task identifiers
- `cwd` values replaced with `/tmp/redacted/project-a`; the encoded project
  directory name (`-tmp-redacted-project-a`) decodes to the same path
- message text shortened to compact synthetic examples

Layout:
- `projects/-tmp-redacted-project-a/session-claude-fixture-01.jsonl` — one
  user + one assistant message, with `cwd`, `timestamp`, and `usage`
- `history.jsonl` — one `{sessionId, timestamp, display}` history entry
