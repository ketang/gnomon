-- The `record` table was a per-JSONL-line raw mirror for transcripts, but
-- transcript imports stopped writing rows to it in the v5 schema change
-- (see the IMPORT_SCHEMA_VERSION docstring). It has no INSERT site, no
-- reader, and is permanently empty in every shard. Drop it and its
-- indexes so the schema reflects what the code actually uses.
--
-- The `codex_rollout_record` and `codex_rollout_event` tables (once p4
-- picks them up from main) are distinct — they are the active raw mirror
-- for Codex rollouts and are written by the Codex normalize path.
DROP INDEX IF EXISTS idx_record_chunk_conversation_sequence;
DROP INDEX IF EXISTS idx_record_stream_sequence;
DROP TABLE record;
