-- `imported_record_count` was intended as a "raw records imported"
-- aggregate, but the normalize-time UPDATE path (before p4's sharding)
-- ran on a shard connection where `import_chunk` is empty, and
-- `compute_shard_counts` overwrites the value from
-- `SELECT COUNT(*) FROM history_event`, which is zero for transcript
-- imports. Nothing in the product reads this column, so keeping a
-- permanently-zero counter is worse than dropping it. Remove.
ALTER TABLE import_chunk DROP COLUMN imported_record_count;
