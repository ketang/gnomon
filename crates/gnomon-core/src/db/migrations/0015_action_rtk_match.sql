CREATE TABLE action_rtk_match (
    action_id    INTEGER PRIMARY KEY REFERENCES action(id) ON DELETE CASCADE,
    rtk_row_id   INTEGER NOT NULL,
    saved_tokens INTEGER NOT NULL CHECK (saved_tokens >= 0),
    savings_pct  REAL    NOT NULL,
    exec_time_ms INTEGER NOT NULL CHECK (exec_time_ms >= 0)
);

ALTER TABLE chunk_action_rollup
    ADD COLUMN rtk_saved_tokens INTEGER NOT NULL DEFAULT 0
        CHECK (rtk_saved_tokens >= 0);
