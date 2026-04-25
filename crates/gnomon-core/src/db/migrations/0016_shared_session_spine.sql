ALTER TABLE conversation ADD COLUMN shared_session_id TEXT;

UPDATE conversation
SET shared_session_id = CASE
    WHEN instr(external_id, ':session:') > 0
        THEN substr(external_id, instr(external_id, ':session:') + 9)
    ELSE NULL
END
WHERE shared_session_id IS NULL;

CREATE INDEX idx_conversation_shared_session_id
    ON conversation(shared_session_id)
    WHERE shared_session_id IS NOT NULL;
