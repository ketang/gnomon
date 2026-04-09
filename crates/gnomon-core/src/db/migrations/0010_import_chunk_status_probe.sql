ALTER TABLE import_chunk
    ADD COLUMN last_attempt_phase TEXT
    CHECK (last_attempt_phase IN ('startup', 'deferred'));

ALTER TABLE import_chunk
    ADD COLUMN last_error_message TEXT;
