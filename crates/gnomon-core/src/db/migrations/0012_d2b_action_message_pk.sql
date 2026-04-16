-- D2b: reduce action_message from 3 btree indexes to 2 by making message_id
-- the PRIMARY KEY (which subsumes the old UNIQUE (message_id) constraint).
--
-- Old schema: PRIMARY KEY (action_id, message_id)
--           + UNIQUE (action_id, ordinal_in_action)
--           + UNIQUE (message_id)                    ← 3 btree indexes
--
-- New schema: PRIMARY KEY (message_id)
--           + UNIQUE (action_id, ordinal_in_action)  ← 2 btree indexes
--
-- Action lookups (JOIN action_message ON action_id) use the leading column of
-- UNIQUE (action_id, ordinal_in_action) — still covered.
-- Message lookups (WHERE message_id IN ...) use PK (message_id) — still covered.
-- Both CASCADE DELETEs remain efficient.
--
-- Existing action_message data is dropped here; IMPORT_SCHEMA_VERSION bump
-- triggers a full reimport that repopulates the table.

DROP TABLE IF EXISTS action_message;

CREATE TABLE action_message (
    action_id INTEGER NOT NULL REFERENCES action(id) ON DELETE CASCADE,
    message_id INTEGER NOT NULL REFERENCES message(id) ON DELETE CASCADE,
    ordinal_in_action INTEGER NOT NULL CHECK (ordinal_in_action >= 0),
    PRIMARY KEY (message_id),
    UNIQUE (action_id, ordinal_in_action)
);
