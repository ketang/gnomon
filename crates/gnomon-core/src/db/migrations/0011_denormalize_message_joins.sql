ALTER TABLE message ADD COLUMN turn_id INTEGER REFERENCES turn(id) ON DELETE CASCADE;
ALTER TABLE message ADD COLUMN action_id INTEGER REFERENCES action(id) ON DELETE CASCADE;

CREATE INDEX idx_message_turn_id ON message(turn_id);
CREATE INDEX idx_message_action_id ON message(action_id);

DROP TABLE turn_message;
DROP TABLE action_message;
