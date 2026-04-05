CREATE TABLE action_skill_attribution (
    action_id INTEGER PRIMARY KEY REFERENCES action(id) ON DELETE CASCADE,
    skill_name TEXT NOT NULL,
    confidence TEXT NOT NULL CHECK (confidence IN ('high'))
);

CREATE INDEX idx_action_skill_attribution_skill
    ON action_skill_attribution(skill_name, confidence);
