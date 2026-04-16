-- D1b: Preset turn_id at message INSERT time
--
-- Adds turn_id and ordinal_in_turn directly onto the message table so that
-- turn membership is set at INSERT time instead of via a post-hoc join table.
-- Eliminates the turn_message join table (295K rows × 3 btrees = 885K btree
-- ops per full-corpus import).  turn_id is nullable: root messages are INSERTed
-- before the turn row exists, then UPDATEd to set turn_id once the turn is
-- persisted.  No index is added on turn_id — per-chunk rollup and classify
-- queries either scan by conversation_id or receive messages in-memory, so a
-- dedicated btree on turn_id would add write overhead without benefiting any
-- hot read path.
ALTER TABLE message ADD COLUMN turn_id INTEGER REFERENCES turn(id) ON DELETE CASCADE;
ALTER TABLE message ADD COLUMN ordinal_in_turn INTEGER CHECK (ordinal_in_turn >= 0);
DROP TABLE turn_message;
