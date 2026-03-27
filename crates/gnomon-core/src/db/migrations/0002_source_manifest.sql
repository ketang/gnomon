ALTER TABLE project ADD COLUMN identity_reason TEXT;

ALTER TABLE source_file ADD COLUMN imported_size_bytes INTEGER;
ALTER TABLE source_file ADD COLUMN imported_modified_at_utc TEXT;
ALTER TABLE source_file ADD COLUMN scan_warnings_json TEXT NOT NULL DEFAULT '[]';
