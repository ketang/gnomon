CREATE TABLE scan_source_cache (
    source_root_path TEXT NOT NULL,
    policy_fingerprint TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    source_kind TEXT NOT NULL
        CHECK (source_kind IN ('transcript', 'claude_history')),
    modified_at_utc TEXT,
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    excluded INTEGER NOT NULL CHECK (excluded IN (0, 1)),
    raw_cwd_path TEXT,
    scan_warnings_json TEXT NOT NULL DEFAULT '[]',
    project_identity_kind TEXT CHECK (project_identity_kind IN ('git', 'path')),
    project_canonical_key TEXT,
    project_display_name TEXT,
    project_root_path TEXT,
    project_git_root_path TEXT,
    project_git_origin TEXT,
    project_identity_reason TEXT,
    updated_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_root_path, policy_fingerprint, source_kind, relative_path)
);

CREATE INDEX idx_scan_source_cache_lookup
    ON scan_source_cache(source_root_path, policy_fingerprint, relative_path);
