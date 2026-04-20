PRAGMA foreign_keys=OFF;

CREATE TABLE source_file_new (
    id INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    relative_path TEXT NOT NULL,
    source_provider TEXT NOT NULL
        CHECK (source_provider IN ('claude', 'codex')),
    source_kind TEXT NOT NULL
        CHECK (source_kind IN ('transcript', 'history', 'rollout', 'session_index')),
    discovered_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_at_utc TEXT,
    size_bytes INTEGER NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
    content_fingerprint TEXT,
    imported_size_bytes INTEGER,
    imported_modified_at_utc TEXT,
    scan_warnings_json TEXT NOT NULL DEFAULT '[]',
    imported_schema_version INTEGER,
    UNIQUE (project_id, source_provider, source_kind, relative_path)
);

INSERT INTO source_file_new (
    id,
    project_id,
    relative_path,
    source_provider,
    source_kind,
    discovered_at_utc,
    modified_at_utc,
    size_bytes,
    content_fingerprint,
    imported_size_bytes,
    imported_modified_at_utc,
    scan_warnings_json,
    imported_schema_version
)
SELECT
    id,
    project_id,
    relative_path,
    'claude',
    CASE source_kind
        WHEN 'claude_history' THEN 'history'
        ELSE 'transcript'
    END,
    discovered_at_utc,
    modified_at_utc,
    size_bytes,
    content_fingerprint,
    imported_size_bytes,
    imported_modified_at_utc,
    scan_warnings_json,
    imported_schema_version
FROM source_file;

DROP TABLE source_file;
ALTER TABLE source_file_new RENAME TO source_file;

CREATE TABLE scan_source_cache_new (
    source_root_path TEXT NOT NULL,
    policy_fingerprint TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    source_provider TEXT NOT NULL
        CHECK (source_provider IN ('claude', 'codex')),
    source_kind TEXT NOT NULL
        CHECK (source_kind IN ('transcript', 'history', 'rollout', 'session_index')),
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
    PRIMARY KEY (
        source_root_path,
        policy_fingerprint,
        source_provider,
        source_kind,
        relative_path
    )
);

INSERT INTO scan_source_cache_new (
    source_root_path,
    policy_fingerprint,
    relative_path,
    source_provider,
    source_kind,
    modified_at_utc,
    size_bytes,
    excluded,
    raw_cwd_path,
    scan_warnings_json,
    project_identity_kind,
    project_canonical_key,
    project_display_name,
    project_root_path,
    project_git_root_path,
    project_git_origin,
    project_identity_reason,
    updated_at_utc
)
SELECT
    source_root_path,
    policy_fingerprint,
    relative_path,
    'claude',
    CASE source_kind
        WHEN 'claude_history' THEN 'history'
        ELSE 'transcript'
    END,
    modified_at_utc,
    size_bytes,
    excluded,
    raw_cwd_path,
    scan_warnings_json,
    project_identity_kind,
    project_canonical_key,
    project_display_name,
    project_root_path,
    project_git_root_path,
    project_git_origin,
    project_identity_reason,
    updated_at_utc
FROM scan_source_cache;

DROP TABLE scan_source_cache;
ALTER TABLE scan_source_cache_new RENAME TO scan_source_cache;

CREATE INDEX idx_scan_source_cache_lookup
    ON scan_source_cache(
        source_root_path,
        policy_fingerprint,
        source_provider,
        source_kind,
        relative_path
    );

PRAGMA foreign_keys=ON;
