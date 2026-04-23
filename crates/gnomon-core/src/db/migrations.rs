use rusqlite_migration::{M, Migrations};

pub fn all() -> Migrations<'static> {
    Migrations::new(vec![
        M::up(include_str!("migrations/0001_initial.sql")),
        M::up(include_str!("migrations/0002_source_manifest.sql")),
        M::up(include_str!("migrations/0003_action_classification.sql")),
        M::up(include_str!("migrations/0004_chunk_rollups.sql")),
        M::up(include_str!("migrations/0005_import_schema_version.sql")),
        M::up(include_str!("migrations/0006_path_rollups.sql")),
        M::up(include_str!("migrations/0007_history_sources.sql")),
        M::up(include_str!("migrations/0008_skill_invocations.sql")),
        M::up(include_str!("migrations/0009_action_skill_attribution.sql")),
        M::up(include_str!(
            "migrations/0010_import_chunk_status_probe.sql"
        )),
        M::up(include_str!("migrations/0011_d1b_message_turn_id.sql")),
        M::up(include_str!("migrations/0013_scan_source_cache.sql")),
        M::up(include_str!("migrations/0014_pending_chunk_rebuild.sql")),
        M::up(include_str!(
            "migrations/0019_drop_imported_record_count.sql"
        )),
        M::up(include_str!("migrations/0020_drop_record_table.sql")),
    ])
}
