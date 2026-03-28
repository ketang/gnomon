use rusqlite_migration::{M, Migrations};

pub fn all() -> Migrations<'static> {
    Migrations::new(vec![
        M::up(include_str!("migrations/0001_initial.sql")),
        M::up(include_str!("migrations/0002_source_manifest.sql")),
        M::up(include_str!("migrations/0003_action_classification.sql")),
        M::up(include_str!("migrations/0004_chunk_rollups.sql")),
    ])
}
