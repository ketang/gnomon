use rusqlite_migration::{M, Migrations};

pub fn all() -> Migrations<'static> {
    Migrations::new(vec![M::up(include_str!("migrations/0001_initial.sql"))])
}
