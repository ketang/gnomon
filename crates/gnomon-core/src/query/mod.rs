#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotBounds {
    pub max_publish_seq: u64,
}

impl SnapshotBounds {
    pub const fn bootstrap() -> Self {
        Self { max_publish_seq: 0 }
    }
}
