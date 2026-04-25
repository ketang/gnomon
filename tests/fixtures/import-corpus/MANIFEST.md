# Import Perf Corpus Snapshot Manifest

Captured: 2026-04-25T00:00:00Z
Host: Linux pontoon 6.6.87.2-microsoft-standard-WSL2 #1 SMP PREEMPT_DYNAMIC Thu Jun  5 18:30:46 UTC 2025 x86_64 x86_64 x86_64 GNU/Linux
Filesystem (this directory): ext2/ext3

## Full corpus
- path: full.tar.zst
- uncompressed_bytes: 1352004701
- jsonl_file_count: 4344
- project_count: 62
- sha256: 7f9a3d110355c3987a945b08f5370421763266640ed00fedf0ff1fbf74c1c16f

## Subset
- path: subset.tar.zst
- selection: largest projects by bytes until >= min(5% full, 100MB)
- uncompressed_bytes: 263087661
- jsonl_file_count: 1127
- project_count: 10
- sha256: a78baa49290551a8a27af565fc3bbb725d1f32f6d1c06c972636bc9b6f345799
