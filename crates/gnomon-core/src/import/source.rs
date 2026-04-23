use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, bail};
use jiff::{Timestamp, tz::TimeZone};
use rayon::prelude::*;
use rusqlite::{OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use crate::config::{
    ProjectFilterAction, ProjectFilterContext, ProjectFilterRule, ProjectIdentityPolicy,
};
use crate::db::Database;
use crate::import::SourceFileKind;
use crate::perf::{PerfLogger, PerfScope};
use crate::vcs::{self, ProjectIdentityKind, ResolvedProject};

const CLAUDE_HISTORY_PROJECT_REASON: &str = "claude history source";
const CLAUDE_HISTORY_RELATIVE_PATH: &str = "../history.jsonl";
const WARNING_INVALID_JSON: &str = "invalid_json";
const WARNING_MISSING_CWD: &str = "missing_cwd";
const WARNING_PATH_PROJECT: &str = "path_project";
const WARNING_REUSED_CACHED_PROJECT: &str = "reused_cached_project";

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ScanReport {
    pub discovered_source_files: usize,
    pub excluded_source_files: usize,
    pub inserted_projects: usize,
    pub updated_projects: usize,
    pub inserted_source_files: usize,
    pub updated_source_files: usize,
    pub deleted_source_files: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScanWarning {
    pub code: String,
    pub message: String,
}

impl ScanWarning {
    fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone)]
struct DiscoveredSourceFile {
    source_kind: SourceFileKind,
    project: ResolvedProject,
    relative_path: String,
    modified_at_utc: Option<String>,
    size_bytes: i64,
    scan_warnings_json: String,
}

#[derive(Debug, Deserialize)]
struct SourceRecordHeader {
    cwd: Option<PathBuf>,
}

#[derive(Debug)]
struct ExtractedCwd {
    cwd: Option<PathBuf>,
    warnings: Vec<ScanWarning>,
}

#[derive(Debug)]
struct StoredProject {
    id: i64,
    identity_kind: String,
    display_name: String,
    root_path: String,
    git_root_path: Option<String>,
    git_origin: Option<String>,
    identity_reason: Option<String>,
}

#[derive(Debug)]
struct StoredSourceFile {
    id: i64,
    source_kind: String,
    modified_at_utc: Option<String>,
    size_bytes: i64,
    scan_warnings_json: String,
}

#[derive(Debug, Clone)]
struct CandidateSourceFile {
    source_kind: SourceFileKind,
    absolute_path: PathBuf,
    relative_path: String,
    modified_at_utc: Option<String>,
    size_bytes: i64,
}

#[derive(Debug, Clone)]
struct ScanCacheRecord {
    source_kind: SourceFileKind,
    relative_path: String,
    modified_at_utc: Option<String>,
    size_bytes: i64,
    raw_cwd_path: Option<String>,
    excluded: bool,
    scan_warnings_json: String,
    project: Option<ResolvedProject>,
}

#[derive(Debug)]
struct StoredScanCacheRecord {
    modified_at_utc: Option<String>,
    size_bytes: i64,
    raw_cwd_path: Option<String>,
    excluded: bool,
    scan_warnings_json: String,
    project_identity_kind: Option<String>,
    project_canonical_key: Option<String>,
    project_display_name: Option<String>,
    project_root_path: Option<String>,
    project_git_root_path: Option<String>,
    project_git_origin: Option<String>,
    project_identity_reason: Option<String>,
}

#[derive(Debug, Default)]
struct DiscoveryStats {
    cache_hit_count: usize,
    cache_miss_count: usize,
}

pub fn scan_source_manifest(database: &mut Database, source_root: &Path) -> Result<ScanReport> {
    scan_source_manifest_with_policy_and_perf_logger(
        database,
        source_root,
        &ProjectIdentityPolicy::default(),
        &[],
        None,
    )
}

/// Like [`scan_source_manifest`] but emits an `import.scan_source` perf span
/// (and a nested `import.discover_source_files` span) using the supplied
/// [`PerfLogger`]. Used by the `import_bench` example to attribute the
/// non-`import.chunk` floor of startup-mode wall time.
pub fn scan_source_manifest_with_perf_logger(
    database: &mut Database,
    source_root: &Path,
    perf_logger: Option<PerfLogger>,
) -> Result<ScanReport> {
    scan_source_manifest_with_policy_and_perf_logger(
        database,
        source_root,
        &ProjectIdentityPolicy::default(),
        &[],
        perf_logger,
    )
}

fn scan_source_manifest_with_policy_and_perf_logger(
    database: &mut Database,
    source_root: &Path,
    identity_policy: &ProjectIdentityPolicy,
    project_filters: &[ProjectFilterRule],
    perf_logger: Option<PerfLogger>,
) -> Result<ScanReport> {
    let mut scope = PerfScope::new(perf_logger.clone(), "import.scan_source");
    scope.field("source_root", source_root.display().to_string());
    let result = scan_source_manifest_with_policy_inner(
        database,
        source_root,
        identity_policy,
        project_filters,
        perf_logger,
    );
    match &result {
        Ok(report) => {
            scope.field("discovered_source_files", report.discovered_source_files);
            scope.field("excluded_source_files", report.excluded_source_files);
            scope.field("inserted_projects", report.inserted_projects);
            scope.field("updated_projects", report.updated_projects);
            scope.field("inserted_source_files", report.inserted_source_files);
            scope.field("updated_source_files", report.updated_source_files);
            scope.field("deleted_source_files", report.deleted_source_files);
            scope.finish_ok();
        }
        Err(err) => scope.finish_error(err),
    }
    result
}

pub fn scan_source_manifest_with_policy(
    database: &mut Database,
    source_root: &Path,
    identity_policy: &ProjectIdentityPolicy,
    project_filters: &[ProjectFilterRule],
) -> Result<ScanReport> {
    scan_source_manifest_with_policy_and_perf_logger(
        database,
        source_root,
        identity_policy,
        project_filters,
        None,
    )
}

fn scan_source_manifest_with_policy_inner(
    database: &mut Database,
    source_root: &Path,
    identity_policy: &ProjectIdentityPolicy,
    project_filters: &[ProjectFilterRule],
    perf_logger: Option<PerfLogger>,
) -> Result<ScanReport> {
    if source_root.exists() && !source_root.is_dir() {
        bail!("source root {} is not a directory", source_root.display());
    }

    let policy_fingerprint = scan_policy_fingerprint(identity_policy, project_filters)
        .context("unable to encode scan source policy fingerprint")?;
    let discovery = discover_source_files(
        database.connection(),
        source_root,
        identity_policy,
        project_filters,
        &policy_fingerprint,
        perf_logger.clone(),
    )?;
    let mut report = ScanReport {
        discovered_source_files: discovery.discovered_source_files,
        excluded_source_files: discovery.excluded_source_files,
        ..ScanReport::default()
    };
    let mut seen_files = HashSet::with_capacity(discovery.files.len());

    let tx = database
        .connection_mut()
        .transaction()
        .context("unable to begin a source manifest scan transaction")?;

    for file in &discovery.files {
        let project_id = upsert_project(&tx, &file.project, &mut report)?;
        upsert_source_file(&tx, project_id, file, &mut report)?;
        seen_files.insert((project_id, file.relative_path.clone()));
    }

    let mut cache_scope = PerfScope::new(perf_logger, "import.persist_scan_source_cache");
    cache_scope.field("cache_hit_count", discovery.stats.cache_hit_count);
    cache_scope.field("cache_miss_count", discovery.stats.cache_miss_count);
    match reconcile_scan_source_cache(
        &tx,
        source_root,
        &policy_fingerprint,
        &discovery.cache_records,
    ) {
        Ok(()) => cache_scope.finish_ok(),
        Err(err) => {
            cache_scope.finish_error(&err);
            return Err(err);
        }
    }

    report.deleted_source_files = delete_missing_source_files(&tx, &seen_files)?;
    delete_orphaned_projects(&tx)?;
    tx.commit()
        .context("unable to commit the source manifest scan transaction")?;

    Ok(report)
}

#[derive(Debug)]
struct DiscoveryResult {
    files: Vec<DiscoveredSourceFile>,
    cache_records: Vec<ScanCacheRecord>,
    discovered_source_files: usize,
    excluded_source_files: usize,
    stats: DiscoveryStats,
}

fn discover_source_files(
    conn: &rusqlite::Connection,
    source_root: &Path,
    identity_policy: &ProjectIdentityPolicy,
    project_filters: &[ProjectFilterRule],
    policy_fingerprint: &str,
    perf_logger: Option<PerfLogger>,
) -> Result<DiscoveryResult> {
    let mut candidate_scope = PerfScope::new(
        perf_logger.clone(),
        "import.discover_source_files.collect_candidates",
    );
    let candidates = collect_candidate_source_files(source_root)?;
    candidate_scope.field("candidate_count", candidates.len());
    candidate_scope.finish_ok();

    let mut cache_scope = PerfScope::new(
        perf_logger.clone(),
        "import.discover_source_files.load_cache",
    );
    let cached_rows = load_scan_source_cache(conn, source_root, policy_fingerprint)?;
    cache_scope.field("cached_row_count", cached_rows.len());
    cache_scope.finish_ok();

    let mut discovered_files = Vec::new();
    let mut cache_records = Vec::with_capacity(candidates.len());
    let mut excluded_source_files = 0usize;
    let mut stats = DiscoveryStats::default();
    let mut misses = Vec::new();

    for candidate in candidates {
        let cache_key = (
            candidate.source_kind.as_str().to_string(),
            candidate.relative_path.clone(),
        );
        if let Some(cached_row) = cached_rows.get(&cache_key)
            && cached_row.modified_at_utc == candidate.modified_at_utc
            && cached_row.size_bytes == candidate.size_bytes
        {
            let cache_record = cached_row.to_cache_record(&candidate)?;
            if let Some(file) = cache_record.to_discovered_source_file()? {
                discovered_files.push(file);
            } else {
                excluded_source_files += 1;
            }
            cache_records.push(cache_record);
            stats.cache_hit_count += 1;
            continue;
        }

        stats.cache_miss_count += 1;
        misses.push(candidate);
    }

    let mut miss_scope = PerfScope::new(
        perf_logger,
        "import.discover_source_files.resolve_cache_misses",
    );
    miss_scope.field("cache_miss_count", misses.len());
    let resolved_misses = resolve_cache_misses_parallel(
        source_root,
        misses,
        identity_policy,
        project_filters,
        &cached_rows,
    )?;
    for cache_record in resolved_misses {
        if let Some(file) = cache_record.to_discovered_source_file()? {
            discovered_files.push(file);
        } else {
            excluded_source_files += 1;
        }
        cache_records.push(cache_record);
    }
    miss_scope.finish_ok();

    discovered_files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    cache_records.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(DiscoveryResult {
        files: discovered_files,
        cache_records,
        discovered_source_files: stats.cache_hit_count + stats.cache_miss_count,
        excluded_source_files,
        stats,
    })
}

fn collect_candidate_source_files(source_root: &Path) -> Result<Vec<CandidateSourceFile>> {
    let mut candidates = Vec::new();

    if source_root.exists() {
        for entry in WalkDir::new(source_root) {
            let entry = entry
                .with_context(|| format!("unable to walk source root {}", source_root.display()))?;
            if !entry.file_type().is_file() {
                continue;
            }
            if entry.path().extension().and_then(|ext| ext.to_str()) != Some("jsonl") {
                continue;
            }
            candidates.push(candidate_source_file(
                source_root,
                entry.path(),
                SourceFileKind::Transcript,
            )?);
        }
    }

    if let Some(history_path) = claude_history_path(source_root)
        && history_path.is_file()
    {
        candidates.push(candidate_source_file(
            source_root,
            &history_path,
            SourceFileKind::ClaudeHistory,
        )?);
    }

    candidates.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(candidates)
}

fn candidate_source_file(
    source_root: &Path,
    source_file_path: &Path,
    source_kind: SourceFileKind,
) -> Result<CandidateSourceFile> {
    let metadata = fs::metadata(source_file_path)
        .with_context(|| format!("unable to read metadata for {}", source_file_path.display()))?;
    let relative_path = match source_kind {
        SourceFileKind::Transcript => source_file_path
            .strip_prefix(source_root)
            .with_context(|| {
                format!(
                    "unable to express {} relative to {}",
                    source_file_path.display(),
                    source_root.display()
                )
            })?
            .to_string_lossy()
            .into_owned(),
        SourceFileKind::ClaudeHistory => CLAUDE_HISTORY_RELATIVE_PATH.to_string(),
    };
    let size_bytes = i64::try_from(metadata.len())
        .with_context(|| format!("source file {} is too large", source_file_path.display()))?;
    let modified_at_utc = modified_at_utc(&metadata).with_context(|| {
        format!(
            "unable to read modified time for {}",
            source_file_path.display()
        )
    })?;

    Ok(CandidateSourceFile {
        source_kind,
        absolute_path: source_file_path.to_path_buf(),
        relative_path,
        modified_at_utc,
        size_bytes,
    })
}

fn resolve_cache_misses_parallel(
    source_root: &Path,
    misses: Vec<CandidateSourceFile>,
    identity_policy: &ProjectIdentityPolicy,
    project_filters: &[ProjectFilterRule],
    cached_rows: &HashMap<(String, String), StoredScanCacheRecord>,
) -> Result<Vec<ScanCacheRecord>> {
    let resolved_project_cache = Arc::new(Mutex::new(HashMap::<PathBuf, ResolvedProject>::new()));
    let project_root = source_root.parent().unwrap_or(source_root).to_path_buf();

    misses
        .into_par_iter()
        .map(|candidate| {
            let cache_key = (
                candidate.source_kind.as_str().to_string(),
                candidate.relative_path.clone(),
            );
            let previous = cached_rows.get(&cache_key);
            resolve_candidate_source_file(
                &candidate,
                identity_policy,
                project_filters,
                &project_root,
                resolved_project_cache.clone(),
                previous,
            )
        })
        .collect()
}

fn resolve_candidate_source_file(
    candidate: &CandidateSourceFile,
    identity_policy: &ProjectIdentityPolicy,
    project_filters: &[ProjectFilterRule],
    history_project_root: &Path,
    resolved_project_cache: Arc<Mutex<HashMap<PathBuf, ResolvedProject>>>,
    previous: Option<&StoredScanCacheRecord>,
) -> Result<ScanCacheRecord> {
    match candidate.source_kind {
        SourceFileKind::Transcript => {
            let ExtractedCwd { cwd, mut warnings } = extract_cwd(&candidate.absolute_path)?;
            let raw_cwd = cwd.clone();
            let mut reused_previous_identity = false;
            let project = match cwd {
                Some(cwd) => {
                    resolve_project_with_memo(cwd, identity_policy, &resolved_project_cache)
                }
                None => {
                    // When the current content no longer yields a cwd (the
                    // file was truncated, corrupted, or is a partial write),
                    // prefer the previously-scanned identity over a fresh
                    // path-fallback. Otherwise a transient corruption flips
                    // the file from project X (git-backed) to a new
                    // path-only project Y — the source_file manifest row
                    // gets reinserted under Y, every shard-resident row
                    // that referenced the old source_file_id becomes
                    // orphan work for the delete-missing cleanup, and
                    // TUI readers mid-scan see the transient project.
                    // Reusing the cached identity keeps the manifest row
                    // stable until content recovers (or another scan
                    // proves the identity truly changed).
                    if let Some(cached) = previous.and_then(cached_git_project) {
                        reused_previous_identity = true;
                        cached
                    } else {
                        vcs::path_project(&candidate.absolute_path, vcs::PATH_REASON_MISSING_CWD)
                    }
                }
            };

            let excluded = (!identity_policy.fallback_path_projects
                && project.identity_kind == ProjectIdentityKind::Path)
                || should_exclude_project(raw_cwd.as_deref(), &project, project_filters)?;

            if project.identity_kind == ProjectIdentityKind::Path
                && let Some(reason) = &project.identity_reason
            {
                warnings.push(ScanWarning::new(WARNING_PATH_PROJECT, reason.clone()));
            }
            if reused_previous_identity {
                warnings.push(ScanWarning::new(
                    WARNING_REUSED_CACHED_PROJECT,
                    "current scan could not extract a cwd; reusing the identity from \
                     the previous successful scan"
                        .to_string(),
                ));
            }

            let scan_warnings_json = serde_json::to_string(&warnings).with_context(|| {
                format!(
                    "unable to serialize scan warnings for {}",
                    candidate.absolute_path.display()
                )
            })?;

            Ok(ScanCacheRecord {
                source_kind: candidate.source_kind,
                relative_path: candidate.relative_path.clone(),
                modified_at_utc: candidate.modified_at_utc.clone(),
                size_bytes: candidate.size_bytes,
                raw_cwd_path: raw_cwd.as_ref().map(|path| path_to_string(path)),
                excluded,
                scan_warnings_json,
                project: Some(project),
            })
        }
        SourceFileKind::ClaudeHistory => {
            let project = vcs::path_project(history_project_root, CLAUDE_HISTORY_PROJECT_REASON);
            let excluded = should_exclude_project(None, &project, project_filters)?;
            Ok(ScanCacheRecord {
                source_kind: candidate.source_kind,
                relative_path: candidate.relative_path.clone(),
                modified_at_utc: candidate.modified_at_utc.clone(),
                size_bytes: candidate.size_bytes,
                raw_cwd_path: None,
                excluded,
                scan_warnings_json: "[]".to_string(),
                project: Some(project),
            })
        }
    }
}

// Reconstructs the ResolvedProject that the previous successful scan
// recorded for this source file, but only if that scan reached a git-backed
// identity. Returns None for cached path-identity rows (there's nothing to
// prefer over a fresh path-fallback) or when any of the required columns
// are missing.
fn cached_git_project(previous: &StoredScanCacheRecord) -> Option<ResolvedProject> {
    if previous.project_identity_kind.as_deref() != Some("git") {
        return None;
    }
    let canonical_key = previous.project_canonical_key.clone()?;
    let display_name = previous.project_display_name.clone()?;
    let root_path = PathBuf::from(previous.project_root_path.clone()?);
    let git_root_path = previous.project_git_root_path.clone().map(PathBuf::from);
    Some(ResolvedProject {
        identity_kind: ProjectIdentityKind::Git,
        canonical_key,
        display_name,
        root_path,
        git_root_path,
        git_origin: previous.project_git_origin.clone(),
        identity_reason: previous.project_identity_reason.clone(),
    })
}

fn resolve_project_with_memo(
    cwd: PathBuf,
    identity_policy: &ProjectIdentityPolicy,
    resolved_project_cache: &Mutex<HashMap<PathBuf, ResolvedProject>>,
) -> ResolvedProject {
    if let Some(project) = resolved_project_cache
        .lock()
        .expect("resolved project memo mutex poisoned")
        .get(&cwd)
        .cloned()
    {
        return project;
    }

    let project = vcs::resolve_project_from_cwd_with_policy(&cwd, identity_policy);
    resolved_project_cache
        .lock()
        .expect("resolved project memo mutex poisoned")
        .insert(cwd, project.clone());
    project
}

impl StoredScanCacheRecord {
    fn to_cache_record(&self, candidate: &CandidateSourceFile) -> Result<ScanCacheRecord> {
        Ok(ScanCacheRecord {
            source_kind: candidate.source_kind,
            relative_path: candidate.relative_path.clone(),
            modified_at_utc: candidate.modified_at_utc.clone(),
            size_bytes: candidate.size_bytes,
            raw_cwd_path: self.raw_cwd_path.clone(),
            excluded: self.excluded,
            scan_warnings_json: self.scan_warnings_json.clone(),
            project: self.to_project()?,
        })
    }

    fn to_project(&self) -> Result<Option<ResolvedProject>> {
        let Some(project_identity_kind) = &self.project_identity_kind else {
            return Ok(None);
        };

        let identity_kind = match project_identity_kind.as_str() {
            "git" => ProjectIdentityKind::Git,
            "path" => ProjectIdentityKind::Path,
            other => bail!("unsupported cached project identity kind `{other}`"),
        };

        let canonical_key = self
            .project_canonical_key
            .clone()
            .context("scan source cache row missing project canonical key")?;
        let display_name = self
            .project_display_name
            .clone()
            .context("scan source cache row missing project display name")?;
        let root_path = PathBuf::from(
            self.project_root_path
                .clone()
                .context("scan source cache row missing project root path")?,
        );
        let git_root_path = self.project_git_root_path.clone().map(PathBuf::from);

        Ok(Some(ResolvedProject {
            identity_kind,
            canonical_key,
            display_name,
            root_path,
            git_root_path,
            git_origin: self.project_git_origin.clone(),
            identity_reason: self.project_identity_reason.clone(),
        }))
    }
}

impl ScanCacheRecord {
    fn to_discovered_source_file(&self) -> Result<Option<DiscoveredSourceFile>> {
        if self.excluded {
            return Ok(None);
        }

        let project = self
            .project
            .clone()
            .context("included scan source cache row missing project payload")?;
        Ok(Some(DiscoveredSourceFile {
            source_kind: self.source_kind,
            project,
            relative_path: self.relative_path.clone(),
            modified_at_utc: self.modified_at_utc.clone(),
            size_bytes: self.size_bytes,
            scan_warnings_json: self.scan_warnings_json.clone(),
        }))
    }
}

fn scan_policy_fingerprint(
    identity_policy: &ProjectIdentityPolicy,
    project_filters: &[ProjectFilterRule],
) -> Result<String> {
    serde_json::to_string(&(identity_policy, project_filters))
        .context("unable to serialize scan source policy fingerprint")
}

fn load_scan_source_cache(
    conn: &rusqlite::Connection,
    source_root: &Path,
    policy_fingerprint: &str,
) -> Result<HashMap<(String, String), StoredScanCacheRecord>> {
    let mut stmt = conn
        .prepare(
            "
            SELECT
                source_kind,
                relative_path,
                modified_at_utc,
                size_bytes,
                raw_cwd_path,
                excluded,
                scan_warnings_json,
                project_identity_kind,
                project_canonical_key,
                project_display_name,
                project_root_path,
                project_git_root_path,
                project_git_origin,
                project_identity_reason
            FROM scan_source_cache
            WHERE source_root_path = ?1 AND policy_fingerprint = ?2
            ",
        )
        .context("unable to prepare scan source cache query")?;
    let rows = stmt
        .query_map(
            params![path_to_string(source_root), policy_fingerprint],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    StoredScanCacheRecord {
                        modified_at_utc: row.get(2)?,
                        size_bytes: row.get(3)?,
                        raw_cwd_path: row.get(4)?,
                        excluded: row.get::<_, i64>(5)? != 0,
                        scan_warnings_json: row.get(6)?,
                        project_identity_kind: row.get(7)?,
                        project_canonical_key: row.get(8)?,
                        project_display_name: row.get(9)?,
                        project_root_path: row.get(10)?,
                        project_git_root_path: row.get(11)?,
                        project_git_origin: row.get(12)?,
                        project_identity_reason: row.get(13)?,
                    },
                ))
            },
        )
        .context("unable to load scan source cache rows")?;

    let mut cache_rows = HashMap::new();
    for row in rows {
        let (source_kind, relative_path, record) =
            row.context("unable to decode a scan source cache row")?;
        cache_rows.insert((source_kind, relative_path), record);
    }

    Ok(cache_rows)
}

fn reconcile_scan_source_cache(
    tx: &Transaction<'_>,
    source_root: &Path,
    policy_fingerprint: &str,
    cache_records: &[ScanCacheRecord],
) -> Result<()> {
    let source_root_path = path_to_string(source_root);
    let mut seen_keys = HashSet::with_capacity(cache_records.len());

    for record in cache_records {
        tx.execute(
            "
            INSERT INTO scan_source_cache (
                source_root_path,
                policy_fingerprint,
                relative_path,
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
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, CURRENT_TIMESTAMP)
            ON CONFLICT(source_root_path, policy_fingerprint, source_kind, relative_path)
            DO UPDATE SET
                modified_at_utc = excluded.modified_at_utc,
                size_bytes = excluded.size_bytes,
                excluded = excluded.excluded,
                raw_cwd_path = excluded.raw_cwd_path,
                scan_warnings_json = excluded.scan_warnings_json,
                project_identity_kind = excluded.project_identity_kind,
                project_canonical_key = excluded.project_canonical_key,
                project_display_name = excluded.project_display_name,
                project_root_path = excluded.project_root_path,
                project_git_root_path = excluded.project_git_root_path,
                project_git_origin = excluded.project_git_origin,
                project_identity_reason = excluded.project_identity_reason,
                updated_at_utc = CURRENT_TIMESTAMP
            ",
            params![
                source_root_path,
                policy_fingerprint,
                record.relative_path,
                record.source_kind.as_str(),
                record.modified_at_utc,
                record.size_bytes,
                if record.excluded { 1 } else { 0 },
                record.raw_cwd_path,
                record.scan_warnings_json,
                record.project.as_ref().map(|project| project.identity_kind.as_str()),
                record.project.as_ref().map(|project| project.canonical_key.as_str()),
                record.project.as_ref().map(|project| project.display_name.as_str()),
                record
                    .project
                    .as_ref()
                    .map(|project| path_to_string(&project.root_path)),
                record
                    .project
                    .as_ref()
                    .and_then(|project| project.git_root_path.as_ref())
                    .map(|path| path_to_string(path)),
                record.project.as_ref().and_then(|project| project.git_origin.as_deref()),
                record
                    .project
                    .as_ref()
                    .and_then(|project| project.identity_reason.as_deref()),
            ],
        )
        .context("unable to upsert a scan source cache row")?;
        seen_keys.insert((
            record.source_kind.as_str().to_string(),
            record.relative_path.clone(),
        ));
    }

    let mut stmt = tx
        .prepare(
            "
            SELECT source_kind, relative_path
            FROM scan_source_cache
            WHERE source_root_path = ?1 AND policy_fingerprint = ?2
            ",
        )
        .context("unable to prepare scan source cache reconciliation query")?;
    let rows = stmt
        .query_map(params![source_root_path, policy_fingerprint], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .context("unable to enumerate existing scan source cache rows")?;

    let mut stale_keys = Vec::new();
    for row in rows {
        let key = row.context("unable to decode existing scan source cache row")?;
        if !seen_keys.contains(&key) {
            stale_keys.push(key);
        }
    }
    drop(stmt);

    for (source_kind, relative_path) in stale_keys {
        tx.execute(
            "
            DELETE FROM scan_source_cache
            WHERE source_root_path = ?1
              AND policy_fingerprint = ?2
              AND source_kind = ?3
              AND relative_path = ?4
            ",
            params![
                source_root_path,
                policy_fingerprint,
                source_kind,
                relative_path
            ],
        )
        .context("unable to delete stale scan source cache row")?;
    }

    Ok(())
}

fn claude_history_path(source_root: &Path) -> Option<PathBuf> {
    let projects_dir_name = source_root.file_name()?.to_str()?;
    let claude_dir = source_root.parent()?;
    let claude_dir_name = claude_dir.file_name()?.to_str()?;
    if projects_dir_name != "projects" || claude_dir_name != ".claude" {
        return None;
    }
    Some(claude_dir.join("history.jsonl"))
}

fn should_exclude_project(
    raw_cwd: Option<&Path>,
    project: &ResolvedProject,
    project_filters: &[ProjectFilterRule],
) -> Result<bool> {
    let context = ProjectFilterContext {
        raw_cwd,
        resolved_root: &project.root_path,
        identity_reason: project.identity_reason.as_deref(),
    };

    for rule in project_filters {
        if rule.matches(&context)? {
            return Ok(matches!(rule.action, ProjectFilterAction::Exclude));
        }
    }

    Ok(false)
}

fn extract_cwd(source_file_path: &Path) -> Result<ExtractedCwd> {
    let file = File::open(source_file_path)
        .with_context(|| format!("unable to open {}", source_file_path.display()))?;
    let reader = BufReader::new(file);

    let mut first_parse_warning = None;
    for (index, line) in reader.lines().enumerate() {
        let line = line.with_context(|| {
            format!(
                "unable to read line {} from {}",
                index + 1,
                source_file_path.display()
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }

        match serde_json::from_str::<SourceRecordHeader>(&line) {
            Ok(record) => {
                if let Some(cwd) = record.cwd {
                    let mut warnings = Vec::new();
                    if let Some(warning) = first_parse_warning {
                        warnings.push(warning);
                    }
                    return Ok(ExtractedCwd {
                        cwd: Some(cwd),
                        warnings,
                    });
                }
            }
            Err(err) => {
                first_parse_warning.get_or_insert_with(|| {
                    ScanWarning::new(
                        WARNING_INVALID_JSON,
                        format!(
                            "unable to parse line {} while locating cwd: {err}",
                            index + 1
                        ),
                    )
                });
            }
        }
    }

    let mut warnings = Vec::new();
    if let Some(warning) = first_parse_warning {
        warnings.push(warning);
    }
    warnings.push(ScanWarning::new(
        WARNING_MISSING_CWD,
        vcs::PATH_REASON_MISSING_CWD,
    ));

    Ok(ExtractedCwd {
        cwd: None,
        warnings,
    })
}

fn modified_at_utc(metadata: &fs::Metadata) -> Result<Option<String>> {
    let modified = match metadata.modified() {
        Ok(modified) => modified,
        Err(_) => return Ok(None),
    };

    Ok(Some(
        Timestamp::try_from(modified)
            .context("modified time is outside the supported timestamp range")?
            .to_string(),
    ))
}

fn local_day_from_stored_timestamp(timestamp: &str) -> Result<Option<String>> {
    let parsed = match parse_scan_timestamp(timestamp) {
        Ok(parsed) => parsed,
        Err(_) => return Ok(None),
    };
    Ok(Some(parsed.to_zoned(TimeZone::system()).date().to_string()))
}

fn parse_scan_timestamp(timestamp: &str) -> Result<Timestamp> {
    if let Ok(parsed) = timestamp.parse::<Timestamp>() {
        return Ok(parsed);
    }

    let sqlite_utc = format!("{}Z", timestamp.replace(' ', "T"));
    sqlite_utc
        .parse::<Timestamp>()
        .with_context(|| format!("unable to parse timestamp {timestamp}"))
}

fn upsert_project(
    tx: &Transaction<'_>,
    project: &ResolvedProject,
    report: &mut ScanReport,
) -> Result<i64> {
    let root_path = path_to_string(&project.root_path);
    let git_root_path = project
        .git_root_path
        .as_ref()
        .map(|path| path_to_string(path));

    let existing = tx
        .query_row(
            "
            SELECT
                id,
                identity_kind,
                display_name,
                root_path,
                git_root_path,
                git_origin,
                identity_reason
            FROM project
            WHERE canonical_key = ?1
            ",
            [project.canonical_key.as_str()],
            |row| {
                Ok(StoredProject {
                    id: row.get(0)?,
                    identity_kind: row.get(1)?,
                    display_name: row.get(2)?,
                    root_path: row.get(3)?,
                    git_root_path: row.get(4)?,
                    git_origin: row.get(5)?,
                    identity_reason: row.get(6)?,
                })
            },
        )
        .optional()
        .context("unable to query an existing project manifest row")?;

    match existing {
        None => {
            tx.execute(
                "
                INSERT INTO project (
                    identity_kind,
                    canonical_key,
                    display_name,
                    root_path,
                    git_root_path,
                    git_origin,
                    identity_reason
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                ",
                params![
                    project.identity_kind.as_str(),
                    project.canonical_key,
                    project.display_name,
                    root_path,
                    git_root_path,
                    project.git_origin,
                    project.identity_reason,
                ],
            )
            .context("unable to insert a discovered project into the manifest")?;
            report.inserted_projects += 1;
            Ok(tx.last_insert_rowid())
        }
        Some(existing) => {
            let needs_update = existing.identity_kind != project.identity_kind.as_str()
                || existing.display_name != project.display_name
                || existing.root_path != root_path
                || existing.git_root_path != git_root_path
                || existing.git_origin != project.git_origin
                || existing.identity_reason != project.identity_reason;

            if needs_update {
                tx.execute(
                    "
                    UPDATE project
                    SET
                        identity_kind = ?2,
                        display_name = ?3,
                        root_path = ?4,
                        git_root_path = ?5,
                        git_origin = ?6,
                        identity_reason = ?7,
                        updated_at_utc = CURRENT_TIMESTAMP
                    WHERE id = ?1
                    ",
                    params![
                        existing.id,
                        project.identity_kind.as_str(),
                        project.display_name,
                        root_path,
                        git_root_path,
                        project.git_origin,
                        project.identity_reason,
                    ],
                )
                .context("unable to update a discovered project in the manifest")?;
                report.updated_projects += 1;
            }

            Ok(existing.id)
        }
    }
}

fn upsert_source_file(
    tx: &Transaction<'_>,
    project_id: i64,
    file: &DiscoveredSourceFile,
    report: &mut ScanReport,
) -> Result<()> {
    let existing = tx
        .query_row(
            "
            SELECT
                id,
                source_kind,
                modified_at_utc,
                size_bytes,
                scan_warnings_json
            FROM source_file
            WHERE project_id = ?1 AND relative_path = ?2
            ",
            params![project_id, file.relative_path],
            |row| {
                Ok(StoredSourceFile {
                    id: row.get(0)?,
                    source_kind: row.get(1)?,
                    modified_at_utc: row.get(2)?,
                    size_bytes: row.get(3)?,
                    scan_warnings_json: row.get(4)?,
                })
            },
        )
        .optional()
        .context("unable to query an existing source file manifest row")?;

    match existing {
        None => {
            tx.execute(
                "
                INSERT INTO source_file (
                    project_id,
                    relative_path,
                    source_kind,
                    modified_at_utc,
                    size_bytes,
                    scan_warnings_json
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ",
                params![
                    project_id,
                    file.relative_path,
                    file.source_kind.as_str(),
                    file.modified_at_utc,
                    file.size_bytes,
                    file.scan_warnings_json,
                ],
            )
            .context("unable to insert a discovered source file into the manifest")?;
            report.inserted_source_files += 1;
        }
        Some(existing) => {
            let needs_update = existing.source_kind != file.source_kind.as_str()
                || existing.modified_at_utc != file.modified_at_utc
                || existing.size_bytes != file.size_bytes
                || existing.scan_warnings_json != file.scan_warnings_json;

            if needs_update {
                tx.execute(
                    "
                    UPDATE source_file
                    SET
                        source_kind = ?2,
                        modified_at_utc = ?3,
                        size_bytes = ?4,
                        scan_warnings_json = ?5
                    WHERE id = ?1
                    ",
                    params![
                        existing.id,
                        file.source_kind.as_str(),
                        file.modified_at_utc,
                        file.size_bytes,
                        file.scan_warnings_json,
                    ],
                )
                .context("unable to update a discovered source file in the manifest")?;
                report.updated_source_files += 1;
            }
        }
    }

    Ok(())
}

fn delete_missing_source_files(
    tx: &Transaction<'_>,
    seen_files: &HashSet<(i64, String)>,
) -> Result<usize> {
    let mut stmt = tx
        .prepare(
            "
            SELECT id, project_id, relative_path, imported_modified_at_utc
            FROM source_file
            ",
        )
        .context("unable to prepare source file manifest reconciliation query")?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .context("unable to enumerate source file manifest rows")?;

    let mut delete_rows = Vec::new();
    for row in rows {
        let (id, project_id, relative_path, imported_modified_at_utc) =
            row.context("unable to read a source file manifest row")?;
        if !seen_files.contains(&(project_id, relative_path)) {
            delete_rows.push((id, project_id, imported_modified_at_utc));
        }
    }
    drop(stmt);

    for (id, project_id, imported_modified_at_utc) in &delete_rows {
        if let Some(imported_modified_at_utc) = imported_modified_at_utc.as_deref()
            && let Some(chunk_day_local) =
                local_day_from_stored_timestamp(imported_modified_at_utc)?
        {
            tx.execute(
                "
                INSERT INTO pending_chunk_rebuild (project_id, chunk_day_local)
                VALUES (?1, ?2)
                ON CONFLICT(project_id, chunk_day_local) DO NOTHING
                ",
                params![project_id, chunk_day_local],
            )
            .context("unable to queue pending chunk rebuild for deleted source file")?;
        }

        // Every shard-resident row that references this source_file must go
        // before we drop the manifest entry. Conversation/stream/message/
        // turn/action and their children live in `shard{project_id %
        // SHARD_COUNT}`; on the main connection those names resolve to a
        // TEMP VIEW shadowed by `configure_read_view`, which SQLite refuses
        // to DELETE through. `purge_shard_source_file_rows` routes the
        // deletes directly at the shard (and falls back to unqualified
        // deletes for the `open_unsharded` test escape hatch).
        purge_shard_source_file_rows(tx, *project_id, *id).with_context(|| {
            format!(
                "unable to purge shard rows for stale source file {id} \
                 (project_id {project_id})"
            )
        })?;
        tx.execute("DELETE FROM source_file WHERE id = ?1", [id])
            .with_context(|| format!("unable to delete stale source file manifest row {id}"))?;
    }

    Ok(delete_rows.len())
}

// Every row reachable from a source_file_id lives in exactly one shard
// (project_id % SHARD_COUNT). On the main connection those tables are masked
// by a TEMP VIEW, so we have to route the deletes directly at the shard. Try
// the shard-qualified path first; if it fails we assume the connection was
// opened without shards (the `Database::open_unsharded` test hatch) and issue
// unqualified deletes instead.
fn purge_shard_source_file_rows(
    tx: &Transaction<'_>,
    project_id: i64,
    source_file_id: i64,
) -> Result<()> {
    let shard_idx = crate::db::shard_index_for_project(project_id);
    let prefix = format!("shard{shard_idx}.");
    if try_purge_source_file_rows_with_prefix(tx, &prefix, source_file_id)?.is_ok() {
        return Ok(());
    }
    try_purge_source_file_rows_with_prefix(tx, "", source_file_id)?
        .map_err(|err| anyhow::anyhow!(err))
        .context("unable to purge source-file rows with unqualified names")?;
    Ok(())
}

// Run the nine DELETEs in FK-topological order (children first). Each DELETE
// is prefixed with an optional schema qualifier so the same body works for
// shard-qualified and unqualified paths. Returns `Ok(Ok(()))` on success and
// `Ok(Err(...))` if the first DELETE fails (so the caller can fall back).
fn try_purge_source_file_rows_with_prefix(
    tx: &Transaction<'_>,
    prefix: &str,
    source_file_id: i64,
) -> Result<std::result::Result<(), rusqlite::Error>> {
    let conversation_sub =
        format!("SELECT id FROM {prefix}conversation WHERE source_file_id = ?1",);
    let message_sub =
        format!("SELECT id FROM {prefix}message WHERE conversation_id IN ({conversation_sub})",);
    let turn_sub =
        format!("SELECT id FROM {prefix}turn WHERE conversation_id IN ({conversation_sub})",);

    let run = |sql: String| -> std::result::Result<(), rusqlite::Error> {
        tx.execute(&sql, [source_file_id]).map(|_| ())
    };

    // The first DELETE fails fast if the prefix is wrong (shard not attached).
    if let Err(err) = run(format!(
        "DELETE FROM {prefix}message_part WHERE message_id IN ({message_sub})",
    )) {
        return Ok(Err(err));
    }
    run(format!(
        "DELETE FROM {prefix}message_path_ref WHERE message_id IN ({message_sub})",
    ))?;
    run(format!(
        "DELETE FROM {prefix}action_message WHERE action_id IN \
         (SELECT id FROM {prefix}action WHERE turn_id IN ({turn_sub}))",
    ))?;
    run(format!(
        "DELETE FROM {prefix}action_skill_attribution WHERE action_id IN \
         (SELECT id FROM {prefix}action WHERE turn_id IN ({turn_sub}))",
    ))?;
    run(format!(
        "DELETE FROM {prefix}action WHERE turn_id IN ({turn_sub})",
    ))?;
    run(format!(
        "DELETE FROM {prefix}turn WHERE conversation_id IN ({conversation_sub})",
    ))?;
    run(format!(
        "DELETE FROM {prefix}message WHERE conversation_id IN ({conversation_sub})",
    ))?;
    run(format!(
        "DELETE FROM {prefix}stream WHERE conversation_id IN ({conversation_sub})",
    ))?;
    run(format!(
        "DELETE FROM {prefix}conversation WHERE source_file_id = ?1",
    ))?;
    run(format!(
        "DELETE FROM {prefix}history_event WHERE source_file_id = ?1",
    ))?;
    run(format!(
        "DELETE FROM {prefix}skill_invocation WHERE source_file_id = ?1",
    ))?;
    run(format!(
        "DELETE FROM {prefix}import_warning WHERE source_file_id = ?1",
    ))?;
    Ok(Ok(()))
}

fn delete_orphaned_projects(tx: &Transaction<'_>) -> Result<usize> {
    let mut stmt = tx
        .prepare(
            "
            SELECT project.id
            FROM project
            WHERE NOT EXISTS (
                SELECT 1
                FROM source_file
                WHERE source_file.project_id = project.id
            )
            ",
        )
        .context("unable to prepare orphaned project query")?;
    let project_ids = stmt
        .query_map([], |row| row.get::<_, i64>(0))
        .context("unable to enumerate orphaned projects")?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("unable to decode orphaned project rows")?;
    drop(stmt);

    for id in &project_ids {
        tx.execute("DELETE FROM project WHERE id = ?1", [id])
            .with_context(|| format!("unable to delete orphaned project row {id}"))?;
    }

    Ok(project_ids.len())
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::process::Command;

    use anyhow::{Context, Result, bail};
    use rusqlite::params;
    use serde_json::json;
    use tempfile::tempdir;

    use super::{
        ScanReport, ScanWarning, WARNING_INVALID_JSON, discover_source_files,
        scan_policy_fingerprint, scan_source_manifest, scan_source_manifest_with_policy,
    };
    use crate::config::{
        ProjectFilterAction, ProjectFilterMatchOn, ProjectFilterRule, ProjectIdentityPolicy,
    };
    use crate::db::Database;
    use crate::vcs::{PATH_REASON_GIT_ROOT_NOT_FOUND, resolve_project_from_cwd};

    #[test]
    fn scan_records_git_and_path_projects_in_the_manifest() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("source");
        let repo_root = temp.path().join("repo");
        let repo_nested = repo_root.join("nested").join("deeper");
        let non_git_root = temp.path().join("scratch").join("notes");

        fs::create_dir_all(&repo_nested)?;
        fs::create_dir_all(&non_git_root)?;
        gix::init(&repo_root)?;

        write_jsonl(&source_root.join("git/session.jsonl"), &repo_nested)?;
        write_jsonl(&source_root.join("path/session.jsonl"), &non_git_root)?;

        let mut db = Database::open_unsharded(temp.path().join("usage.sqlite3"))?;
        let report = scan_source_manifest(&mut db, &source_root)?;

        assert_eq!(
            report,
            ScanReport {
                discovered_source_files: 2,
                excluded_source_files: 0,
                inserted_projects: 2,
                updated_projects: 0,
                inserted_source_files: 2,
                updated_source_files: 0,
                deleted_source_files: 0,
            }
        );

        let mut stmt = db.connection().prepare(
            "
            SELECT
                identity_kind,
                canonical_key,
                display_name,
                root_path,
                git_root_path,
                identity_reason
            FROM project
            ORDER BY canonical_key
            ",
        )?;
        let projects = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        assert_eq!(projects.len(), 2);
        assert_eq!(projects[0].0, "git");
        assert_eq!(projects[0].1, format!("git:{}", repo_root.display()));
        assert_eq!(projects[0].2, "repo");
        assert_eq!(projects[0].3, repo_root.display().to_string());
        assert_eq!(projects[0].4, Some(repo_root.display().to_string()));
        assert_eq!(projects[0].5, None);

        assert_eq!(projects[1].0, "path");
        assert_eq!(projects[1].1, format!("path:{}", non_git_root.display()));
        assert_eq!(projects[1].2, "notes");
        assert_eq!(projects[1].3, non_git_root.display().to_string());
        assert_eq!(projects[1].4, None);
        assert_eq!(
            projects[1].5.as_deref(),
            Some(PATH_REASON_GIT_ROOT_NOT_FOUND)
        );

        let mut stmt = db.connection().prepare(
            "
            SELECT
                project.identity_kind,
                source_file.relative_path,
                source_file.size_bytes,
                source_file.modified_at_utc,
                source_file.imported_size_bytes,
                source_file.imported_modified_at_utc,
                source_file.scan_warnings_json
            FROM source_file
            JOIN project ON project.id = source_file.project_id
            ORDER BY source_file.relative_path
            ",
        )?;
        let source_files = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, String>(6)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        assert_eq!(source_files.len(), 2);
        assert_eq!(source_files[0].0, "git");
        assert_eq!(source_files[0].1, "git/session.jsonl");
        assert!(source_files[0].2 > 0);
        assert!(source_files[0].3.is_some());
        assert_eq!(source_files[0].4, None);
        assert_eq!(source_files[0].5, None);
        assert_eq!(source_files[0].6, "[]");

        assert_eq!(source_files[1].0, "path");
        let warnings: Vec<ScanWarning> = serde_json::from_str(&source_files[1].6)?;
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].code, "path_project");
        assert_eq!(warnings[0].message, PATH_REASON_GIT_ROOT_NOT_FOUND);

        Ok(())
    }

    #[test]
    fn repeated_scans_do_not_churn_the_manifest() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("source");
        let repo_root = temp.path().join("repo");
        let cwd = repo_root.join("workspace");

        fs::create_dir_all(&cwd)?;
        gix::init(&repo_root)?;
        write_jsonl(&source_root.join("project/session.jsonl"), &cwd)?;

        let mut db = Database::open_unsharded(temp.path().join("usage.sqlite3"))?;
        let first_scan = scan_source_manifest(&mut db, &source_root)?;
        let second_scan = scan_source_manifest(&mut db, &source_root)?;

        assert_eq!(first_scan.discovered_source_files, 1);
        assert_eq!(
            second_scan,
            ScanReport {
                discovered_source_files: 1,
                excluded_source_files: 0,
                inserted_projects: 0,
                updated_projects: 0,
                inserted_source_files: 0,
                updated_source_files: 0,
                deleted_source_files: 0,
            }
        );

        Ok(())
    }

    #[test]
    fn second_discovery_reuses_scan_source_cache_for_unchanged_files() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("source");
        let repo_root = temp.path().join("repo");
        let cwd = repo_root.join("workspace");

        fs::create_dir_all(&cwd)?;
        gix::init(&repo_root)?;
        write_jsonl(&source_root.join("project/session.jsonl"), &cwd)?;

        let mut db = Database::open_unsharded(temp.path().join("usage.sqlite3"))?;
        scan_source_manifest(&mut db, &source_root)?;

        let policy = ProjectIdentityPolicy::default();
        let policy_fingerprint = scan_policy_fingerprint(&policy, &[])?;
        let discovery = discover_source_files(
            db.connection(),
            &source_root,
            &policy,
            &[],
            &policy_fingerprint,
            None,
        )?;

        assert_eq!(discovery.stats.cache_hit_count, 1);
        assert_eq!(discovery.stats.cache_miss_count, 0);
        assert_eq!(discovery.files.len(), 1);

        Ok(())
    }

    #[test]
    fn scan_source_cache_is_namespaced_by_policy_fingerprint() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("source");
        let tmp_project = temp.path().join("tmp-root").join("session-root");

        fs::create_dir_all(&tmp_project)?;
        write_jsonl(&source_root.join("tmp/session.jsonl"), &tmp_project)?;

        let mut db = Database::open_unsharded(temp.path().join("usage.sqlite3"))?;
        scan_source_manifest(&mut db, &source_root)?;

        let exclude_rule = ProjectFilterRule {
            action: ProjectFilterAction::Exclude,
            match_on: ProjectFilterMatchOn::ResolvedRoot,
            path_prefix: Some(temp.path().join("tmp-root").to_string_lossy().into_owned()),
            glob: None,
            equals: None,
        };
        let policy = ProjectIdentityPolicy::default();
        let policy_fingerprint =
            scan_policy_fingerprint(&policy, std::slice::from_ref(&exclude_rule))?;
        let discovery = discover_source_files(
            db.connection(),
            &source_root,
            &policy,
            &[exclude_rule],
            &policy_fingerprint,
            None,
        )?;

        assert_eq!(discovery.stats.cache_hit_count, 0);
        assert_eq!(discovery.stats.cache_miss_count, 1);
        assert_eq!(discovery.excluded_source_files, 1);

        Ok(())
    }

    #[test]
    fn linked_worktrees_collapse_into_one_canonical_git_project() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("source");
        let repo_root = temp.path().join("repo");
        let worktree_root = temp.path().join("repo-worktree");

        init_git_repo(&repo_root)?;
        run_git(
            &repo_root,
            [
                "worktree",
                "add",
                "-b",
                "feature-worktree",
                worktree_root.to_str().context("non-utf8 worktree path")?,
            ],
        )?;

        let main_cwd = repo_root.join("src");
        let worktree_cwd = worktree_root.join("src");
        fs::create_dir_all(&main_cwd)?;
        fs::create_dir_all(&worktree_cwd)?;

        write_jsonl(&source_root.join("main/session.jsonl"), &main_cwd)?;
        write_jsonl(&source_root.join("worktree/session.jsonl"), &worktree_cwd)?;

        let mut db = Database::open_unsharded(temp.path().join("usage.sqlite3"))?;
        let report = scan_source_manifest(&mut db, &source_root)?;
        assert_eq!(report.discovered_source_files, 2);

        let project_count: i64 = db.connection().query_row(
            "SELECT COUNT(*) FROM project WHERE identity_kind = 'git'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(project_count, 1);

        let canonical_root: String = db.connection().query_row(
            "SELECT root_path FROM project WHERE identity_kind = 'git'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(canonical_root, repo_root.display().to_string());

        let distinct_project_ids: i64 = db.connection().query_row(
            "SELECT COUNT(DISTINCT project_id) FROM source_file",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(distinct_project_ids, 1);

        Ok(())
    }

    #[test]
    fn deleted_worktree_cwds_recover_to_the_main_repo_project() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("source");
        let repo_root = temp.path().join("repo");
        let worktree_root = repo_root
            .join(".claude")
            .join("worktrees")
            .join("feature-worktree");

        init_git_repo(&repo_root)?;
        run_git(&repo_root, ["branch", "-m", "main"])?;
        run_git(&repo_root, ["branch", "feature"])?;
        run_git_args(&[
            "-C",
            repo_root.to_str().context("non-utf8 repo path")?,
            "worktree",
            "add",
            worktree_root.to_str().context("non-utf8 worktree path")?,
            "feature",
        ])?;

        write_jsonl(&source_root.join("worktree/session.jsonl"), &worktree_root)?;
        run_git_args(&[
            "-C",
            repo_root.to_str().context("non-utf8 repo path")?,
            "worktree",
            "remove",
            "--force",
            worktree_root.to_str().context("non-utf8 worktree path")?,
        ])?;

        let mut db = Database::open_unsharded(temp.path().join("usage.sqlite3"))?;
        let report = scan_source_manifest(&mut db, &source_root)?;
        assert_eq!(report.discovered_source_files, 1);

        let project: (String, String, String, Option<String>) = db.connection().query_row(
            "
            SELECT identity_kind, display_name, root_path, identity_reason
            FROM project
            ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;

        assert_eq!(project.0, "git");
        assert_eq!(project.1, "repo");
        assert_eq!(project.2, repo_root.display().to_string());
        assert_eq!(project.3, None);

        Ok(())
    }

    #[test]
    fn separate_clones_of_same_repo_remain_distinct_git_projects() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("source");
        let repo_root = temp.path().join("repo");
        let clone_a = temp.path().join("clone-a");
        let clone_b = temp.path().join("clone-b");

        init_git_repo(&repo_root)?;
        run_git_args(&[
            "clone",
            repo_root.to_str().context("non-utf8 repo path")?,
            clone_a.to_str().context("non-utf8 clone-a path")?,
        ])?;
        run_git_args(&[
            "clone",
            repo_root.to_str().context("non-utf8 repo path")?,
            clone_b.to_str().context("non-utf8 clone-b path")?,
        ])?;

        let clone_a_cwd = clone_a.join("src");
        let clone_b_cwd = clone_b.join("src");
        fs::create_dir_all(&clone_a_cwd)?;
        fs::create_dir_all(&clone_b_cwd)?;

        write_jsonl(&source_root.join("clone-a/session.jsonl"), &clone_a_cwd)?;
        write_jsonl(&source_root.join("clone-b/session.jsonl"), &clone_b_cwd)?;

        let mut db = Database::open_unsharded(temp.path().join("usage.sqlite3"))?;
        let report = scan_source_manifest(&mut db, &source_root)?;
        assert_eq!(report.discovered_source_files, 2);

        let projects: Vec<(String, String)> = db
            .connection()
            .prepare(
                "
                SELECT display_name, root_path
                FROM project
                WHERE identity_kind = 'git'
                ORDER BY root_path
                ",
            )?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        assert_eq!(
            projects,
            vec![
                ("clone-a".to_string(), clone_a.display().to_string(),),
                ("clone-b".to_string(), clone_b.display().to_string(),),
            ]
        );

        Ok(())
    }

    #[test]
    fn nonexistent_cwd_paths_remain_distinct_path_projects() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("source");
        let missing_a = temp.path().join("agent-a").join("session-root");
        let missing_b = temp.path().join("teammate-b").join("session-root");

        write_jsonl(&source_root.join("a/session.jsonl"), &missing_a)?;
        write_jsonl(&source_root.join("b/session.jsonl"), &missing_b)?;

        let mut db = Database::open_unsharded(temp.path().join("usage.sqlite3"))?;
        let report = scan_source_manifest(&mut db, &source_root)?;
        assert_eq!(report.discovered_source_files, 2);

        let projects: Vec<(String, String, String)> = db
            .connection()
            .prepare(
                "
                SELECT display_name, root_path, identity_reason
                FROM project
                WHERE identity_kind = 'path'
                ORDER BY root_path
                ",
            )?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        assert_eq!(
            projects,
            vec![
                (
                    "session-root".to_string(),
                    missing_a.display().to_string(),
                    PATH_REASON_GIT_ROOT_NOT_FOUND.to_string(),
                ),
                (
                    "session-root".to_string(),
                    missing_b.display().to_string(),
                    PATH_REASON_GIT_ROOT_NOT_FOUND.to_string(),
                ),
            ]
        );

        Ok(())
    }

    #[test]
    fn common_dir_backed_worktrees_collapse_into_one_canonical_git_project() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("source");
        let repo_root = temp.path().join("repo");
        let bare_root = temp.path().join("repo.git");
        let worktree_a = temp.path().join("agent-a");
        let worktree_b = temp.path().join("agent-b");

        init_git_repo(&repo_root)?;
        run_git(&repo_root, ["branch", "-m", "main"])?;
        run_git_args(&[
            "clone",
            "--bare",
            repo_root.to_str().context("non-utf8 repo path")?,
            bare_root.to_str().context("non-utf8 bare path")?,
        ])?;
        run_git_args(&[
            "-C",
            bare_root.to_str().context("non-utf8 bare path")?,
            "worktree",
            "add",
            worktree_a.to_str().context("non-utf8 worktree-a path")?,
            "main",
        ])?;
        run_git_args(&[
            "-C",
            bare_root.to_str().context("non-utf8 bare path")?,
            "worktree",
            "add",
            "-b",
            "feature-b",
            worktree_b.to_str().context("non-utf8 worktree-b path")?,
            "main",
        ])?;

        let worktree_a_cwd = worktree_a.join("src");
        let worktree_b_cwd = worktree_b.join("src");
        fs::create_dir_all(&worktree_a_cwd)?;
        fs::create_dir_all(&worktree_b_cwd)?;

        write_jsonl(&source_root.join("a/session.jsonl"), &worktree_a_cwd)?;
        write_jsonl(&source_root.join("b/session.jsonl"), &worktree_b_cwd)?;

        let mut db = Database::open_unsharded(temp.path().join("usage.sqlite3"))?;
        let report = scan_source_manifest(&mut db, &source_root)?;
        assert_eq!(report.discovered_source_files, 2);

        let project_count: i64 = db.connection().query_row(
            "SELECT COUNT(*) FROM project WHERE identity_kind = 'git'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(project_count, 1);

        let canonical_root: String = db.connection().query_row(
            "SELECT root_path FROM project WHERE identity_kind = 'git'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(canonical_root, bare_root.display().to_string());

        let distinct_project_ids: i64 = db.connection().query_row(
            "SELECT COUNT(DISTINCT project_id) FROM source_file",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(distinct_project_ids, 1);

        Ok(())
    }

    #[test]
    fn git_resolution_uses_the_main_repo_root_for_linked_worktrees() -> Result<()> {
        let temp = tempdir()?;
        let repo_root = temp.path().join("repo");
        let worktree_root = temp.path().join("repo-worktree");

        init_git_repo(&repo_root)?;
        run_git(
            &repo_root,
            [
                "worktree",
                "add",
                "-b",
                "feature-worktree",
                worktree_root.to_str().context("non-utf8 worktree path")?,
            ],
        )?;
        fs::create_dir_all(worktree_root.join("nested"))?;

        let project = resolve_project_from_cwd(&worktree_root.join("nested"));
        assert_eq!(project.identity_kind.as_str(), "git");
        assert_eq!(project.root_path, repo_root.clone());
        assert_eq!(project.git_root_path, Some(repo_root));

        Ok(())
    }

    #[test]
    fn scan_empty_source_root_returns_zero_counts() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("source");
        fs::create_dir_all(&source_root)?;

        let mut db = Database::open_unsharded(temp.path().join("usage.sqlite3"))?;
        let report = scan_source_manifest(&mut db, &source_root)?;

        assert_eq!(report, ScanReport::default());
        Ok(())
    }

    #[test]
    fn scan_discovers_sibling_claude_history_jsonl() -> Result<()> {
        let temp = tempdir()?;
        let claude_root = temp.path().join(".claude");
        let source_root = claude_root.join("projects");
        fs::create_dir_all(&source_root)?;
        fs::write(
            claude_root.join("history.jsonl"),
            "{\"sessionId\":\"session-1\",\"timestamp\":\"2026-03-26T08:00:00Z\",\"display\":\"hello\"}\n",
        )?;

        let mut db = Database::open_unsharded(temp.path().join("usage.sqlite3"))?;
        let report = scan_source_manifest(&mut db, &source_root)?;

        assert_eq!(report.discovered_source_files, 1);
        assert_eq!(report.inserted_source_files, 1);

        let row: (String, String) = db.connection().query_row(
            "SELECT relative_path, source_kind FROM source_file",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(row.0, "../history.jsonl");
        assert_eq!(row.1, "claude_history");

        Ok(())
    }

    #[test]
    fn scan_with_multiple_files_in_same_project_counts_correctly() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("source");
        let repo_root = temp.path().join("repo");
        let cwd = repo_root.join("work");

        init_git_repo(&repo_root)?;
        fs::create_dir_all(&cwd)?;
        write_jsonl(&source_root.join("session1.jsonl"), &cwd)?;
        write_jsonl(&source_root.join("session2.jsonl"), &cwd)?;
        write_jsonl(&source_root.join("session3.jsonl"), &cwd)?;

        let mut db = Database::open_unsharded(temp.path().join("usage.sqlite3"))?;
        let report = scan_source_manifest(&mut db, &source_root)?;

        assert_eq!(report.discovered_source_files, 3);
        assert_eq!(
            report.inserted_projects, 1,
            "all three files share one project"
        );
        assert_eq!(report.updated_projects, 0);
        assert_eq!(report.inserted_source_files, 3);
        assert_eq!(report.updated_source_files, 0);
        Ok(())
    }

    #[test]
    fn deleted_files_are_removed_on_rescan() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("source");
        let repo_root = temp.path().join("repo");
        let cwd = repo_root.join("work");

        init_git_repo(&repo_root)?;
        fs::create_dir_all(&cwd)?;

        let file1 = source_root.join("session1.jsonl");
        let file2 = source_root.join("session2.jsonl");
        write_jsonl(&file1, &cwd)?;
        write_jsonl(&file2, &cwd)?;

        let mut db = Database::open_unsharded(temp.path().join("usage.sqlite3"))?;
        let first = scan_source_manifest(&mut db, &source_root)?;
        assert_eq!(first.inserted_source_files, 2);

        fs::remove_file(&file2)?;
        let second = scan_source_manifest(&mut db, &source_root)?;

        assert_eq!(second.discovered_source_files, 1);
        assert_eq!(second.deleted_source_files, 1);

        let remaining: i64 =
            db.connection()
                .query_row("SELECT COUNT(*) FROM source_file", [], |row| row.get(0))?;
        assert_eq!(
            remaining, 1,
            "deleted file should be removed from source_file table"
        );
        Ok(())
    }

    #[test]
    fn second_scan_inserts_new_file_without_reinserting_project() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("source");
        let repo_root = temp.path().join("repo");
        let cwd = repo_root.join("work");

        init_git_repo(&repo_root)?;
        fs::create_dir_all(&cwd)?;
        write_jsonl(&source_root.join("session1.jsonl"), &cwd)?;

        let mut db = Database::open_unsharded(temp.path().join("usage.sqlite3"))?;
        let first = scan_source_manifest(&mut db, &source_root)?;
        assert_eq!(first.inserted_projects, 1);
        assert_eq!(first.inserted_source_files, 1);

        // Add a second file belonging to the same project.
        write_jsonl(&source_root.join("session2.jsonl"), &cwd)?;
        let second = scan_source_manifest(&mut db, &source_root)?;

        assert_eq!(
            second.inserted_projects, 0,
            "project already exists; must not re-insert"
        );
        assert_eq!(
            second.inserted_source_files, 1,
            "only the new file should be inserted"
        );
        assert_eq!(second.discovered_source_files, 2);
        Ok(())
    }

    #[test]
    fn deleting_an_imported_file_queues_a_pending_chunk_rebuild() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("source");
        let repo_root = temp.path().join("repo");
        let cwd = repo_root.join("work");
        let source_file_path = source_root.join("session1.jsonl");
        let sibling_file_path = source_root.join("session2.jsonl");

        init_git_repo(&repo_root)?;
        fs::create_dir_all(&cwd)?;
        write_jsonl(&source_file_path, &cwd)?;
        write_jsonl(&sibling_file_path, &cwd)?;

        let mut db = Database::open_unsharded(temp.path().join("usage.sqlite3"))?;
        let first = scan_source_manifest(&mut db, &source_root)?;
        assert_eq!(first.inserted_source_files, 2);

        db.connection().execute(
            "
            UPDATE source_file
            SET imported_modified_at_utc = modified_at_utc
            ",
            [],
        )?;

        fs::remove_file(&source_file_path)?;
        let second = scan_source_manifest(&mut db, &source_root)?;
        assert_eq!(second.deleted_source_files, 1);

        let queued: Vec<String> = db
            .connection()
            .prepare(
                "
                SELECT chunk_day_local
                FROM pending_chunk_rebuild
                ORDER BY chunk_day_local
                ",
            )?
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(queued.len(), 1);

        Ok(())
    }

    #[test]
    fn rescan_removes_orphaned_projects_after_identity_changes() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("source");
        let db_path = temp.path().join("usage.sqlite3");
        let stale_cwd = temp.path().join("agent-a").join("session-root");
        let repo_root = temp.path().join("repo");
        let canonical_cwd = repo_root.join("src");
        let source_file_path = source_root.join("session.jsonl");

        write_jsonl(&source_file_path, &stale_cwd)?;

        let mut db = Database::open_unsharded(&db_path)?;
        let first = scan_source_manifest(&mut db, &source_root)?;
        assert_eq!(first.inserted_projects, 1);
        assert_eq!(first.inserted_source_files, 1);

        let stale_project_id: i64 = db.connection().query_row(
            "SELECT id FROM project WHERE identity_kind = 'path'",
            [],
            |row| row.get(0),
        )?;
        db.connection().execute(
            "INSERT INTO import_chunk (project_id, chunk_day_local, state) VALUES (?1, ?2, ?3)",
            params![stale_project_id, "2026-03-29", "complete"],
        )?;

        init_git_repo(&repo_root)?;
        fs::create_dir_all(&canonical_cwd)?;
        write_jsonl(&source_file_path, &canonical_cwd)?;

        let second = scan_source_manifest(&mut db, &source_root)?;
        assert_eq!(second.discovered_source_files, 1);
        assert_eq!(second.inserted_projects, 1);
        assert_eq!(second.inserted_source_files, 1);
        assert_eq!(second.deleted_source_files, 1);

        let remaining_projects: Vec<(String, String)> = db
            .connection()
            .prepare(
                "
                SELECT identity_kind, root_path
                FROM project
                ORDER BY root_path
                ",
            )?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(
            remaining_projects,
            vec![("git".to_string(), repo_root.display().to_string())]
        );

        let stale_chunks: i64 = db.connection().query_row(
            "SELECT COUNT(*) FROM import_chunk WHERE project_id = ?1",
            [stale_project_id],
            |row| row.get(0),
        )?;
        assert_eq!(
            stale_chunks, 0,
            "deleting the orphaned project should cascade stale import state"
        );

        Ok(())
    }

    #[test]
    fn invalid_jsonl_produces_scan_warning() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("source");
        fs::create_dir_all(&source_root)?;

        // Write a file whose only line is not valid JSON.
        fs::write(source_root.join("bad.jsonl"), "not valid json at all\n")?;

        let mut db = Database::open_unsharded(temp.path().join("usage.sqlite3"))?;
        let report = scan_source_manifest(&mut db, &source_root)?;

        // The file is discovered and stored, but with warnings.
        assert_eq!(report.discovered_source_files, 1);

        let warnings_json: String =
            db.connection()
                .query_row("SELECT scan_warnings_json FROM source_file", [], |row| {
                    row.get(0)
                })?;
        let warnings: Vec<ScanWarning> = serde_json::from_str(&warnings_json)?;
        assert!(
            warnings.iter().any(|w| w.code == WARNING_INVALID_JSON),
            "expected an invalid_json warning; got: {warnings:?}"
        );
        Ok(())
    }

    #[test]
    fn project_filters_exclude_tmp_roots_before_manifest_upsert() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("source");
        let tmp_project = temp.path().join("tmp-root").join("sea-smoke-123");
        let repo_root = temp.path().join("repo");
        let repo_cwd = repo_root.join("work");

        fs::create_dir_all(&tmp_project)?;
        init_git_repo(&repo_root)?;
        fs::create_dir_all(&repo_cwd)?;

        write_jsonl(&source_root.join("tmp/session.jsonl"), &tmp_project)?;
        write_jsonl(&source_root.join("repo/session.jsonl"), &repo_cwd)?;

        let mut db = Database::open_unsharded(temp.path().join("usage.sqlite3"))?;
        let report = scan_source_manifest_with_policy(
            &mut db,
            &source_root,
            &ProjectIdentityPolicy::default(),
            &[ProjectFilterRule {
                action: ProjectFilterAction::Exclude,
                match_on: ProjectFilterMatchOn::ResolvedRoot,
                path_prefix: Some(temp.path().join("tmp-root").to_string_lossy().into_owned()),
                glob: None,
                equals: None,
            }],
        )?;

        assert_eq!(report.discovered_source_files, 2);
        assert_eq!(report.excluded_source_files, 1);

        let projects: Vec<(String, String)> = db
            .connection()
            .prepare("SELECT identity_kind, root_path FROM project ORDER BY root_path")?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(
            projects,
            vec![("git".to_string(), repo_root.display().to_string())]
        );

        Ok(())
    }

    fn write_jsonl(path: &Path, cwd: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        fs::write(path, format!("{}\n", json!({ "cwd": cwd })))
            .with_context(|| format!("unable to write {}", path.display()))?;
        Ok(())
    }

    fn init_git_repo(repo_root: &Path) -> Result<()> {
        fs::create_dir_all(repo_root)?;
        run_git(repo_root, ["init"])?;
        run_git(repo_root, ["config", "user.email", "gnomon@example.com"])?;
        run_git(repo_root, ["config", "user.name", "Gnomon Tests"])?;
        fs::write(repo_root.join("README.md"), "seed\n")?;
        run_git(repo_root, ["add", "."])?;
        run_git(repo_root, ["commit", "-m", "seed"])?;
        Ok(())
    }

    fn run_git<const N: usize>(repo_root: &Path, args: [&str; N]) -> Result<()> {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo_root)
            .args(args)
            .output()
            .with_context(|| format!("unable to run git {:?}", args))?;
        if !output.status.success() {
            bail!(
                "git {:?} failed: {}",
                args,
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    }

    fn run_git_args(args: &[&str]) -> Result<()> {
        let output = Command::new("git")
            .args(args)
            .output()
            .with_context(|| format!("unable to run git {:?}", args))?;
        if !output.status.success() {
            bail!(
                "git {:?} failed: {}",
                args,
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    }
}
