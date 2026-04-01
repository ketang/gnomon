use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use jiff::Timestamp;
use rusqlite::{OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use crate::config::{
    ProjectFilterAction, ProjectFilterContext, ProjectFilterRule, ProjectIdentityPolicy,
};
use crate::db::Database;
use crate::vcs::{self, ProjectIdentityKind, ResolvedProject};

const WARNING_INVALID_JSON: &str = "invalid_json";
const WARNING_MISSING_CWD: &str = "missing_cwd";
const WARNING_PATH_PROJECT: &str = "path_project";

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
    modified_at_utc: Option<String>,
    size_bytes: i64,
    scan_warnings_json: String,
}

pub fn scan_source_manifest(database: &mut Database, source_root: &Path) -> Result<ScanReport> {
    scan_source_manifest_with_policy(
        database,
        source_root,
        &ProjectIdentityPolicy::default(),
        &[],
    )
}

pub fn scan_source_manifest_with_policy(
    database: &mut Database,
    source_root: &Path,
    identity_policy: &ProjectIdentityPolicy,
    project_filters: &[ProjectFilterRule],
) -> Result<ScanReport> {
    if !source_root.exists() {
        return Ok(ScanReport::default());
    }
    if !source_root.is_dir() {
        bail!("source root {} is not a directory", source_root.display());
    }

    let discovery = discover_source_files(source_root, identity_policy, project_filters)?;
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

    report.deleted_source_files = delete_missing_source_files(&tx, &seen_files)?;
    delete_orphaned_projects(&tx)?;
    tx.commit()
        .context("unable to commit the source manifest scan transaction")?;

    Ok(report)
}

#[derive(Debug)]
struct DiscoveryResult {
    files: Vec<DiscoveredSourceFile>,
    discovered_source_files: usize,
    excluded_source_files: usize,
}

fn discover_source_files(
    source_root: &Path,
    identity_policy: &ProjectIdentityPolicy,
    project_filters: &[ProjectFilterRule],
) -> Result<DiscoveryResult> {
    let mut discovered_files = Vec::new();
    let mut discovered_source_files = 0usize;
    let mut excluded_source_files = 0usize;

    for entry in WalkDir::new(source_root) {
        let entry = entry
            .with_context(|| format!("unable to walk source root {}", source_root.display()))?;
        if !entry.file_type().is_file() {
            continue;
        }
        if entry.path().extension().and_then(|ext| ext.to_str()) != Some("jsonl") {
            continue;
        }

        discovered_source_files += 1;
        if let Some(file) =
            discover_source_file(source_root, entry.path(), identity_policy, project_filters)?
        {
            discovered_files.push(file);
        } else {
            excluded_source_files += 1;
        }
    }

    discovered_files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(DiscoveryResult {
        files: discovered_files,
        discovered_source_files,
        excluded_source_files,
    })
}

fn discover_source_file(
    source_root: &Path,
    source_file_path: &Path,
    identity_policy: &ProjectIdentityPolicy,
    project_filters: &[ProjectFilterRule],
) -> Result<Option<DiscoveredSourceFile>> {
    let metadata = fs::metadata(source_file_path)
        .with_context(|| format!("unable to read metadata for {}", source_file_path.display()))?;
    let relative_path = source_file_path
        .strip_prefix(source_root)
        .with_context(|| {
            format!(
                "unable to express {} relative to {}",
                source_file_path.display(),
                source_root.display()
            )
        })?
        .to_string_lossy()
        .into_owned();
    let size_bytes = i64::try_from(metadata.len())
        .with_context(|| format!("source file {} is too large", source_file_path.display()))?;
    let modified_at_utc = modified_at_utc(&metadata).with_context(|| {
        format!(
            "unable to read modified time for {}",
            source_file_path.display()
        )
    })?;

    let ExtractedCwd { cwd, mut warnings } = extract_cwd(source_file_path)?;
    let raw_cwd = cwd.clone();
    let project = match cwd {
        Some(cwd) => vcs::resolve_project_from_cwd_with_policy(&cwd, identity_policy),
        None => vcs::path_project(source_file_path, vcs::PATH_REASON_MISSING_CWD),
    };

    if !identity_policy.fallback_path_projects && project.identity_kind == ProjectIdentityKind::Path
    {
        return Ok(None);
    }

    if should_exclude_project(raw_cwd.as_deref(), &project, project_filters)? {
        return Ok(None);
    }

    if project.identity_kind == ProjectIdentityKind::Path
        && let Some(reason) = &project.identity_reason
    {
        warnings.push(ScanWarning::new(WARNING_PATH_PROJECT, reason.clone()));
    }

    let scan_warnings_json = serde_json::to_string(&warnings).with_context(|| {
        format!(
            "unable to serialize scan warnings for {}",
            source_file_path.display()
        )
    })?;

    Ok(Some(DiscoveredSourceFile {
        project,
        relative_path,
        modified_at_utc,
        size_bytes,
        scan_warnings_json,
    }))
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
                    modified_at_utc: row.get(1)?,
                    size_bytes: row.get(2)?,
                    scan_warnings_json: row.get(3)?,
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
                    modified_at_utc,
                    size_bytes,
                    scan_warnings_json
                ) VALUES (?1, ?2, ?3, ?4, ?5)
                ",
                params![
                    project_id,
                    file.relative_path,
                    file.modified_at_utc,
                    file.size_bytes,
                    file.scan_warnings_json,
                ],
            )
            .context("unable to insert a discovered source file into the manifest")?;
            report.inserted_source_files += 1;
        }
        Some(existing) => {
            let needs_update = existing.modified_at_utc != file.modified_at_utc
                || existing.size_bytes != file.size_bytes
                || existing.scan_warnings_json != file.scan_warnings_json;

            if needs_update {
                tx.execute(
                    "
                    UPDATE source_file
                    SET
                        modified_at_utc = ?2,
                        size_bytes = ?3,
                        scan_warnings_json = ?4
                    WHERE id = ?1
                    ",
                    params![
                        existing.id,
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
        .prepare("SELECT id, project_id, relative_path FROM source_file")
        .context("unable to prepare source file manifest reconciliation query")?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .context("unable to enumerate source file manifest rows")?;

    let mut delete_ids = Vec::new();
    for row in rows {
        let (id, project_id, relative_path) =
            row.context("unable to read a source file manifest row")?;
        if !seen_files.contains(&(project_id, relative_path)) {
            delete_ids.push(id);
        }
    }
    drop(stmt);

    for id in &delete_ids {
        tx.execute("DELETE FROM source_file WHERE id = ?1", [id])
            .with_context(|| format!("unable to delete stale source file manifest row {id}"))?;
    }

    Ok(delete_ids.len())
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
        ScanReport, ScanWarning, WARNING_INVALID_JSON, scan_source_manifest,
        scan_source_manifest_with_policy,
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

        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
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

        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
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

        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
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

        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
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

        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
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

        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
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

        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
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

        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let report = scan_source_manifest(&mut db, &source_root)?;

        assert_eq!(report, ScanReport::default());
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

        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
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

        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
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

        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
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
    fn rescan_removes_orphaned_projects_after_identity_changes() -> Result<()> {
        let temp = tempdir()?;
        let source_root = temp.path().join("source");
        let db_path = temp.path().join("usage.sqlite3");
        let stale_cwd = temp.path().join("agent-a").join("session-root");
        let repo_root = temp.path().join("repo");
        let canonical_cwd = repo_root.join("src");
        let source_file_path = source_root.join("session.jsonl");

        write_jsonl(&source_file_path, &stale_cwd)?;

        let mut db = Database::open(&db_path)?;
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

        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
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

        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
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
