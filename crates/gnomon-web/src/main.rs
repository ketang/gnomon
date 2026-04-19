use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;

use anyhow::{Context, Result};
use axum::extract::{Query, State};
use axum::http::header::{self, HeaderValue};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::{Args, Parser};
use gnomon_core::config::{ConfigOverrides, RuntimeConfig};
use gnomon_core::db::Database;
use gnomon_core::import::{
    StartupOpenReason, StartupProgressUpdate, StartupWorkerEvent,
    scan_sources_manifest_with_policy, start_startup_import_with_sources_and_mode_and_progress,
};
use gnomon_core::opportunity::{OpportunityCategory, OpportunityConfidence};
use gnomon_core::query::{
    ActionKey, BrowseFilters, BrowsePath, BrowseReport, BrowseRequest, ClassificationState,
    FilterOptions, MetricLens, OpportunitiesFilters, OpportunitiesReport, QueryEngine, RollupRow,
    RootView, SnapshotBounds, SnapshotCoverageSummary, TimeWindowFilter,
};
#[cfg(test)]
use gnomon_core::sources::ConfiguredSources;
use gnomon_core::sources::SourceProvider;
use serde::{Deserialize, Serialize};

include!(concat!(env!("OUT_DIR"), "/embedded_assets.rs"));

#[derive(Debug, Parser)]
#[command(
    name = "gnomon-web",
    version,
    about = "Serve a local browser UI for exploring gnomon data."
)]
struct Cli {
    #[command(flatten)]
    global: GlobalArgs,
    #[command(flatten)]
    serve: ServeArgs,
}

#[derive(Debug, Clone, Args, Default, PartialEq, Eq)]
struct GlobalArgs {
    #[arg(long, env = "GNOMON_DB", value_name = "PATH", global = true)]
    db: Option<PathBuf>,
    #[arg(long, env = "GNOMON_SOURCE_ROOT", value_name = "PATH", global = true)]
    source_root: Option<PathBuf>,
}

#[derive(Debug, Clone, Args, PartialEq, Eq)]
struct ServeArgs {
    #[arg(long, env = "GNOMON_WEB_PORT", default_value_t = 4680)]
    port: u16,
}

#[derive(Debug, Clone)]
struct AppState {
    config: RuntimeConfig,
    startup: Arc<Mutex<StartupState>>,
    using_built_assets: bool,
}

#[derive(Debug, Clone)]
struct StartupState {
    pinned_snapshot: SnapshotBounds,
    open_reason: StartupOpenReason,
    startup_status_message: Option<String>,
    deferred_status_message: Option<String>,
    latest_progress: Option<StartupProgressUpdate>,
    import_finished: bool,
}

#[derive(Debug, Clone, Serialize)]
struct StatusResponse {
    pinned_snapshot: SnapshotBounds,
    latest_snapshot: SnapshotBounds,
    has_newer_snapshot: bool,
    coverage: SnapshotCoverageSummary,
    using_built_assets: bool,
    startup: StartupStatusResponse,
}

#[derive(Debug, Clone, Serialize)]
struct StartupStatusResponse {
    open_reason: &'static str,
    startup_status_message: Option<String>,
    deferred_status_message: Option<String>,
    latest_progress: Option<StartupProgressResponse>,
    import_finished: bool,
}

#[derive(Debug, Clone, Serialize)]
struct StartupProgressResponse {
    label: &'static str,
    current: usize,
    total: usize,
    detail: String,
}

#[derive(Debug, Deserialize)]
struct RootQuery {
    #[serde(default)]
    root: RootQueryRoot,
    #[serde(default)]
    lens: RootQueryLens,
}

#[derive(Debug, Deserialize)]
struct BrowseQuery {
    #[serde(default)]
    root: RootQueryRoot,
    #[serde(default)]
    lens: RootQueryLens,
    #[serde(default)]
    path: BrowsePathKind,
    project_id: Option<i64>,
    category: Option<String>,
    parent_path: Option<String>,
    start_at_utc: Option<String>,
    end_at_utc: Option<String>,
    provider: Option<ProviderQuery>,
    model: Option<String>,
    filter_category: Option<String>,
    classification_state: Option<ClassificationStateQuery>,
    normalized_action: Option<String>,
    command_family: Option<String>,
    base_command: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpportunitiesQuery {
    project_id: Option<i64>,
    provider: Option<ProviderQuery>,
    category: Option<OpportunityCategoryQuery>,
    min_confidence: Option<OpportunityConfidenceQuery>,
    #[serde(default)]
    min_score: f64,
    start_at_utc: Option<String>,
    end_at_utc: Option<String>,
    #[serde(default)]
    include_empty: bool,
}

#[derive(Debug, Deserialize)]
struct DetailQuery {
    #[serde(flatten)]
    browse: BrowseQuery,
    row_key: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RootQueryRoot {
    #[default]
    Project,
    Category,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RootQueryLens {
    #[default]
    UncachedInput,
    GrossInput,
    Output,
    Total,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BrowsePathKind {
    #[default]
    Root,
    Project,
    ProjectCategory,
    ProjectAction,
    Category,
    CategoryAction,
    CategoryActionProject,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ClassificationStateQuery {
    Classified,
    Mixed,
    Unclassified,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProviderQuery {
    Claude,
    Codex,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OpportunityCategoryQuery {
    SessionSetup,
    TaskSetup,
    HistoryDrag,
    Delegation,
    ModelMismatch,
    PromptYield,
    SearchChurn,
    ToolResultBloat,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OpportunityConfidenceQuery {
    Low,
    Medium,
    High,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug, Serialize)]
struct DetailResponse {
    snapshot: SnapshotBounds,
    request: BrowseRequest,
    row: RollupRow,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let config = RuntimeConfig::load(ConfigOverrides {
        db_path: cli.global.db,
        source_root: cli.global.source_root,
        ..Default::default()
    })?;
    let app = build_router(build_app_state(config)?);
    let addr = local_bind_addr(cli.serve.port);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("unable to bind local web listener on {addr}"))?;
    tracing::info!("gnomon-web listening on http://{addr}");
    axum::serve(listener, app)
        .await
        .context("gnomon-web server exited unexpectedly")
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt::try_init();
}
fn local_bind_addr(port: u16) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
}

fn build_app_state(config: RuntimeConfig) -> Result<AppState> {
    config.ensure_dirs()?;
    let mut database = Database::open(&config.db_path)?;
    let _scan = scan_sources_manifest_with_policy(
        &mut database,
        &config.sources,
        &config.project_identity,
        &config.project_filters,
    )?;
    let mut startup_import = start_startup_import_with_sources_and_mode_and_progress(
        database.connection(),
        &config.db_path,
        &config.sources,
        gnomon_core::import::StartupImportMode::RecentFirst,
        |_| {},
    )?;
    let startup = Arc::new(Mutex::new(StartupState {
        pinned_snapshot: startup_import.snapshot.clone(),
        open_reason: startup_import.open_reason,
        startup_status_message: startup_import.startup_status_message.clone(),
        deferred_status_message: None,
        latest_progress: startup_import.startup_progress_update.clone(),
        import_finished: false,
    }));
    if let Some(receiver) = startup_import.take_status_updates() {
        let state = Arc::clone(&startup);
        thread::Builder::new()
            .name("gnomon-web-import-monitor".into())
            .spawn(move || {
                while let Ok(event) = receiver.recv() {
                    let mut startup = state.lock().expect("startup state mutex poisoned");
                    match event {
                        StartupWorkerEvent::Progress { update } => {
                            startup.latest_progress = Some(update)
                        }
                        StartupWorkerEvent::StartupSettled {
                            startup_status_message,
                        } => startup.startup_status_message = startup_status_message,
                        StartupWorkerEvent::DeferredFailures {
                            deferred_status_message,
                        } => startup.deferred_status_message = deferred_status_message,
                        StartupWorkerEvent::Finished => {
                            startup.import_finished = true;
                            break;
                        }
                    }
                }
            })
            .context("unable to spawn gnomon-web import monitor thread")?;
    } else {
        startup
            .lock()
            .expect("startup state mutex poisoned")
            .import_finished = true;
    }
    Ok(AppState {
        config,
        startup,
        using_built_assets: USING_BUILT_ASSETS,
    })
}

fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/app.js", get(app_js))
        .route("/app.css", get(app_css))
        .route("/api/status", get(status))
        .route("/api/refresh", post(refresh))
        .route("/api/root", get(root_rollup))
        .route("/api/browse", get(browse))
        .route("/api/detail", get(detail))
        .route("/api/filters", get(filter_options))
        .route("/api/opportunities", get(opportunities))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}
async fn app_js() -> Response {
    asset_response("text/javascript; charset=utf-8", APP_JS)
}
async fn app_css() -> Response {
    asset_response("text/css; charset=utf-8", APP_CSS)
}
async fn status(State(state): State<AppState>) -> Result<Json<StatusResponse>, AppError> {
    status_impl(state, false).await
}
async fn refresh(State(state): State<AppState>) -> Result<Json<StatusResponse>, AppError> {
    status_impl(state, true).await
}

async fn status_impl(
    state: AppState,
    refresh_requested: bool,
) -> Result<Json<StatusResponse>, AppError> {
    let database = Database::open_read_only(&state.config.db_path)?;
    let engine = QueryEngine::new(database.connection());
    let latest_snapshot = engine.latest_snapshot_bounds()?;
    let startup = if refresh_requested {
        let mut startup = lock_startup_state(&state)?;
        if latest_snapshot.max_publish_seq > startup.pinned_snapshot.max_publish_seq {
            startup.pinned_snapshot = latest_snapshot.clone();
        }
        startup.clone()
    } else {
        snapshot_startup_state(&state)?
    };
    let coverage = engine.snapshot_coverage_summary(&startup.pinned_snapshot)?;
    Ok(Json(StatusResponse {
        pinned_snapshot: startup.pinned_snapshot.clone(),
        latest_snapshot: latest_snapshot.clone(),
        has_newer_snapshot: latest_snapshot.max_publish_seq
            > startup.pinned_snapshot.max_publish_seq,
        coverage,
        using_built_assets: state.using_built_assets,
        startup: StartupStatusResponse {
            open_reason: match startup.open_reason {
                StartupOpenReason::Last24hReady => "last_24h_ready",
                StartupOpenReason::FullImportReady => "full_import_ready",
                StartupOpenReason::TimedOut => "timed_out",
            },
            startup_status_message: startup.startup_status_message,
            deferred_status_message: startup.deferred_status_message,
            latest_progress: startup.latest_progress.map(Into::into),
            import_finished: startup.import_finished,
        },
    }))
}

async fn root_rollup(
    State(state): State<AppState>,
    Query(query): Query<RootQuery>,
) -> Result<Json<BrowseReport>, AppError> {
    let query = BrowseQuery {
        root: query.root,
        lens: query.lens,
        path: BrowsePathKind::Root,
        project_id: None,
        category: None,
        parent_path: None,
        start_at_utc: None,
        end_at_utc: None,
        provider: None,
        model: None,
        filter_category: None,
        classification_state: None,
        normalized_action: None,
        command_family: None,
        base_command: None,
    };
    browse_impl(state, query).await
}
async fn browse(
    State(state): State<AppState>,
    Query(query): Query<BrowseQuery>,
) -> Result<Json<BrowseReport>, AppError> {
    browse_impl(state, query).await
}
async fn browse_impl(state: AppState, query: BrowseQuery) -> Result<Json<BrowseReport>, AppError> {
    let startup = snapshot_startup_state(&state)?;
    let database = Database::open_read_only(&state.config.db_path)?;
    let engine = QueryEngine::new(database.connection());
    Ok(Json(engine.browse_report(
        query.build_request(startup.pinned_snapshot)?,
    )?))
}
async fn detail(
    State(state): State<AppState>,
    Query(query): Query<DetailQuery>,
) -> Result<Json<DetailResponse>, AppError> {
    let startup = snapshot_startup_state(&state)?;
    let request = query.build_request(startup.pinned_snapshot)?;
    let database = Database::open_read_only(&state.config.db_path)?;
    let engine = QueryEngine::new(database.connection());
    let report = engine.browse_report(request.clone())?;
    let row_key = query.required_row_key()?;
    let row = report
        .rows
        .into_iter()
        .find(|row| row.key == row_key)
        .ok_or_else(|| AppError::not_found(format!("row_key `{row_key}` was not found")))?;
    Ok(Json(DetailResponse {
        snapshot: report.snapshot,
        request,
        row,
    }))
}
async fn filter_options(State(state): State<AppState>) -> Result<Json<FilterOptions>, AppError> {
    let startup = snapshot_startup_state(&state)?;
    let database = Database::open_read_only(&state.config.db_path)?;
    let engine = QueryEngine::new(database.connection());
    Ok(Json(engine.filter_options(&startup.pinned_snapshot)?))
}
async fn opportunities(
    State(state): State<AppState>,
    Query(query): Query<OpportunitiesQuery>,
) -> Result<Json<OpportunitiesReport>, AppError> {
    let startup = snapshot_startup_state(&state)?;
    let database = Database::open_read_only(&state.config.db_path)?;
    let engine = QueryEngine::new(database.connection());
    let mut report = engine.opportunities_report(&startup.pinned_snapshot, &query.to_filters())?;
    if query.category.is_some() || query.min_confidence.is_some() || query.min_score > 0.0 {
        let category = query.category.map(Into::into);
        let min_confidence = query.min_confidence.map(Into::into);
        let min_score = query.min_score;
        for row in &mut report.rows {
            row.opportunities.annotations.retain(|annotation| {
                category.is_none_or(|c| annotation.category == c)
                    && min_confidence.is_none_or(|c| annotation.confidence >= c)
                    && annotation.score >= min_score
            });
        }
        if !query.include_empty {
            report
                .rows
                .retain(|row| !row.opportunities.annotations.is_empty());
        }
    }
    Ok(Json(report))
}

fn snapshot_startup_state(state: &AppState) -> Result<StartupState, AppError> {
    lock_startup_state(state).map(|g| g.clone())
}
fn lock_startup_state(
    state: &AppState,
) -> Result<std::sync::MutexGuard<'_, StartupState>, AppError> {
    state
        .startup
        .lock()
        .map_err(|_| AppError::internal("startup state mutex poisoned"))
}
fn asset_response(content_type: &'static str, body: &'static str) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    (StatusCode::OK, headers, body).into_response()
}

impl From<RootQueryRoot> for RootView {
    fn from(value: RootQueryRoot) -> Self {
        match value {
            RootQueryRoot::Project => RootView::ProjectHierarchy,
            RootQueryRoot::Category => RootView::CategoryHierarchy,
        }
    }
}
impl From<RootQueryLens> for MetricLens {
    fn from(value: RootQueryLens) -> Self {
        match value {
            RootQueryLens::UncachedInput => MetricLens::UncachedInput,
            RootQueryLens::GrossInput => MetricLens::GrossInput,
            RootQueryLens::Output => MetricLens::Output,
            RootQueryLens::Total => MetricLens::Total,
        }
    }
}
impl From<ClassificationStateQuery> for ClassificationState {
    fn from(value: ClassificationStateQuery) -> Self {
        match value {
            ClassificationStateQuery::Classified => ClassificationState::Classified,
            ClassificationStateQuery::Mixed => ClassificationState::Mixed,
            ClassificationStateQuery::Unclassified => ClassificationState::Unclassified,
        }
    }
}
impl From<ProviderQuery> for SourceProvider {
    fn from(value: ProviderQuery) -> Self {
        match value {
            ProviderQuery::Claude => SourceProvider::Claude,
            ProviderQuery::Codex => SourceProvider::Codex,
        }
    }
}
impl From<OpportunityCategoryQuery> for OpportunityCategory {
    fn from(value: OpportunityCategoryQuery) -> Self {
        match value {
            OpportunityCategoryQuery::SessionSetup => OpportunityCategory::SessionSetup,
            OpportunityCategoryQuery::TaskSetup => OpportunityCategory::TaskSetup,
            OpportunityCategoryQuery::HistoryDrag => OpportunityCategory::HistoryDrag,
            OpportunityCategoryQuery::Delegation => OpportunityCategory::Delegation,
            OpportunityCategoryQuery::ModelMismatch => OpportunityCategory::ModelMismatch,
            OpportunityCategoryQuery::PromptYield => OpportunityCategory::PromptYield,
            OpportunityCategoryQuery::SearchChurn => OpportunityCategory::SearchChurn,
            OpportunityCategoryQuery::ToolResultBloat => OpportunityCategory::ToolResultBloat,
        }
    }
}
impl From<OpportunityConfidenceQuery> for OpportunityConfidence {
    fn from(value: OpportunityConfidenceQuery) -> Self {
        match value {
            OpportunityConfidenceQuery::Low => OpportunityConfidence::Low,
            OpportunityConfidenceQuery::Medium => OpportunityConfidence::Medium,
            OpportunityConfidenceQuery::High => OpportunityConfidence::High,
        }
    }
}
impl From<StartupProgressUpdate> for StartupProgressResponse {
    fn from(value: StartupProgressUpdate) -> Self {
        Self {
            label: value.label,
            current: value.current,
            total: value.total,
            detail: value.detail,
        }
    }
}

impl BrowseQuery {
    fn build_request(&self, snapshot: SnapshotBounds) -> Result<BrowseRequest, AppError> {
        Ok(BrowseRequest {
            snapshot,
            root: self.root.into(),
            lens: self.lens.into(),
            filters: self.filters(),
            path: self.build_path()?,
        })
    }
    fn filters(&self) -> BrowseFilters {
        let time_window = if self.start_at_utc.is_some() || self.end_at_utc.is_some() {
            Some(TimeWindowFilter {
                start_at_utc: self.start_at_utc.clone(),
                end_at_utc: self.end_at_utc.clone(),
            })
        } else {
            None
        };
        BrowseFilters {
            time_window,
            provider: self.provider.map(Into::into),
            model: self.model.clone(),
            project_id: self.project_id,
            action_category: self.filter_category.clone(),
            action: None,
        }
    }
    fn build_path(&self) -> Result<BrowsePath, AppError> {
        match self.path {
            BrowsePathKind::Root => Ok(BrowsePath::Root),
            BrowsePathKind::Project => Ok(BrowsePath::Project {
                project_id: self.required_project_id("project path")?,
            }),
            BrowsePathKind::ProjectCategory => Ok(BrowsePath::ProjectCategory {
                project_id: self.required_project_id("project-category path")?,
                category: self.required_category("project-category path")?,
            }),
            BrowsePathKind::ProjectAction => Ok(BrowsePath::ProjectAction {
                project_id: self.required_project_id("project-action path")?,
                category: self.required_category("project-action path")?,
                action: self.required_action("project-action path")?,
                parent_path: self.parent_path.clone(),
            }),
            BrowsePathKind::Category => Ok(BrowsePath::Category {
                category: self.required_category("category path")?,
            }),
            BrowsePathKind::CategoryAction => Ok(BrowsePath::CategoryAction {
                category: self.required_category("category-action path")?,
                action: self.required_action("category-action path")?,
            }),
            BrowsePathKind::CategoryActionProject => Ok(BrowsePath::CategoryActionProject {
                category: self.required_category("category-action-project path")?,
                action: self.required_action("category-action-project path")?,
                project_id: self.required_project_id("category-action-project path")?,
                parent_path: self.parent_path.clone(),
            }),
        }
    }
    fn required_project_id(&self, context: &str) -> Result<i64, AppError> {
        self.project_id
            .ok_or_else(|| AppError::bad_request(format!("{context} requires project_id")))
    }
    fn required_category(&self, context: &str) -> Result<String, AppError> {
        self.category
            .clone()
            .ok_or_else(|| AppError::bad_request(format!("{context} requires category")))
    }
    fn required_action(&self, context: &str) -> Result<ActionKey, AppError> {
        Ok(ActionKey {
            classification_state: self.classification_state.map(Into::into).ok_or_else(|| {
                AppError::bad_request(format!("{context} requires classification_state"))
            })?,
            normalized_action: self.normalized_action.clone(),
            command_family: self.command_family.clone(),
            base_command: self.base_command.clone(),
        })
    }
}
impl DetailQuery {
    fn build_request(&self, snapshot: SnapshotBounds) -> Result<BrowseRequest, AppError> {
        self.browse.build_request(snapshot)
    }
    fn required_row_key(&self) -> Result<&str, AppError> {
        self.row_key
            .as_deref()
            .ok_or_else(|| AppError::bad_request("detail request requires row_key"))
    }
}
impl OpportunitiesQuery {
    fn to_filters(&self) -> OpportunitiesFilters {
        OpportunitiesFilters {
            provider: self.provider.map(Into::into),
            project_id: self.project_id,
            start_at_utc: self.start_at_utc.clone(),
            end_at_utc: self.end_at_utc.clone(),
            include_empty: self.include_empty,
        }
    }
}

#[derive(Debug)]
struct AppError {
    status: StatusCode,
    message: String,
}
impl AppError {
    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }
    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}
impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(error: E) -> Self {
        Self::internal(format!("{:#}", error.into()))
    }
}
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        (
            self.status,
            headers,
            Json(ErrorResponse {
                error: self.message,
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{self, Body};
    use axum::http::Request;
    use serde_json::Value;
    use tempfile::TempDir;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn serves_index_html() {
        let app = build_router(test_app_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/")
                    .body(Body::empty())
                    .expect("building index request should succeed"),
            )
            .await
            .expect("index request should succeed");
        assert_eq!(response.status(), StatusCode::OK);
        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("reading index response body should succeed");
        let text = String::from_utf8(body.to_vec()).expect("index response should be valid utf-8");
        assert!(text.contains("<!doctype html>"));
    }

    #[tokio::test]
    async fn status_reports_bootstrap_snapshot_for_empty_database() {
        let (app, _temp) = test_router_with_temp_db();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/status")
                    .body(Body::empty())
                    .expect("building status request should succeed"),
            )
            .await
            .expect("status request should succeed");
        assert_eq!(response.status(), StatusCode::OK);
        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("reading status response body should succeed");
        let json: Value =
            serde_json::from_slice(&body).expect("status response should be valid json");
        assert_eq!(json["pinned_snapshot"]["max_publish_seq"], 0);
    }

    #[tokio::test]
    async fn detail_route_returns_bad_request_when_row_key_is_missing() {
        let (app, _temp) = test_router_with_temp_db();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/detail")
                    .body(Body::empty())
                    .expect("building detail request should succeed"),
            )
            .await
            .expect("detail request should succeed");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    fn test_app_state() -> AppState {
        let source_root = PathBuf::from("/tmp/gnomon-web-test/source");
        AppState {
            config: RuntimeConfig {
                app_name: "gnomon",
                state_dir: PathBuf::from("/tmp/gnomon-web-test"),
                config_path: PathBuf::from("/tmp/gnomon-web-test/config.toml"),
                db_path: PathBuf::from("/tmp/gnomon-web-test/usage.sqlite3"),
                source_root: source_root.clone(),
                sources: ConfiguredSources::legacy_claude(&source_root),
                project_identity: Default::default(),
                project_filters: Vec::new(),
            },
            startup: Arc::new(Mutex::new(StartupState {
                pinned_snapshot: SnapshotBounds::bootstrap(),
                open_reason: StartupOpenReason::Last24hReady,
                startup_status_message: None,
                deferred_status_message: None,
                latest_progress: None,
                import_finished: true,
            })),
            using_built_assets: USING_BUILT_ASSETS,
        }
    }
    fn test_router_with_temp_db() -> (Router, TempDir) {
        let temp = tempfile::tempdir().expect("creating temp dir for test db should succeed");
        let root = temp.path().to_path_buf();
        let db_path = root.join("usage.sqlite3");
        let source_root = root.join("source");
        Database::open(&db_path).expect("opening test database should succeed");
        let app_state = AppState {
            config: RuntimeConfig {
                app_name: "gnomon",
                state_dir: root.join("state"),
                config_path: root.join("config.toml"),
                db_path,
                source_root: source_root.clone(),
                sources: ConfiguredSources::legacy_claude(&source_root),
                project_identity: Default::default(),
                project_filters: Vec::new(),
            },
            startup: Arc::new(Mutex::new(StartupState {
                pinned_snapshot: SnapshotBounds::bootstrap(),
                open_reason: StartupOpenReason::Last24hReady,
                startup_status_message: None,
                deferred_status_message: None,
                latest_progress: None,
                import_finished: true,
            })),
            using_built_assets: USING_BUILT_ASSETS,
        };
        (build_router(app_state), temp)
    }
}
