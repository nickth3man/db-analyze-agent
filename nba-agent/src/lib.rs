pub mod agent;
pub mod db;
use ipnet::IpNet;

use axum::{
    Json, Router,
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, HeaderValue, Request, StatusCode},
    middleware::{self, Next},
    response::{
        IntoResponse, Response,
        sse::{Event, Sse},
    },
    routing::{get, post},
};
use futures::StreamExt;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Instant,
};
use tower_http::{
    compression::CompressionLayer,
    cors::{AllowOrigin, CorsLayer},
    services::ServeDir,
};

/// Per-IP rate limiter.
///
/// Keyed by client IP. The middleware determines the effective client IP via
/// `X-Forwarded-For` ONLY when the direct connection peer is a trusted proxy
/// (CIDR list in `TRUSTED_PROXIES`); otherwise the peer IP is used. Bounded
/// to a max number of distinct tracked IPs so the map cannot grow without
/// limit.
#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<Mutex<HashMap<IpAddr, VecDeque<Instant>>>>,
    max_requests: usize,
    window_secs: u64,
    max_tracked_ips: usize,
    trusted_proxies: Arc<Vec<IpNet>>,
}

impl RateLimiter {
    pub fn new(max_requests: usize, window_secs: u64) -> Self {
        let trusted_proxies = std::env::var("TRUSTED_PROXIES")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|list| list.split(',').filter_map(|s| s.trim().parse::<IpNet>().ok()).collect::<Vec<_>>())
            .unwrap_or_default();
        if !trusted_proxies.is_empty() {
            tracing::info!("RateLimiter: X-Forwarded-For will be honored for connections from {:?}", trusted_proxies);
        } else {
            tracing::warn!(
                "RateLimiter: TRUSTED_PROXIES not set — X-Forwarded-For is IGNORED. \
                 Direct peer IP is always used. Configure TRUSTED_PROXIES=10.0.0.0/8 \
                 (comma-separated CIDRs) if running behind a reverse proxy."
            );
        }
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            max_requests,
            window_secs,
            max_tracked_ips: 10_000,
            trusted_proxies: Arc::new(trusted_proxies),
        }
    }

    #[cfg(test)]
    fn with_trusted_proxies(max_requests: usize, window_secs: u64, proxies: Vec<IpNet>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            max_requests,
            window_secs,
            max_tracked_ips: 10_000,
            trusted_proxies: Arc::new(proxies),
        }
    }
    /// Resolve the client IP for a given peer, honoring XFF only when the
    /// peer is a trusted proxy.
    pub fn client_ip_for(&self, headers: &HeaderMap, peer: IpAddr) -> IpAddr {
        let peer_is_trusted = self.trusted_proxies.iter().any(|net| net.contains(&peer));
        if !peer_is_trusted {
            return peer;
        }
        if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
            // XFF is `client, proxy1, proxy2`; leftmost is the original client.
            if let Some(first) = xff.split(',').next() {
                if let Ok(ip) = first.trim().parse::<IpAddr>() {
                    return ip;
                }
            }
        }
        peer
    }

    /// Records a request from `client_ip` and returns `true` if it is allowed.
    pub fn check(&self, client_ip: IpAddr) -> bool {
        let mut entries = self.inner.lock();
        let now = Instant::now();
        let window = std::time::Duration::from_secs(self.window_secs);

        // If at cap and this is a new IP, reject before we even track it.
        if !entries.contains_key(&client_ip) && entries.len() >= self.max_tracked_ips {
            return false;
        }

        let queue = entries.entry(client_ip).or_default();
        queue.retain(|t| now.duration_since(*t) < window);
        if queue.len() >= self.max_requests {
            return false;
        }
        queue.push_back(now);
        true
    }
}

/// Fallback peer IP when the test/runtime doesn't supply a `ConnectInfo`
/// (e.g. `tower::ServiceExt::oneshot`).
fn unknown_peer() -> IpAddr {
    IpAddr::from([0, 0, 0, 0])
}

pub async fn rate_limit_middleware(
    State(limiter): State<RateLimiter>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    // ConnectInfo<SocketAddr> is only present when axum has a real peer addr.
    // `from_request_parts` would 500 without it, so we extract via the
    // extension map populated by axum's ConnectInfo layer instead.
    let peer = request.extensions().get::<ConnectInfo<SocketAddr>>().map(|c| c.0.ip()).unwrap_or_else(unknown_peer);
    let client_ip = limiter.client_ip_for(request.headers(), peer);
    if !limiter.check(client_ip) {
        return (StatusCode::TOO_MANY_REQUESTS, "Rate limit exceeded.").into_response();
    }
    next.run(request).await
}

/// Optional bearer-token auth.
///
/// If `API_KEY` env var is set, all `/api/*` routes (except `/api/health`)
/// require `Authorization: Bearer <API_KEY>`. If unset, the server runs in
/// unauthenticated dev mode and logs a one-time warning.
#[derive(Clone)]
pub struct ApiAuth {
    key: Option<String>,
}

impl ApiAuth {
    pub fn from_env() -> Self {
        let key = std::env::var("API_KEY").ok().filter(|s| !s.is_empty());
        if key.is_none() {
            tracing::warn!(
                "API_KEY not set — server is running in unauthenticated dev mode. \
                 Set API_KEY env var to require bearer-token auth on /api/* routes."
            );
        } else {
            tracing::info!("API_KEY set — /api/* routes require Authorization: Bearer <key>");
        }
        Self { key }
    }

    pub fn is_enabled(&self) -> bool {
        self.key.is_some()
    }
}

pub async fn auth_middleware(State(auth): State<ApiAuth>, request: Request<axum::body::Body>, next: Next) -> Response {
    let expected = match &auth.key {
        Some(k) => k,
        // Auth disabled — pass through.
        None => return next.run(request).await,
    };

    let path = request.uri().path();
    // Public-by-policy endpoints (still useful unauthenticated for liveness probes).
    if path == "/api/health" {
        return next.run(request).await;
    }

    let supplied =
        request.headers().get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "));

    match supplied {
        Some(token) if constant_time_eq(token.as_bytes(), expected.as_bytes()) => next.run(request).await,
        _ => (StatusCode::UNAUTHORIZED, "Unauthorized").into_response(),
    }
}

/// Constant-time byte comparison to prevent timing-leak auth bypasses.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[derive(Clone)]
pub struct AppState {
    pub agent: Arc<agent::Agent>,
    pub db: db::DbContext,
    pub insights: Arc<db::InsightsResponse>,
    pub started_at: std::time::Instant,
}

#[derive(Deserialize)]
struct ChatRequest {
    session_id: Option<String>,
    message: String,
}

#[derive(Deserialize)]
struct StreamQuery {
    session_id: Option<String>,
    message: String,
}

#[derive(Deserialize)]
struct ResetRequest {
    session_id: String,
}

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    database: String,
}

pub async fn build_state(db: db::DbContext, sessions_path: String) -> anyhow::Result<AppState> {
    let insights = Arc::new(db.generate_insights().await);
    let insights_brief = db::DbContext::format_insights_for_prompt(&insights);
    let agent = Arc::new(agent::Agent::new(db.clone(), insights_brief, sessions_path).await?);
    Ok(AppState { agent, db, insights, started_at: std::time::Instant::now() })
}

fn build_cors_layer() -> CorsLayer {
    // SEC-002: configurable origin allowlist. Comma-separated origins via
    // `CORS_ALLOWED_ORIGINS`. If unset (or empty), fall back to permissive
    // dev mode with a one-time warning.
    match std::env::var("CORS_ALLOWED_ORIGINS").ok().filter(|s| !s.is_empty()) {
        Some(list) => {
            let origins: Vec<HeaderValue> =
                list.split(',').filter_map(|o| HeaderValue::from_str(o.trim()).ok()).collect();
            tracing::info!("CORS allowlist: {:?}", origins);
            CorsLayer::new()
                .allow_origin(AllowOrigin::list(origins))
                .allow_methods(tower_http::cors::Any)
                .allow_headers(tower_http::cors::Any)
        }
        None => {
            tracing::warn!(
                "CORS_ALLOWED_ORIGINS not set — using permissive CORS (any origin). \
                 Set CORS_ALLOWED_ORIGINS=https://your.domain for production."
            );
            CorsLayer::permissive()
        }
    }
}

pub fn build_router(state: AppState) -> Router {
    let limiter = RateLimiter::new(60, 60);
    let auth = ApiAuth::from_env();

    Router::new()
        .route("/api/chat", post(chat_handler))
        .route("/api/chat/stream", get(chat_stream_handler))
        .route("/api/reset", post(reset_handler))
        .route("/api/health", get(health_handler))
        .route("/api/test-query", get(test_query_handler))
        .route("/api/insights", get(insights_handler))
        .route("/api/export", get(export_handler))
        .route("/api/history", get(history_handler))
        .route("/api/stats", get(stats_handler))
        .route_layer(middleware::from_fn_with_state(limiter, rate_limit_middleware))
        .route_layer(middleware::from_fn_with_state(auth, auth_middleware))
        .layer(CompressionLayer::new())
        .layer(build_cors_layer())
        .fallback_service(ServeDir::new("static"))
        .with_state(state)
}

async fn chat_handler(
    State(state): State<AppState>,
    Json(payload): Json<ChatRequest>,
) -> Result<Json<agent::ConversationTrace>, (StatusCode, String)> {
    state.agent.run_conversation(payload.session_id, &payload.message).await.map(Json).map_err(|e| {
        tracing::error!("Error processing chat conversation: {}", e);
        (StatusCode::BAD_GATEWAY, format!("Agent error: {}", e))
    })
}

async fn chat_stream_handler(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(query): Query<StreamQuery>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let stream = state.agent.run_conversation_stream(query.session_id, query.message);

    let event_stream = stream.map(move |res| {
        let json_str = match res {
            Ok(evt) => serde_json::to_string(&evt).unwrap_or_default(),
            Err(e) => {
                tracing::error!("Streaming agent error from {}: {}", addr.ip(), e);
                serde_json::to_string(&agent::AgentStreamEvent::Error { message: e.to_string() }).unwrap_or_default()
            }
        };
        Ok(Event::default().data(json_str))
    });

    Sse::new(event_stream)
}

async fn reset_handler(State(state): State<AppState>, Json(payload): Json<ResetRequest>) -> Json<serde_json::Value> {
    state.agent.reset_session(&payload.session_id);
    Json(serde_json::json!({ "status": "ok", "message": "Session reset" }))
}

async fn health_handler(State(state): State<AppState>) -> Json<HealthResponse> {
    let db_status = match state.db.run_sql("SELECT 1;".to_string(), Some(1)).await {
        Ok(_) => "connected",
        Err(_) => "error",
    };

    Json(HealthResponse { status: "ok".to_string(), database: db_status.to_string() })
}

#[derive(Serialize)]
pub struct TestQueryResponse {
    pub query: String,
    pub rows: Vec<serde_json::Value>,
    pub row_count: usize,
}

async fn test_query_handler(State(state): State<AppState>) -> Json<TestQueryResponse> {
    let query = "SELECT game_id, game_date, season_id, team_id_home, team_id_away FROM game LIMIT 5;".to_string();
    match state.db.run_sql(query.clone(), Some(5)).await {
        Ok(rows) => {
            let row_count = rows.len();
            Json(TestQueryResponse { query, rows, row_count })
        }
        Err(e) => {
            tracing::error!("Test query failed: {}", e);
            Json(TestQueryResponse { query, rows: vec![serde_json::json!({"error": e.to_string()})], row_count: 0 })
        }
    }
}
async fn insights_handler(State(state): State<AppState>) -> Json<Arc<db::InsightsResponse>> {
    Json(state.insights.clone())
}

#[derive(Deserialize)]
struct ExportQuery {
    session: String,
}

/// Sanitize a session id for use in a Content-Disposition filename: keep at
/// most 8 ASCII alphanumeric/dash/underscore characters; replace everything
/// else with `_`. Prevents both the byte-boundary panic on multi-byte input
/// and header-injection from CR/LF/control bytes.
fn sanitize_filename_part(s: &str, max_chars: usize) -> String {
    let mut out = String::with_capacity(max_chars.min(s.len()));
    for c in s.chars().take(max_chars) {
        if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
            out.push(c);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push_str("session");
    }
    out
}

async fn export_handler(
    State(state): State<AppState>,
    Query(query): Query<ExportQuery>,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    match state.agent.export_session_markdown(&query.session) {
        Some(md) => {
            let filename = sanitize_filename_part(&query.session, 8);
            let disposition = format!("attachment; filename=\"nba-report-{}.md\"", filename);
            let header_val =
                HeaderValue::from_str(&disposition).map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
            axum::response::Response::builder()
                .header("Content-Type", "text/markdown; charset=utf-8")
                .header("Content-Disposition", header_val)
                .body(axum::body::Body::from(md))
                .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)
        }
        None => Err(axum::http::StatusCode::NOT_FOUND),
    }
}

async fn history_handler(State(state): State<AppState>) -> Json<Vec<db::DbHistoryEntry>> {
    Json(state.db.list_history())
}

#[derive(Serialize)]
struct StatsResponse {
    uptime_secs: u64,
    total_queries: usize,
    successful_queries: usize,
    cache_hits: usize,
    db_tables: usize,
}

async fn stats_handler(State(state): State<AppState>) -> Json<StatsResponse> {
    let history = state.db.list_history();
    let successful = history.iter().filter(|e| e.success).count();
    let cache_hits = history.iter().filter(|e| e.cache_hit).count();
    Json(StatsResponse {
        uptime_secs: state.started_at.elapsed().as_secs(),
        total_queries: state.db.get_lifetime_query_count(),
        successful_queries: successful,
        cache_hits,
        db_tables: state.insights.total_tables,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_allows_within_limit() {
        let limiter = RateLimiter::new(5, 60);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        for _ in 0..5 {
            assert!(limiter.check(ip), "Should allow request within limit");
        }
    }

    #[test]
    fn test_rate_limiter_blocks_exceeding_limit() {
        let limiter = RateLimiter::new(3, 60);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        assert!(limiter.check(ip));
        assert!(limiter.check(ip));
        assert!(limiter.check(ip));
        assert!(!limiter.check(ip), "Should block 4th request from same IP");
    }

    #[test]
    fn test_rate_limiter_per_ip_isolation() {
        let limiter = RateLimiter::new(2, 60);
        let a: IpAddr = "10.0.0.1".parse().unwrap();
        let b: IpAddr = "10.0.0.2".parse().unwrap();
        assert!(limiter.check(a));
        assert!(limiter.check(a));
        assert!(!limiter.check(a), "A exhausted");
        // B has its own budget
        assert!(limiter.check(b), "B has its own bucket");
        assert!(limiter.check(b));
    }

    #[test]
    fn test_rate_limiter_window_expiry() {
        let limiter = RateLimiter::new(2, 0);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        assert!(limiter.check(ip));
        assert!(limiter.check(ip), "0s window should expire instantly");
        assert!(limiter.check(ip));
    }

    #[test]
    fn test_rate_limiter_xff_ignored_when_peer_untrusted() {
        // No trusted proxies configured → XFF must be ignored.
        let lim = RateLimiter::new(2, 60);
        let peer: IpAddr = "203.0.113.7".parse().unwrap();
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", HeaderValue::from_static("10.0.0.5"));
        let resolved = lim.client_ip_for(&h, peer);
        assert_eq!(resolved, peer, "XFF must be ignored when peer is not a trusted proxy");
    }

    #[test]
    fn test_rate_limiter_xff_honored_for_trusted_proxy() {
        // Peer inside the trusted CIDR → XFF leftmost entry is the real client.
        let trusted: Vec<IpNet> = vec!["10.0.0.0/8".parse().unwrap()];
        let lim = RateLimiter::with_trusted_proxies(2, 60, trusted);
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", HeaderValue::from_static("203.0.113.99, 10.0.0.1"));
        let resolved = lim.client_ip_for(&h, peer);
        assert_eq!(resolved, "203.0.113.99".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn test_rate_limiter_xff_unparseable_falls_back_to_peer() {
        let trusted: Vec<IpNet> = vec!["10.0.0.0/8".parse().unwrap()];
        let lim = RateLimiter::with_trusted_proxies(2, 60, trusted);
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", HeaderValue::from_static("not-an-ip"));
        let resolved = lim.client_ip_for(&h, peer);
        assert_eq!(resolved, peer);
    }

    #[test]
    fn test_sanitize_filename_part() {
        assert_eq!(sanitize_filename_part("abcdefgh", 8), "abcdefgh");
        assert_eq!(sanitize_filename_part("abcdef", 8), "abcdef");
        // Truncates by char count, not byte index
        assert_eq!(sanitize_filename_part("aaaaaaaé", 8), "aaaaaaa_");
        // Strips control bytes (no header-injection)
        assert_eq!(sanitize_filename_part("aa\r\nbb", 8), "aa__bb");
        // Empty / all-bad → "session"
        assert_eq!(sanitize_filename_part("", 8), "session");
        assert_eq!(sanitize_filename_part("漢字", 8), "__");
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(constant_time_eq(b"", b""));
    }
}
