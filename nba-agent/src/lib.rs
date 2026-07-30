pub mod agent;
pub mod db;

use axum::{
    Json, Router,
    extract::{ConnectInfo, Query, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response, sse::{Event, Sse}},
    routing::{get, post},
};
use futures::StreamExt;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, convert::Infallible, net::SocketAddr, sync::Arc, time::Instant};
use tower_http::services::ServeDir;


#[derive(Clone)]
struct RateLimiter {
    inner: Arc<Mutex<HashMap<SocketAddr, Vec<Instant>>>>,
    max_requests: usize,
    window_secs: u64,
}

impl RateLimiter {
    fn new(max_requests: usize, window_secs: u64) -> Self {
        Self { inner: Arc::new(Mutex::new(HashMap::new())), max_requests, window_secs }
    }

    fn check(&self, addr: SocketAddr) -> bool {
        let mut map = self.inner.lock();
        let now = Instant::now();
        let window = std::time::Duration::from_secs(self.window_secs);
        let entries = map.entry(addr).or_default();
        entries.retain(|t| now.duration_since(*t) < window);
        if entries.len() >= self.max_requests {
            return false;
        }
        entries.push(now);
        true
    }
}

async fn rate_limit_middleware(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(limiter): State<RateLimiter>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    if !limiter.check(addr) {
        return (StatusCode::TOO_MANY_REQUESTS, "Rate limit exceeded. Try again later.").into_response();
    }
    next.run(request).await
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

pub async fn build_state(db: db::DbContext) -> anyhow::Result<AppState> {
    let insights = Arc::new(db.generate_insights().await);
    let insights_brief = db::DbContext::format_insights_for_prompt(&insights);
    let agent = Arc::new(agent::Agent::new(db.clone(), insights_brief).await?);
    Ok(AppState { agent, db, insights, started_at: std::time::Instant::now() })
}

pub fn build_router(state: AppState) -> Router {
    let limiter = RateLimiter::new(60, 60); // 60 requests per 60 seconds
    Router::new()
        .route("/api/chat", post(chat_handler))
        .route("/api/chat/stream", get(chat_stream_handler))
        .route("/api/reset", post(reset_handler))
        .route("/api/health", get(health_handler))
        .route("/api/test-query", get(test_query_handler))
        .route("/api/insights", get(insights_handler))
        .route("/api/export", get(export_handler))
        .route("/api/sessions", get(sessions_handler))
        .route("/api/history", get(history_handler))
        .route("/api/stats", get(stats_handler))
        .route_layer(middleware::from_fn_with_state(limiter, rate_limit_middleware))
        .fallback_service(ServeDir::new("static"))
        .with_state(state)
}

async fn chat_handler(
    State(state): State<AppState>,
    Json(payload): Json<ChatRequest>,
) -> Json<agent::ConversationTrace> {
    match state.agent.run_conversation(payload.session_id, &payload.message).await {
        Ok(trace) => Json(trace),
        Err(e) => {
            tracing::error!("Error processing chat conversation: {}", e);
            Json(agent::ConversationTrace {
                session_id: String::new(),
                steps: vec![],
                final_answer: format!("Error executing agent loop: {}", e),
            })
        }
    }
}

async fn chat_stream_handler(
    State(state): State<AppState>,
    Query(query): Query<StreamQuery>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let stream = state.agent.run_conversation_stream(query.session_id, query.message);

    let event_stream = stream.map(|res| {
        let json_str = match res {
            Ok(evt) => serde_json::to_string(&evt).unwrap_or_default(),
            Err(e) => {
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
            Json(TestQueryResponse {
                query,
                rows: vec![serde_json::json!({"error": e.to_string()})],
                row_count: 0,
            })
        }
    }
}
async fn insights_handler(State(state): State<AppState>) -> Json<db::InsightsResponse> {
    Json((*state.insights).clone())
}

#[derive(Deserialize)]
struct ExportQuery {
    session: String,
}

async fn export_handler(
    State(state): State<AppState>,
    Query(query): Query<ExportQuery>,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    match state.agent.export_session_markdown(&query.session) {
        Some(md) => Ok(axum::response::Response::builder()
            .header("Content-Type", "text/markdown; charset=utf-8")
            .header("Content-Disposition", format!("attachment; filename=\"nba-report-{}.md\"", &query.session[..8.min(query.session.len())]))
            .body(axum::body::Body::from(md))
            .unwrap()),
        None => Err(axum::http::StatusCode::NOT_FOUND),
    }
}

async fn sessions_handler(State(state): State<AppState>) -> Json<Vec<serde_json::Value>> {
    let sessions: Vec<_> = state.agent.list_sessions().into_iter().map(|(id, count)| {
        serde_json::json!({"session_id": id, "message_count": count})
    }).collect();
    Json(sessions)
}

async fn history_handler(State(state): State<AppState>) -> Json<Vec<db::DbHistoryEntry>> {
    Json(state.db.list_history())
}

#[derive(Serialize)]
struct StatsResponse {
    uptime_secs: u64,
    active_sessions: usize,
    total_queries: usize,
    db_tables: usize,
}

async fn stats_handler(State(state): State<AppState>) -> Json<StatsResponse> {
    Json(StatsResponse {
        uptime_secs: state.started_at.elapsed().as_secs(),
        active_sessions: state.agent.list_sessions().len(),
        total_queries: state.db.list_history().len(),
        db_tables: state.insights.total_tables,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_rate_limiter_allows_within_limit() {
        let limiter = RateLimiter::new(5, 60);
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345);
        for _ in 0..5 {
            assert!(limiter.check(addr), "Should allow request within limit");
        }
    }

    #[test]
    fn test_rate_limiter_blocks_exceeding_limit() {
        let limiter = RateLimiter::new(3, 60);
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345);
        assert!(limiter.check(addr));
        assert!(limiter.check(addr));
        assert!(limiter.check(addr));
        assert!(!limiter.check(addr), "Should block 4th request");
    }

    #[test]
    fn test_rate_limiter_per_ip_isolation() {
        let limiter = RateLimiter::new(2, 60);
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1000);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 2000);
        assert!(limiter.check(addr1));
        assert!(limiter.check(addr1));
        assert!(limiter.check(addr2), "Different IP should not be affected");
        assert!(!limiter.check(addr1), "addr1 should be blocked");
        assert!(limiter.check(addr2), "addr2 should still have quota");
    }

    #[test]
    fn test_rate_limiter_window_expiry() {
        let limiter = RateLimiter::new(2, 0); // 0-second window = instant expiry
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345);
        assert!(limiter.check(addr));
        // With 0-second window, old entries are immediately expired
        assert!(limiter.check(addr), "0s window should expire instantly");
        assert!(limiter.check(addr));
    }
}
