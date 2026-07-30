pub mod agent;
pub mod db;

use axum::{
    Json, Router,
    extract::{Query, State},
    response::sse::{Event, Sse},
    routing::{get, post},
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, sync::Arc};
use tower_http::services::ServeDir;

#[derive(Clone)]
pub struct AppState {
    pub agent: Arc<agent::Agent>,
    pub db: db::DbContext,
    pub insights: Arc<db::InsightsResponse>,
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
    Ok(AppState { agent, db, insights })
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/api/chat", post(chat_handler))
        .route("/api/chat/stream", get(chat_stream_handler))
        .route("/api/reset", post(reset_handler))
        .route("/api/health", get(health_handler))
        .route("/api/test-query", get(test_query_handler))
        .route("/api/insights", get(insights_handler))
        .route("/api/export", get(export_handler))
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
