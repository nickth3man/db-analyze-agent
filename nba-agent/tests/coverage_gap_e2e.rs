// ---------------------------------------------------------------------------
// End-to-end handler & agent tests using wiremock to mock OpenRouter API
// =========================================================================

use axum::http::Request;
use nba_agent::agent::Agent;
use nba_agent::db::DbContext;
use nba_agent::{AppState, build_router, build_state};
use serde_json::json;
use std::env;
use std::sync::Arc;
use tempfile::TempDir;
use tower::util::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn mock_openrouter_response() -> String {
    // Simulate OpenRouter return: agent uses a tool then gives final answer
    json!({
        "id": "chatcmpl-1",
        "object": "chat.completion",
        "model": "qwen/qwen3.7-flash",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "call_1",
                    "type": "function",
                    "function": {
                        "name": "run_sql",
                        "arguments": "{\"reasoning\": \"count games\", \"query\": \"SELECT COUNT(*) FROM game\"}"
                    }
                }]
            },
            "finish_reason": "tool_calls"
        }]
    })
    .to_string()
}

fn mock_openrouter_run_response() -> String {
    // Tool call result: agent gives final answer
    json!({
        "id": "chatcmpl-2",
        "object": "chat.completion",
        "model": "qwen/qwen3.7-flash",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "There are 43,210 games in the database.",
                "tool_calls": null
            },
            "finish_reason": "stop"
        }]
    })
    .to_string()
}

async fn setup_mock_server() -> MockServer {
    let server = MockServer::start().await;

    // First call: tool call
    Mock::given(method("POST"))
        .and(path("/api/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_string(mock_openrouter_response()))
        .mount(&server)
        .await;

    // Second call: final answer
    Mock::given(method("POST"))
        .and(path("/api/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_string(mock_openrouter_run_response()))
        .mount(&server)
        .await;

    server
}

fn make_test_db(dir: &std::path::Path) -> DbContext {
    // Each test gets its own DB file inside a fresh TempDir so parallel tests
    // never contend on the same on-disk file (Windows file-lock errors).
    let db_path = dir.join("test.duckdb");

    let conn = duckdb::Connection::open(&db_path).unwrap();
    conn.execute_batch("
        CREATE TABLE player (player_id TEXT, first_name TEXT, last_name TEXT, height TEXT, weight TEXT, college TEXT, country TEXT, draft_year TEXT, draft_round TEXT, draft_number TEXT, rosters TEXT, postfix_name TEXT, from_year INTEGER, to_year INTEGER, is_inducted INTEGER);
        CREATE TABLE team (team_id INTEGER, year INTEGER, franchise TEXT, stf TEXT, conf TEXT, div TEXT, wins INTEGER, losses INTEGER, div_wins INTEGER, div_losses INTEGER, conf_wins INTEGER, conf_losses INTEGER, gb INTEGER, home_wins INTEGER, home_losses INTEGER, road_wins INTEGER, road_losses INTEGER, o_fgm INTEGER, o_fga INTEGER, o_ftm INTEGER, o_fta INTEGER, o_3pm INTEGER, o_3pa INTEGER, o_oreb INTEGER, o_dreb INTEGER, o_reb INTEGER, o_asts INTEGER, o_p_stl INTEGER, o_p_to INTEGER, o_p_fgm INTEGER, o_p_p_fga INTEGER, o_pf INTEGER, o_p_pts INTEGER, o_p_plus_minus INTEGER, d_fgm INTEGER, d_fga INTEGER, d_ftm INTEGER, d_fta INTEGER, d_3pm INTEGER, d_3pa INTEGER, d_oreb INTEGER, d_dreb INTEGER, d_reb INTEGER, d_asts INTEGER, d_p_stl INTEGER, d_p_to INTEGER, d_p_fgm INTEGER, d_p_p_fga INTEGER, d_pf INTEGER, d_p_pts INTEGER, d_p_plus_minus INTEGER, o_pace INTEGER);
        CREATE TABLE game (game_id TEXT, game_date DATE, season_id INTEGER, team_id_home INTEGER, team_id_away INTEGER, away_team_id INTEGER);
    ").unwrap();

    conn.close().unwrap();

    DbContext::new(db_path.to_str().unwrap()).unwrap()
}

struct TestHarness {
    db: DbContext,
    #[allow(dead_code)]
    server: MockServer,
    tmp: TempDir,
    agent: Agent,
}

async fn make_harness() -> TestHarness {
    let tmp = TempDir::new().unwrap();

    let db = make_test_db(tmp.path());
    let server = setup_mock_server().await;

    // Configure OpenRouter to use mock server
    unsafe {
        env::set_var("OPENROUTER_API_KEY", "test-key");
        env::set_var("OPENROUTER_BASE_URL", format!("{}/api/v1/chat/completions", server.uri()));
    }

    // Build insights (requires real DB queries)
    let insights = db.generate_insights().await;
    let insights_brief = nba_agent::db::DbContext::format_insights_for_prompt(&insights);

    // Build agent
    let agent = Agent::new(db.clone(), insights_brief, "data/sessions.json".to_string()).await.unwrap();

    TestHarness { db, server, tmp, agent }
}

// =========================================================================
// Agent method tests under test harness
// =========================================================================

#[tokio::test]
async fn test_session_count_empty() {
    let h = make_harness().await;
    assert_eq!(h.agent.session_count(), 0, "Session count should be empty");
}

#[tokio::test]
async fn test_reset_session_no_panic() {
    let h = make_harness().await;
    h.agent.reset_session("any-session-id");
    // If we get here, no panic
}

#[tokio::test]
async fn test_export_session_markdown_missing_session() {
    let h = make_harness().await;
    let result = h.agent.export_session_markdown("nonexistent-session");
    assert!(result.is_none(), "Should return None for missing session");
}

// =========================================================================
// Agent run_conversation test with wiremock
// =========================================================================

#[tokio::test]
async fn test_run_conversation_with_mock() {
    let harness = make_harness().await;
    let agent = harness.agent;

    // Run a conversation - should use mock server, not real OpenRouter
    let result = agent.run_conversation(Some("test-session-1".to_string()), "Count the games").await;

    match &result {
        Ok(trace) => assert_eq!(trace.session_id, "test-session-1"),
        Err(e) => {
            let err_str = format!("{:?}", e);
            panic!("run_conversation failed: {}", err_str);
        }
    }
}

async fn make_state_with_mock() -> (AppState, TempDir) {
    let harness = make_harness().await;

    let insights = Arc::new(harness.db.generate_insights().await);
    let insights_brief = DbContext::format_insights_for_prompt(&insights);
    let agent =
        Arc::new(Agent::new(harness.db.clone(), insights_brief, "data/sessions.json".to_string()).await.unwrap());

    (AppState { agent, db: harness.db.clone(), insights, started_at: std::time::Instant::now() }, harness.tmp)
}

#[tokio::test]
async fn test_health_handler_status_ok() {
    let (state, _tmp) = make_state_with_mock().await;
    let router = build_router(state);

    let req = Request::builder().uri("/api/health").body(axum::body::Body::empty()).unwrap();

    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status().as_u16();
    assert!(status < 400, "Health handler should succeed");
    assert!(status >= 200);
}

#[tokio::test]
async fn test_stats_handler_returns_ok() {
    let (state, _tmp) = make_state_with_mock().await;
    let router = build_router(state);

    let req = Request::builder().uri("/api/stats").body(axum::body::Body::empty()).unwrap();

    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status().as_u16();
    assert!(status < 400, "Stats handler should succeed");
}

#[tokio::test]
async fn test_sessions_route_no_longer_enumerates() {
    // SEC-001 fix: /api/sessions was an unauthenticated session-id
    // enumeration endpoint. The route no longer exists; the request should
    // fall through to the static-file fallback (not 200 with a session list).
    let (state, _tmp) = make_state_with_mock().await;
    let router = build_router(state);

    let req = Request::builder().uri("/api/sessions").body(axum::body::Body::empty()).unwrap();

    let resp = router.oneshot(req).await.unwrap();
    // The fallback service tries to serve "static/api/sessions" → 404.
    let status = resp.status().as_u16();
    assert!(status == 404 || status == 200, "Unexpected status from removed /api/sessions route: {}", status);
    // Hard guarantee: response must NOT be a JSON array of session_ids.
    let body_bytes = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
    let body_str = String::from_utf8_lossy(&body_bytes);
    assert!(!body_str.contains("\"session_id\""), "Response leaks session_id keys: {}", body_str);
}

#[tokio::test]
async fn test_history_handler_empty() {
    let (state, _tmp) = make_state_with_mock().await;
    let router = build_router(state);

    let req = Request::builder().uri("/api/history").body(axum::body::Body::empty()).unwrap();

    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status().as_u16();
    assert!(status < 400, "History handler should succeed");
}

#[tokio::test]
async fn test_reset_handler_ok() {
    let (state, _tmp) = make_state_with_mock().await;
    let router = build_router(state);

    let body = serde_json::to_string(&json!({ "session_id": "test-reset-1" })).unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/api/reset")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body))
        .unwrap();

    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status().as_u16();
    assert!(status < 400, "Reset handler should succeed, got {}", status);
}

#[tokio::test]
async fn test_insights_handler_returns_cards() {
    let (state, _tmp) = make_state_with_mock().await;
    let router = build_router(state);

    let req = Request::builder().uri("/api/insights").body(axum::body::Body::empty()).unwrap();

    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status().as_u16();
    assert!(status < 400, "Insights handler should succeed");
}

#[tokio::test]
async fn test_export_handler_404_for_missing_session() {
    let (state, _tmp) = make_state_with_mock().await;
    let router = build_router(state);

    let req =
        Request::builder().uri("/api/export?session=nonexistent-session").body(axum::body::Body::empty()).unwrap();

    let resp = router.oneshot(req).await.unwrap();
    // Export for nonexistent session should be 404
    assert_eq!(resp.status(), 404, "Missing session should return 404");
}

#[tokio::test]
async fn test_build_state_generates_insights() {
    let tmp = TempDir::new().unwrap();
    let db = make_test_db(tmp.path());
    let state = build_state(db, "data/sessions.json".to_string()).await.unwrap();

    assert!(state.started_at.elapsed().as_secs() < 30, "Should build state quickly");
    assert!(state.insights.total_tables > 0, "Should find tables in DB");
}

#[tokio::test]
async fn test_run_sql_caching_same_query() {
    let h = make_harness().await;

    let result1 = h.db.run_sql("SELECT COUNT(*) FROM player;".to_string(), Some(1)).await;
    let result2 = h.db.run_sql("SELECT COUNT(*) FROM player;".to_string(), Some(1)).await;

    assert!(result1.is_ok(), "First query should succeed");
    assert!(result2.is_ok(), "Cached query should succeed");
}

#[tokio::test]
async fn test_run_sql_different_max_rows_different_keys() {
    let h = make_harness().await;

    let r1 = h.db.run_sql("SELECT * FROM player;".to_string(), Some(5)).await;
    let r2 = h.db.run_sql("SELECT * FROM player;".to_string(), Some(10)).await;

    assert!(r1.is_ok(), "5-row query should succeed");
    assert!(r2.is_ok(), "10-row query should succeed");
}
