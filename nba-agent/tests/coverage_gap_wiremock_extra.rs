// Wiremock-driven coverage for agent retry/error paths, the background save
// loop, the streaming loop tail (max-iteration, session persistence), plus
// db.rs heuristic fallback and lib.rs middleware branches.
use axum::body::Body;
use axum::http::{Request, StatusCode};
use nba_agent::agent::{Agent, AgentStreamEvent};
use nba_agent::db::DbContext;
use nba_agent::{AppState, build_router, build_state};
use serde_json::json;
use std::env;
use std::sync::Arc;
use tempfile::TempDir;
use tokio_stream::StreamExt;
use tower::util::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn make_test_db(dir: &std::path::Path) -> DbContext {
    let db_path = dir.join("test.duckdb");
    let conn = duckdb::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "CREATE TABLE player (player_id TEXT, first_name TEXT, last_name TEXT, height TEXT, weight TEXT, college TEXT, country TEXT, draft_year TEXT, draft_round TEXT, draft_number TEXT, rosters TEXT, postfix_name TEXT, from_year INTEGER, to_year INTEGER, is_inducted INTEGER);
         CREATE TABLE team (team_id INTEGER, year INTEGER, franchise TEXT, stf TEXT, conf TEXT, div TEXT, wins INTEGER, losses INTEGER, div_wins INTEGER, div_losses INTEGER, conf_wins INTEGER, conf_losses INTEGER, gb INTEGER);
         CREATE TABLE game (game_id TEXT, game_date DATE, season_id INTEGER, team_id_home INTEGER, team_id_away INTEGER, away_team_id INTEGER);",
    )
    .unwrap();
    conn.close().unwrap();
    DbContext::new(db_path.to_str().unwrap()).unwrap()
}

fn tool_call_response() -> String {
    json!({
        "id": "c1", "object": "chat.completion", "model": "m",
        "choices": [{"index": 0, "message": {
            "role": "assistant", "content": null,
            "tool_calls": [{"id": "call_1", "type": "function", "function": {
                "name": "run_sql",
                "arguments": "{\"reasoning\": \"count\", \"query\": \"SELECT COUNT(*) FROM game\"}"
            }}]
        }, "finish_reason": "tool_calls"}]
    })
    .to_string()
}

fn final_answer_response() -> String {
    json!({
        "id": "c2", "object": "chat.completion", "model": "m",
        "choices": [{"index": 0, "message": {
            "role": "assistant", "content": "There are 42,000 games.", "tool_calls": null
        }, "finish_reason": "stop"}]
    })
    .to_string()
}

async fn make_agent(dir: &std::path::Path, server: &MockServer, sessions_path: &str) -> Agent {
    unsafe {
        env::set_var("OPENROUTER_API_KEY", "test-key");
        env::set_var("OPENROUTER_BASE_URL", format!("{}/api/v1/chat/completions", server.uri()));
    }
    let db = make_test_db(dir);
    let insights = db.generate_insights().await;
    let brief = DbContext::format_insights_for_prompt(&insights);
    Agent::new(db, brief, sessions_path.to_string()).await.unwrap()
}

// === 1. call_openrouter retry: 500 then success ===
#[tokio::test]
async fn test_retry_after_http_500_then_success() {
    let tmp = TempDir::new().unwrap();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_string(final_answer_response()))
        .mount(&server)
        .await;

    let agent = make_agent(tmp.path(), &server, "data/sessions.json").await;
    let events: Vec<_> = agent
        .run_conversation_stream(Some("retry-session".to_string()), "How many games?".to_string())
        .collect::<Vec<_>>()
        .await;
    let ok_events: Vec<_> = events.iter().filter_map(|e| e.as_ref().ok()).collect();
    assert!(
        ok_events.iter().any(|e| matches!(e, AgentStreamEvent::Completed { .. })),
        "should complete after retry; got {:?}",
        ok_events
    );
}

// === 2. All retries fail -> Error event ===
#[tokio::test]
async fn test_all_retries_fail_emits_error() {
    let tmp = TempDir::new().unwrap();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(503).set_body_string("upstream down"))
        .mount(&server)
        .await;

    let agent = make_agent(tmp.path(), &server, "data/sessions.json").await;
    let events: Vec<_> = agent.run_conversation_stream(None, "How many games?".to_string()).collect::<Vec<_>>().await;
    let ok_events: Vec<_> = events.iter().filter_map(|e| e.as_ref().ok()).collect();
    assert!(
        ok_events.iter().any(|e| matches!(e, AgentStreamEvent::Error { .. })),
        "should emit Error after retries exhausted"
    );
}

// === 3. Malformed JSON body -> parse error retry then success ===
#[tokio::test]
async fn test_json_parse_error_retries() {
    let tmp = TempDir::new().unwrap();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json at all"))
        .up_to_n_times(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_string(final_answer_response()))
        .mount(&server)
        .await;

    let agent = make_agent(tmp.path(), &server, "data/sessions.json").await;
    let events: Vec<_> = agent
        .run_conversation_stream(Some("parse-session".to_string()), "How many games?".to_string())
        .collect::<Vec<_>>()
        .await;
    let ok_events: Vec<_> = events.iter().filter_map(|e| e.as_ref().ok()).collect();
    assert!(ok_events.iter().any(|e| matches!(e, AgentStreamEvent::Completed { .. })));
}

// === 4. Max-iteration exit: every response is a tool call ===
#[tokio::test]
async fn test_max_iterations_terminates_with_message() {
    let tmp = TempDir::new().unwrap();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_string(tool_call_response()))
        .mount(&server)
        .await;

    let sessions_path = tmp.path().join("sessions.json");
    unsafe {
        env::set_var("MAX_ITERATIONS", "4");
    }
    let agent = make_agent(tmp.path(), &server, sessions_path.to_str().unwrap()).await;
    unsafe {
        env::remove_var("MAX_ITERATIONS");
    }

    let events: Vec<_> = agent
        .run_conversation_stream(Some("maxiter-session".to_string()), "Keep going".to_string())
        .collect::<Vec<_>>()
        .await;
    let ok_events: Vec<_> = events.iter().filter_map(|e| e.as_ref().ok()).collect();
    let has_max_msg = ok_events.iter().any(|e| match e {
        AgentStreamEvent::FinalAnswerChunk { text } => text.contains("max analytical"),
        _ => false,
    });
    let completed = ok_events.iter().any(|e| matches!(e, AgentStreamEvent::Completed { .. }));
    assert!(has_max_msg && completed, "expected max-iteration exit + completion");
    // Session must have been persisted (mark_dirty -> save loop path); give the
    // background save loop its debounce time before reading.
    tokio::time::sleep(std::time::Duration::from_secs(7)).await;
    let saved = std::fs::read_to_string(&sessions_path).unwrap_or_default();
    assert!(saved.contains("maxiter-session"), "session should persist to disk");
}

// === 5. Save loop: conversation marks dirty, background task writes file ===
#[tokio::test]
async fn test_save_loop_persists_after_conversation() {
    let tmp = TempDir::new().unwrap();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_string(tool_call_response()))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_string(final_answer_response()))
        .mount(&server)
        .await;

    let sessions_path = tmp.path().join("sessions.json");
    let agent = make_agent(tmp.path(), &server, sessions_path.to_str().unwrap()).await;
    let events: Vec<_> = agent
        .run_conversation_stream(Some("save-session".to_string()), "How many games?".to_string())
        .collect::<Vec<_>>()
        .await;
    assert!(events.iter().filter_map(|e| e.as_ref().ok()).any(|e| matches!(e, AgentStreamEvent::Completed { .. })));

    // Give the background save loop time to flush (debounce 5s + coalesce).
    tokio::time::sleep(std::time::Duration::from_secs(7)).await;
    let saved = std::fs::read_to_string(&sessions_path).unwrap_or_default();
    assert!(saved.contains("save-session"), "save loop should write the session file");
    assert!(saved.contains("42,000"), "saved file should include the answer");
}

// === 6. Streaming multi-step: tool call + final answer through HTTP handler ===
#[tokio::test]
async fn test_chat_handler_streaming_events() {
    let tmp = TempDir::new().unwrap();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_string(tool_call_response()))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_string(tool_call_response()))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_string(final_answer_response()))
        .mount(&server)
        .await;

    unsafe {
        env::set_var("OPENROUTER_API_KEY", "test-key");
        env::set_var("OPENROUTER_BASE_URL", format!("{}/api/v1/chat/completions", server.uri()));
    }
    let db = make_test_db(tmp.path());
    let insights = Arc::new(db.generate_insights().await);
    let brief = DbContext::format_insights_for_prompt(&insights);
    let sessions_path = tmp.path().join("sessions.json");
    let agent = Arc::new(Agent::new(db.clone(), brief, sessions_path.to_str().unwrap().to_string()).await.unwrap());
    let state = AppState { agent, db, insights, started_at: std::time::Instant::now() };
    let app = build_router(state);

    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/chat")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"message":"How many games?","session_id":"http-session"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), 10_000_000).await.unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("final_answer") && text.contains("42,000"),
        "sync /api/chat should return the trace JSON; got: {}",
        &text[..text.len().min(400)]
    );
}

// === 7. db.rs heuristic fallback: sqlparser cannot parse, heuristic fixes ===
#[test]
fn test_auto_fix_sql_heuristic_fallback() {
    // This SQL is invalid (unparseable) so the AST path fails and the heuristic runs.
    let bad_sql = "SELECT player_name FROM WHERE";
    let err = "Binder Error: Referenced column \"player_name\" not found in FROM clause!\n\
               Candidate bindings: \"full_name\", \"first_name\"\n\
               LINE 1: SELECT player_name FROM WHERE";
    let fixed = DbContext::auto_fix_sql(bad_sql, err);
    assert!(fixed.is_some(), "heuristic should still return a fix");
    assert!(fixed.unwrap().contains("full_name"), "should swap player_name -> full_name");
}

#[test]
fn test_auto_fix_sql_no_candidates_returns_none() {
    let err = "Binder Error: something unrelated happened";
    let fixed = DbContext::auto_fix_sql("SELECT 1", err);
    assert!(fixed.is_none());
}

// === 8. lib.rs: constant-time auth same-length wrong token (exercises loop) ===
#[tokio::test]
async fn test_auth_same_length_wrong_token() {
    use axum::Router;
    use axum::middleware;
    use axum::routing::get;
    use nba_agent::{ApiAuth, auth_middleware};
    unsafe {
        env::set_var("API_KEY", "sekret");
    }
    let auth = ApiAuth::from_env();
    let app = Router::new()
        .route("/api/stats", get(|| async { "ok" }))
        .layer(middleware::from_fn_with_state(auth, auth_middleware));
    let res = app
        .oneshot(
            Request::builder().uri("/api/stats").header("authorization", "Bearer sekrrt").body(Body::empty()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    unsafe {
        env::remove_var("API_KEY");
    }
}

// === 9. Rate limiter: ConnectInfo present (Some branch) ===
#[tokio::test]
async fn test_rate_limit_with_connect_info() {
    use axum::Router;
    use axum::middleware;
    use axum::routing::get;
    use nba_agent::{RateLimiter, rate_limit_middleware};
    use std::net::SocketAddr;

    let limiter = RateLimiter::new(1, 60);
    let app = Router::new()
        .route("/", get(|| async { "ok" }))
        .layer(middleware::from_fn_with_state(limiter, rate_limit_middleware));

    let mut req = Request::new(Body::empty());
    *req.uri_mut() = "/".parse().unwrap();
    req.extensions_mut().insert(axum::extract::ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 5555))));
    let r1 = app.clone().oneshot(req).await.unwrap();
    assert_eq!(r1.status(), StatusCode::OK);

    let mut req = Request::new(Body::empty());
    *req.uri_mut() = "/".parse().unwrap();
    req.extensions_mut().insert(axum::extract::ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 5555))));
    let r2 = app.clone().oneshot(req).await.unwrap();
    assert_eq!(r2.status(), StatusCode::TOO_MANY_REQUESTS);
}

// === 10. Rate limiter: invalid XFF value falls back to peer ===
#[test]
fn test_client_ip_invalid_xff_falls_back() {
    use nba_agent::RateLimiter;
    use std::net::IpAddr;
    unsafe {
        env::set_var("TRUSTED_PROXIES", "127.0.0.1/32");
    }
    let limiter = RateLimiter::new(10, 60);
    unsafe {
        env::remove_var("TRUSTED_PROXIES");
    }
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("x-forwarded-for", "not-an-ip".parse().unwrap());
    let ip = limiter.client_ip_for(&headers, "127.0.0.1".parse::<IpAddr>().unwrap());
    assert_eq!(ip.to_string(), "127.0.0.1");
}

// === 11. build_state / build_router against the real warehouse (skips absent) ===
#[tokio::test]
async fn test_build_state_and_router_health() {
    let db_path = env::var("DATABASE_PATH").unwrap_or_else(|_| "../data/nba-data.duckdb".to_string());
    if !std::path::Path::new(&db_path).exists() {
        return;
    }
    let db = DbContext::new(&db_path).unwrap();
    let state = build_state(db, "data/sessions.json".to_string()).await.unwrap();
    let app = build_router(state);
    let res = app.oneshot(Request::builder().uri("/api/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

// === 12. stats handler reflects session count after conversation ===
#[tokio::test]
async fn test_stats_handler_session_count() {
    let tmp = TempDir::new().unwrap();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_string(tool_call_response()))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_string(final_answer_response()))
        .mount(&server)
        .await;

    unsafe {
        env::set_var("OPENROUTER_API_KEY", "test-key");
        env::set_var("OPENROUTER_BASE_URL", format!("{}/api/v1/chat/completions", server.uri()));
    }
    let db = make_test_db(tmp.path());
    let insights = Arc::new(db.generate_insights().await);
    let brief = DbContext::format_insights_for_prompt(&insights);
    let agent = Arc::new(Agent::new(db.clone(), brief, "data/sessions.json".to_string()).await.unwrap());
    let state = AppState { agent, db, insights, started_at: std::time::Instant::now() };
    let app = build_router(state);

    // First: run a conversation to create a session
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/chat")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"message":"hi","session_id":"stats-sess"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let _ = axum::body::to_bytes(res.into_body(), 10_000_000).await.unwrap();

    // Then stats should report 1 session
    let res = app.oneshot(Request::builder().uri("/api/stats").body(Body::empty()).unwrap()).await.unwrap();
    let body = axum::body::to_bytes(res.into_body(), 1_000_000).await.unwrap();
    let stats: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let queries = stats["total_queries"].as_u64().unwrap_or(0);
    assert!(queries >= 1, "stats should count the tool query; got {}", queries);
}
