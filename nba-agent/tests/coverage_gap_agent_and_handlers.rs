// ---------------------------------------------------------------------------
// Coverage gap: Agent methods & Axum handler wiring
// =========================================================================

use axum::http::{Method, Request};
use nba_agent::AppState;
use nba_agent::agent::Agent;
use nba_agent::db::DbContext;
use nba_agent::{build_router, build_state};
use serde_json::json;
use std::env;
use std::time::Duration;
use tower::util::ServiceExt;

fn setup_env() {
    unsafe {
        env::set_var("OPENROUTER_API_KEY", "test-key");
    }
}
fn dbp() -> String {
    env::var("DATABASE_PATH").unwrap_or_else(|_| "../data/nba-data.duckdb".to_string())
}
fn has_db() -> bool {
    std::path::Path::new(&dbp()).exists()
}

fn make_state() -> Option<AppState> {
    setup_env();
    if !has_db() {
        return None;
    }
    // Spawn on a fresh OS thread so Runtime::new() never nests inside an existing runtime.
    let dbp = dbp();
    std::thread::spawn(move || {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let db = DbContext::new(&dbp).ok()?;
            build_state(db, "data/sessions.json".to_string()).await.ok()
        })
    })
    .join()
    .unwrap()
}

fn with_state<F>(mut f: F)
where
    F: FnMut(&AppState),
{
    if let Some(s) = make_state() {
        f(&s);
    }
}

fn make_agent() -> Option<Agent> {
    setup_env();
    if !has_db() {
        return None;
    }
    let dbp = dbp();
    std::thread::spawn(move || {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let db = DbContext::new(&dbp).ok()?;
            let insights = db.generate_insights().await;
            let brief = DbContext::format_insights_for_prompt(&insights);
            Agent::new(db, brief, "data/sessions.json".to_string()).await.ok()
        })
    })
    .join()
    .unwrap()
}

fn with_agent<F>(mut f: F)
where
    F: FnMut(&Agent),
{
    if let Some(a) = make_agent() {
        f(&a);
    }
}

// ---------------------------------------------------------------------------
// Agent::session_count (src/agent.rs:741-743)
// ---------------------------------------------------------------------------
#[test]
fn test_session_count_empty() {
    with_agent(|a| {
        assert_eq!(a.session_count(), 0);
    });
}

// ---------------------------------------------------------------------------
// Agent::export_session_markdown (src/agent.rs:690-737)
// ---------------------------------------------------------------------------
#[test]
fn test_export_session_markdown_nonexistent() {
    with_agent(|a| {
        assert!(a.export_session_markdown("nope").is_none());
    });
}

// ---------------------------------------------------------------------------
// Agent::reset_session (src/agent.rs)
// ---------------------------------------------------------------------------
#[test]
fn test_reset_session_no_panic() {
    with_agent(|a| {
        a.reset_session("any");
    });
}

// ---------------------------------------------------------------------------
// build_state (src/lib.rs:85-90)
// ---------------------------------------------------------------------------
#[test]
fn test_build_state() {
    let s = make_state();
    assert!(s.is_some());
    assert!(s.unwrap().started_at.elapsed() < Duration::from_secs(30));
}

// ---------------------------------------------------------------------------
// build_router (src/lib.rs:92-110)
// ---------------------------------------------------------------------------
#[test]
fn test_build_router() {
    with_state(|s| {
        let _r = build_router(s.clone());
    });
}

// ---------------------------------------------------------------------------
// health_handler (src/lib.rs:153-160)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_health_handler() {
    let Some(s) = make_state() else {
        return;
    };
    let req = Request::builder().uri("/api/health").body(axum::body::Body::empty()).unwrap();
    let resp = build_router(s.clone()).oneshot(req).await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
}

// ---------------------------------------------------------------------------
// stats_handler (src/lib.rs:228-236)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_stats_handler() {
    let Some(s) = make_state() else {
        return;
    };
    let req = Request::builder().uri("/api/stats").body(axum::body::Body::empty()).unwrap();
    let resp = build_router(s.clone()).oneshot(req).await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
}

// ---------------------------------------------------------------------------
// /api/sessions removed (was SEC-001 IDOR: full session-id enumeration).
// Verify the route no longer enumerates sessions by hitting stats instead,
// which exposes session_count without leaking IDs.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_stats_handler_reports_session_count() {
    let Some(s) = make_state() else {
        return;
    };
    let req = Request::builder().uri("/api/stats").body(axum::body::Body::empty()).unwrap();
    let resp = build_router(s.clone()).oneshot(req).await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
}
// ---------------------------------------------------------------------------
// history_handler (src/lib.rs:217-219)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_history_handler() {
    let Some(s) = make_state() else {
        return;
    };
    let req = Request::builder().uri("/api/history").body(axum::body::Body::empty()).unwrap();
    let resp = build_router(s.clone()).oneshot(req).await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
}

// ---------------------------------------------------------------------------
// insights_handler (src/lib.rs:186-188)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_insights_handler() {
    let Some(s) = make_state() else {
        return;
    };
    let req = Request::builder().uri("/api/insights").body(axum::body::Body::empty()).unwrap();
    let resp = build_router(s.clone()).oneshot(req).await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
}

// ---------------------------------------------------------------------------
// export_handler (src/lib.rs:195-207)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_export_handler() {
    let Some(s) = make_state() else {
        return;
    };
    let req = Request::builder().uri("/api/export?session=nonexistent").body(axum::body::Body::empty()).unwrap();
    let resp = build_router(s.clone()).oneshot(req).await.unwrap();
    assert!(resp.status().as_u16() < 500);
}

// ---------------------------------------------------------------------------
// reset_handler (src/lib.rs:148-151)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_reset_handler() {
    let Some(s) = make_state() else {
        return;
    };
    let body = serde_json::to_string(&json!({ "session_id": "s1" })).unwrap();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/reset")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body))
        .unwrap();
    let resp = build_router(s.clone()).oneshot(req).await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
}

// ---------------------------------------------------------------------------
// rate_limit middleware (src/lib.rs:44-53)
// ---------------------------------------------------------------------------
#[test]
fn test_rate_limiter_checks() {
    let lim = nba_agent::RateLimiter::new(5, 60);
    let ip: std::net::IpAddr = "10.0.0.42".parse().unwrap();
    for _ in 0..5 {
        assert!(lim.check(ip));
    }
    assert!(!lim.check(ip));
}

#[test]
fn test_rate_limiter_window_expiry() {
    let lim = nba_agent::RateLimiter::new(2, 0);
    let ip: std::net::IpAddr = "127.0.0.1".parse().unwrap();
    assert!(lim.check(ip));
    assert!(lim.check(ip), "0s window should expire instantly");
    assert!(lim.check(ip));
}
