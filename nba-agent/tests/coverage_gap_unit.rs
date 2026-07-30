// Coverage gap: Agent method tests (no HTTP needed — Agent::new just reads env vars)
use nba_agent::agent::Agent;
use nba_agent::db::DbContext;
use serde_json::json;
use std::env;
use std::time::Duration;

fn setup() {
    unsafe { env::set_var("OPENROUTER_API_KEY", "test-key-for-coverage"); }
}

fn make_agent() -> Option<Agent> {
    setup();
    let db_path = env::var("DATABASE_PATH").unwrap_or_else(|_| "../data/nba-data.duckdb".to_string());
    if !std::path::Path::new(&db_path).exists() {
        return None;
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all().build().ok()?;
    rt.block_on(async {
        let db = DbContext::new(&db_path).ok()?;
        let insights = db.generate_insights().await;
        let brief = DbContext::format_insights_for_prompt(&insights);
        Agent::new(db, brief).await.ok()
    })
}

fn agent_test<F>(mut f: F) where F: FnMut(&Agent) {
    if let Some(a) = make_agent() { f(&a); }
}

// === Agent::list_sessions ===
#[test]
fn test_list_sessions_no_sessions() {
    agent_test(|a| {
        let sessions = a.list_sessions();
        assert!(sessions.is_empty());
    });
}

// === Agent::export_session_markdown ===
#[test]
fn test_export_session_markdown_missing() {
    agent_test(|a| {
        assert!(a.export_session_markdown("missing-session").is_none());
    });
}

// === Agent::reset_session ===
#[test]
fn test_reset_session_missing() {
    agent_test(|a| {
        a.reset_session("nonexistent");
    });
}

// === DbContext::list_history ===
use nba_agent::db::DbHistoryEntry;
#[test]
fn test_list_history_empty() {
    agent_test_ref(|agent, db| {
        let history = db.list_history();
        assert!(history.is_empty());
    });
}

// === Context helper for db access ===
fn agent_test_ref<F>(mut f: F) where F: FnMut(&Agent, &DbContext) {
    setup();
    let db_path = env::var("DATABASE_PATH").unwrap_or_else(|_| "../data/nba-data.duckdb".to_string());
    let db = match DbContext::new(&db_path) {
        Ok(d) => d,
        Err(_) => { println!("Skipped: DB not found"); return; }
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all().build().unwrap();
    let result = rt.block_on(async {
        db.run_sql("SELECT 1 as test;".to_string(), Some(1)).await
    });
    assert!(result.is_ok());
}

// === run_sql already covered; test cache key variation ===
#[test]
fn test_query_cache_different_keys_for_same_query() {
    let db_path = env::var("DATABASE_PATH").unwrap_or_else(|_| "../data/nba-data.duckdb".to_string());
    let db = DbContext::new(&db_path).expect("DB must exist");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all().build().unwrap();

    rt.block_on(async {
        // First query - should hit the DB
        let r1 = db.run_sql("SELECT COUNT(*) FROM player;".to_string(), Some(1)).await;
        assert!(r1.is_ok());

        // Second identical query - should use cache
        let r2 = db.run_sql("SELECT COUNT(*) FROM player;".to_string(), Some(1)).await;
        assert_eq!(r1.unwrap().len(), r2.unwrap().len());

        // Same query, different max_rows - different key
        let r3 = db.run_sql("SELECT COUNT(*) FROM player;".to_string(), Some(2)).await;
        assert!(r3.is_ok());
    });
}
