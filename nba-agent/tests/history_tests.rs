// ---------------------------------------------------------------------------
// Query history tests (DbContext-level)
// ---------------------------------------------------------------------------

use nba_agent::db::DbHistoryEntry;

#[test]
fn test_db_history_entry_serialization() {
    let entry = DbHistoryEntry {
        timestamp: 1700000000,
        sql: "SELECT COUNT(*) FROM game;".to_string(),
        row_count: 5,
        elapsed_ms: 234,
        success: true,
        error_category: None,
        cache_hit: false,
        model: None,
        session_id: None,
        tool_name: None,
    };
    let json = serde_json::to_string(&entry).unwrap();
    assert!(json.contains("SELECT COUNT(*)"), "Should contain SQL");
    assert!(json.contains("234"), "Should contain elapsed_ms");

    let parsed: DbHistoryEntry = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.row_count, 5);
    assert_eq!(parsed.elapsed_ms, 234);
    assert!(parsed.success);
}

#[test]
fn test_db_history_entry_failure() {
    let entry = DbHistoryEntry {
        timestamp: 1700000001,
        sql: "SELECT * FROM nonexistent;".to_string(),
        row_count: 0,
        elapsed_ms: 12,
        success: false,
        error_category: Some("execution_error".to_string()),
        cache_hit: false,
        model: None,
        session_id: None,
        tool_name: None,
    };
    let json = serde_json::to_string(&entry).unwrap();
    assert!(json.contains("\"success\":false"), "Should serialize failure status");
}
