use nba_agent::db::DbContext;
use serde_json::Value;

fn get_test_db() -> Option<DbContext> {
    let path = std::env::var("DATABASE_PATH").unwrap_or_else(|_| "../data/nba-data.duckdb".to_string());
    if std::path::Path::new(&path).exists() {
        DbContext::new(&path).ok()
    } else {
        println!("Database not found at {}, skipping live DuckDB test.", path);
        None
    }
}

// ---------------------------------------------------------------------------
// run_sql tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_run_sql_select_one() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let rows = db.run_sql("SELECT 1 as test_val;".to_string(), Some(1)).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["test_val"], 1);
}

#[tokio::test]
async fn test_run_sql_game_table_has_expected_columns() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let query = "SELECT game_id, game_date, season_id, team_id_home, team_id_away FROM game LIMIT 5;";
    let rows = db.run_sql(query.to_string(), Some(5)).await.unwrap();
    assert_eq!(rows.len(), 5, "Expected 5 rows from game table");
    for col in &["game_id", "game_date", "season_id", "team_id_home", "team_id_away"] {
        assert!(rows[0].get(col).is_some(), "Missing column: {}", col);
    }
}

#[tokio::test]
async fn test_run_sql_caps_at_50_by_default() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let rows = db.run_sql("SELECT * FROM game;".to_string(), None).await.unwrap();
    assert!(rows.len() <= 51, "Should cap at ~50 rows; got {}", rows.len());
}

#[tokio::test]
async fn test_run_sql_empty_result() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let rows = db.run_sql("SELECT * FROM game WHERE game_id = 'nonexistent_game_xyz';".to_string(), None).await.unwrap();
    assert!(rows.is_empty(), "Expected empty result");
}

#[tokio::test]
async fn test_run_sql_explicit_limit_3() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let rows = db.run_sql("SELECT * FROM game LIMIT 3;".to_string(), Some(3)).await.unwrap();
    assert_eq!(rows.len(), 3, "LIMIT 3 should return exactly 3 rows");
}

#[tokio::test]
async fn test_run_sql_aggregate_query() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let rows = db.run_sql("SELECT COUNT(*) as cnt FROM game;".to_string(), Some(1)).await.unwrap();
    assert_eq!(rows.len(), 1);
    let count: i64 = rows[0]["cnt"].as_i64().unwrap_or(0);
    assert!(count > 0, "game table should have rows, got count {}", count);
}

// ---------------------------------------------------------------------------
// list_tables tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_list_tables_returns_all() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let all = db.list_tables(None).await.unwrap();
    assert!(!all.is_empty(), "Database should have tables");
    assert!(all.len() > 10, "Expected more than 10 tables, got {}", all.len());
}

#[tokio::test]
async fn test_list_tables_with_pattern() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let player_tables = db.list_tables(Some("player%".to_string())).await.unwrap();
    assert!(!player_tables.is_empty(), "Should find tables matching player%");
    for t in &player_tables {
        assert!(t.to_lowercase().starts_with("player"), "Table '{}' should start with 'player'", t);
    }
}

#[tokio::test]
async fn test_list_tables_no_match() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let result = db.list_tables(Some("zzzz_nonexistent_%".to_string())).await.unwrap();
    assert!(result.is_empty(), "Should return empty for bogus pattern");
}

// ---------------------------------------------------------------------------
// search_tables tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_search_tables_finds_clutch() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let res = db.search_tables("clutch".to_string()).await.unwrap();
    let found = !res.matched_tables.is_empty() || !res.matched_columns.is_empty();
    assert!(found, "Should find tables or columns matching 'clutch'");
}

#[tokio::test]
async fn test_search_tables_finds_player() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let res = db.search_tables("player".to_string()).await.unwrap();
    assert!(!res.matched_tables.is_empty(), "Should find tables matching 'player'");
}

#[tokio::test]
async fn test_search_tables_no_match() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let res = db.search_tables("xyznonexistentzzz".to_string()).await.unwrap();
    assert!(res.matched_tables.is_empty(), "Should find no tables for bogus keyword");
    assert!(res.matched_columns.is_empty(), "Should find no columns for bogus keyword");
}

// ---------------------------------------------------------------------------
// describe_table tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_describe_table_player() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let info = db.describe_table("player".to_string()).await.unwrap();
    assert_eq!(info.table_name, "player");
    assert!(!info.columns.is_empty(), "player table should have columns");
    assert!(info.columns.iter().any(|c| c.name.to_lowercase().contains("id")), "Should have id-like column in player");
}

#[tokio::test]
async fn test_describe_table_game() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let info = db.describe_table("game".to_string()).await.unwrap();
    assert_eq!(info.table_name, "game");
    assert!(!info.columns.is_empty());
}

#[tokio::test]
async fn test_describe_nonexistent_table_errors() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let result = db.describe_table("nonexistent_table_xyz_123".to_string()).await;
    assert!(result.is_err(), "Describing nonexistent table should return Err");
}

// ---------------------------------------------------------------------------
// explain_query tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_explain_query_returns_plan() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let plan = db.explain_query("SELECT COUNT(*) FROM game;".to_string()).await.unwrap();
    assert!(!plan.is_empty(), "EXPLAIN should return a non-empty query plan");
}

#[tokio::test]
async fn test_explain_query_invalid_sql() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let result = db.explain_query("SELECTZZZ BROKEN SQL!!!".to_string()).await;
    assert!(result.is_err(), "EXPLAIN of invalid SQL should error");
}

// ---------------------------------------------------------------------------
// schema_summary tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_schema_summary_mentions_key_tables() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let summary = db.get_schema_summary().await.unwrap();
    assert!(summary.contains("player"), "Schema summary should mention player table");
    assert!(summary.contains("team"), "Schema summary should mention team table");
    assert!(summary.contains("game"), "Schema summary should mention game table");
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_null_values_in_results() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let rows = db.run_sql(
        "SELECT first_name, last_name, rosterstatus FROM common_player_info LIMIT 5;".to_string(),
        Some(5),
    ).await.unwrap();
    assert!(!rows.is_empty());
    let has_first_name = rows[0].get("first_name").map(|v| !v.is_null()).unwrap_or(false);
    assert!(has_first_name, "first_name should be non-null for first row");
}

#[tokio::test]
async fn test_count_query_returns_integer() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let rows = db.run_sql("SELECT COUNT(*) as n FROM game;".to_string(), Some(1)).await.unwrap();
    let val = &rows[0]["n"];
    assert!(val.is_number(), "COUNT should return a number, got {:?}", val);
}
