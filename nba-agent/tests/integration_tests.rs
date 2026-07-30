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

// ---------------------------------------------------------------------------
// Enriched schema tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_build_enriched_schema_with_filter() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let key_tables: &[&str] = &["game", "player", "team"];
    let schema = db.build_enriched_schema(Some(key_tables)).await.unwrap();
    assert!(schema.total_tables >= 3, "Should find at least 3 key tables");
    assert!(schema.tables.iter().any(|t| t.table_name == "game"), "Should have game table");
    assert!(schema.tables.iter().any(|t| t.table_name == "player"), "Should have player table");
    // game table should have rows
    let game = schema.tables.iter().find(|t| t.table_name == "game").unwrap();
    assert!(game.row_count > 0, "game table should have rows");
    assert!(!game.columns.is_empty(), "game table should have columns");
}

#[tokio::test]
async fn test_enriched_schema_has_fk_hints() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let key_tables: &[&str] = &["game", "player", "team", "common_player_info"];
    let schema = db.build_enriched_schema(Some(key_tables)).await.unwrap();
    // game table columns ending in _id should be FK candidates
    let game = schema.tables.iter().find(|t| t.table_name == "game").unwrap();
    let fk_cols: Vec<_> = game.columns.iter().filter(|c| c.is_fk_candidate).collect();
    assert!(!fk_cols.is_empty(), "game table should have FK candidate columns");
    // Should detect team_id → team relationship
    let team_fk = fk_cols.iter().find(|c| c.name.contains("team_id"));
    assert!(team_fk.is_some(), "game table should have team_id FK column");
}

#[tokio::test]
async fn test_enriched_schema_fk_relationships() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let key_tables: &[&str] = &["game", "player", "team"];
    let schema = db.build_enriched_schema(Some(key_tables)).await.unwrap();
    // Should detect game.team_id_home → team (or similar)
    assert!(!schema.fk_relationships.is_empty(), "Should detect some FK relationships");
}

#[tokio::test]
async fn test_format_enriched_schema_output() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let key_tables: &[&str] = &["game", "player"];
    let schema = db.build_enriched_schema(Some(key_tables)).await.unwrap();
    let formatted = nba_agent::db::DbContext::format_enriched_schema(&schema);
    assert!(formatted.contains("Database Overview"), "Should have header");
    assert!(formatted.contains("game"), "Should mention game table");
    assert!(formatted.contains("player"), "Should mention player table");
    assert!(formatted.contains("Key Table Schemas"), "Should have schemas section");
}

#[tokio::test]
async fn test_enriched_schema_date_range() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let key_tables: &[&str] = &["game"];
    let schema = db.build_enriched_schema(Some(key_tables)).await.unwrap();
    let game = schema.tables.iter().find(|t| t.table_name == "game").unwrap();
    // game table should have a date range (game_date column exists)
    assert!(game.date_range.is_some(), "game table should have date range");
}

#[tokio::test]
async fn test_enriched_schema_sample_values() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let key_tables: &[&str] = &["player", "team"];
    let schema = db.build_enriched_schema(Some(key_tables)).await.unwrap();
    // Find a column with sample values (name-like columns should have samples)
    let has_samples = schema.tables.iter().any(|t| {
        t.columns.iter().any(|c| !c.sample_values.is_empty())
    });
    assert!(has_samples, "Should have columns with sample values");
}

// ---------------------------------------------------------------------------
// Auto SQL error recovery tests
// ---------------------------------------------------------------------------

#[test]
fn test_auto_fix_sql_season_to_season_id() {
    let query = "SELECT game_id, game_date, season, home_team_id FROM game LIMIT 5;";
    let error = "Binder Error: Referenced column \"season\" not found in FROM clause!\n\
                 Candidate bindings: \"season_id\", \"season_type\"\n\
                 LINE 1: SELECT game_id, game_date, season, home_team_id FROM game LIMIT 5;";
    let fixed = nba_agent::db::DbContext::auto_fix_sql(query, error).unwrap();
    assert!(fixed.contains("season_id"), "Should replace season with season_id");
    assert!(!fixed.contains("\"season\""), "Should not have bare season");
}

#[test]
fn test_auto_fix_sql_player_name_to_first_name() {
    let query = "SELECT player_name, roster_status FROM common_player_info LIMIT 5;";
    let error = "Binder Error: Referenced column \"player_name\" not found in FROM clause!\n\
                 Candidate bindings: \"playercode\", \"player_slug\", \"first_name\"\n\
                 LINE 1: SELECT player_name, roster_status FROM common_player_info LIMIT 5;";
    let fixed = nba_agent::db::DbContext::auto_fix_sql(query, error).unwrap();
    assert!(fixed.contains("playercode"), "Should replace player_name with playercode (first candidate)");
    assert!(!fixed.contains("player_name"), "Should remove player_name");
}

#[test]
fn test_auto_fix_sql_roster_status() {
    let query = "SELECT first_name, last_name, roster_status FROM common_player_info LIMIT 5;";
    let error = "Binder Error: Referenced column \"roster_status\" not found in FROM clause!\n\
                 Candidate bindings: \"rosterstatus\", \"first_name\", \"to_year\"\n\
                 LINE 1: SELECT first_name, last_name, roster_status FROM common_player_info LIMIT 5;";
    let fixed = nba_agent::db::DbContext::auto_fix_sql(query, error).unwrap();
    assert!(fixed.contains("rosterstatus"), "Should replace roster_status with rosterstatus");
    assert!(!fixed.contains("roster_status"), "Should remove roster_status");
}

#[test]
fn test_auto_fix_sql_home_team_id() {
    let query = "SELECT game_id, home_team_id, away_team_id FROM game LIMIT 5;";
    let error = "Binder Error: Referenced column \"home_team_id\" not found in FROM clause!\n\
                 Candidate bindings: \"team_id_home\", \"team_name_home\"\n\
                 LINE 1: SELECT game_id, home_team_id, away_team_id FROM game LIMIT 5;";
    let fixed = nba_agent::db::DbContext::auto_fix_sql(query, error).unwrap();
    assert!(fixed.contains("team_id_home"), "Should replace home_team_id with team_id_home");
}

#[test]
fn test_auto_fix_sql_no_candidates_returns_none() {
    let query = "SELECT * FROM nonexistent_table;";
    let error = "Binder Error: Table \"nonexistent_table\" does not exist";
    let fixed = nba_agent::db::DbContext::auto_fix_sql(query, error);
    assert!(fixed.is_none(), "Should return None when no candidate bindings");
}

#[test]
fn test_auto_fix_sql_preserves_string_literals() {
    // If a column name appears in a string literal, it should NOT be replaced
    let query = "SELECT 'season' as label, season FROM game LIMIT 1;";
    let error = "Binder Error: Referenced column \"season\" not found in FROM clause!\n\
                 Candidate bindings: \"season_id\", \"season_type\"\n\
                 LINE 1: SELECT 'season' as label, season FROM game LIMIT 1;";
    let fixed = nba_agent::db::DbContext::auto_fix_sql(query, error).unwrap();
    // The string literal 'season' should stay, the bare identifier season should become season_id
    assert!(fixed.contains("'season'"), "String literal should be preserved");
    assert!(fixed.contains("season_id"), "Column reference should be fixed");
}

#[tokio::test]
async fn test_auto_fix_integration_with_real_db() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };

    // Query with a wrong column name that has known candidates
    let bad_query = "SELECT player_name FROM common_player_info LIMIT 1;";
    let result = db.run_sql(bad_query.to_string(), Some(1)).await;
    assert!(result.is_err(), "Bad query should fail initially");

    // Auto-fix should work
    let err_msg = result.unwrap_err().to_string();
    let fixed = nba_agent::db::DbContext::auto_fix_sql(bad_query, &err_msg);
    assert!(fixed.is_some(), "Should auto-fix player_name → first_name or similar");

    // Fixed query should succeed
    let fixed_query = fixed.unwrap();
    let rows = db.run_sql(fixed_query, Some(1)).await.unwrap();
    assert!(!rows.is_empty(), "Fixed query should return results");
}

#[test]
fn test_auto_fix_sql_multiple_candidates() {
    let query = "SELECT pts FROM player_game_stats LIMIT 5;";
    let error = "Binder Error: Referenced column \"pts\" not found in FROM clause!\n\
                 Candidate bindings: \"pts_home\", \"pts_away\", \"pts_total\"\n\
                 LINE 1: SELECT pts FROM player_game_stats LIMIT 5;";
    let fixed = nba_agent::db::DbContext::auto_fix_sql(query, error).unwrap();
    assert!(fixed.contains("pts_home"), "Should pick first candidate: pts_home");
    assert!(!fixed.contains("pts_away"), "Should not pick second candidate");
}

// ---------------------------------------------------------------------------
// Insight cards tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_generate_insights_returns_cards() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let insights = db.generate_insights().await;
    assert!(insights.total_queries > 0, "Should have at least one card");
    assert_eq!(insights.cards.len(), insights.total_queries);
    // At least the simple count queries should succeed
    let successful = insights.cards.iter().filter(|c| c.error.is_none()).count();
    assert!(successful > 0, "At least some insight cards should succeed");
}

#[tokio::test]
async fn test_insight_cards_have_required_fields() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let insights = db.generate_insights().await;
    for card in &insights.cards {
        assert!(!card.id.is_empty(), "Card should have id");
        assert!(!card.title.is_empty(), "Card should have title");
        assert!(!card.category.is_empty(), "Card should have category");
    }
}

#[tokio::test]
async fn test_game_count_insight_accurate() {
    let db = match get_test_db() {
        Some(d) => d,
        None => return,
    };
    let insights = db.generate_insights().await;
    let game_card = insights.cards.iter().find(|c| c.id == "total_games");
    assert!(game_card.is_some(), "Should have total_games card");
    let card = game_card.unwrap();
    if card.error.is_none() {
        let actual: i64 = db.run_sql("SELECT COUNT(*) as val FROM game;".to_string(), Some(1)).await.unwrap()[0]["val"].as_i64().unwrap();
        let card_val: i64 = card.value.replace(',', "").parse().unwrap_or(0);
        assert_eq!(card_val, actual, "Game count should match actual DB count");
    }
}
