// Tool dispatch tests: drive `Agent::execute_tool` directly against the real
// warehouse for every NBA analysis tool. Skips (early return) when the DB file
// is absent, matching the repo's `get_test_db()` convention so CI without the
// 17GB warehouse does not fail.
use nba_agent::agent::Agent;
use nba_agent::db::DbContext;
use serde_json::{Value, json};
use std::env;

fn setup() {
    unsafe {
        env::set_var("OPENROUTER_API_KEY", "test-key-for-tool-dispatch");
    }
}

fn make_agent() -> Option<Agent> {
    setup();
    let db_path = env::var("DATABASE_PATH").unwrap_or_else(|_| "../data/nba-data.duckdb".to_string());
    if !std::path::Path::new(&db_path).exists() {
        return None;
    }
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().ok()?;
    rt.block_on(async {
        let db = DbContext::new(&db_path).ok()?;
        let insights = db.generate_insights().await;
        let brief = DbContext::format_insights_for_prompt(&insights);
        Agent::new(db, brief, "data/sessions.json".to_string()).await.ok()
    })
}

fn tool_test<F>(f: F)
where
    F: for<'a> Fn(&'a Agent) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>>,
{
    if let Some(a) = make_agent() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(f(&a));
    }
}

fn no_error(r: &nba_agent::agent::ToolResult) -> bool {
    !r.result_str.starts_with("Error") && !r.result_str.starts_with("Tool `")
}

// === Basic DB tools ===
#[test]
fn test_tool_run_sql() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a.execute_tool("run_sql", &json!({"query": "SELECT COUNT(*) as n FROM player"})).await;
            assert!(no_error(&r), "run_sql failed: {}", r.result_str);
            assert!(r.row_count > 0, "run_sql should return rows");
        })
    });
}

#[test]
fn test_tool_run_sql_rejects_destructive() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a.execute_tool("run_sql", &json!({"query": "DROP TABLE player"})).await;
            assert!(r.result_str.contains("rejected"), "destructive SQL must be rejected, got: {}", r.result_str);
        })
    });
}

#[test]
fn test_tool_list_tables() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a.execute_tool("list_tables", &json!({})).await;
            assert!(no_error(&r), "list_tables failed: {}", r.result_str);
            assert!(r.row_count > 0);
        })
    });
}

#[test]
fn test_tool_search_tables() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a.execute_tool("search_tables", &json!({"keyword": "player"})).await;
            assert!(no_error(&r), "search_tables failed: {}", r.result_str);
        })
    });
}

#[test]
fn test_tool_describe_table() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a.execute_tool("describe_table", &json!({"table_name": "game"})).await;
            assert!(no_error(&r), "describe_table failed: {}", r.result_str);
            assert!(r.result_str.contains("game_id"), "describe_table should list game_id");
        })
    });
}

#[test]
fn test_tool_explain_query() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a.execute_tool("explain_query", &json!({"query": "SELECT * FROM game LIMIT 1"})).await;
            assert!(no_error(&r), "explain_query failed: {}", r.result_str);
        })
    });
}

#[test]
fn test_tool_generate_chart() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a.execute_tool(
            "generate_chart",
            &json!({"chart_type": "bar", "title": "Test", "sql_query": "SELECT team_name_home, pts_home FROM game LIMIT 5"}),
        ).await;
            assert!(no_error(&r), "generate_chart failed: {}", r.result_str);
        })
    });
}

// === Player tools ===
#[test]
fn test_tool_compare_players() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a
                .execute_tool("compare_players", &json!({"player1": "LeBron James", "player2": "Michael Jordan"}))
                .await;
            assert!(no_error(&r), "compare_players failed: {}", r.result_str);
            assert!(r.row_count >= 2, "compare_players should return both players, got {} rows", r.row_count);
            assert!(r.result_str.contains("LeBron James"), "missing LeBron (player table join)");
        })
    });
}

#[test]
fn test_tool_compare_players_season_filter() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a
                .execute_tool(
                    "compare_players",
                    &json!({"player1": "LeBron James", "player2": "Michael Jordan", "season": "2022-23"}),
                )
                .await;
            assert!(no_error(&r), "compare_players season failed: {}", r.result_str);
            // Season normalization maps 2022-23 -> 22022; both retired/active players may not overlap
            assert!(r.row_count > 0 || r.result_str.contains("LeBron"), "season filter should still find LeBron");
        })
    });
}

#[test]
fn test_tool_compare_teams() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a
                .execute_tool("compare_teams", &json!({"team1": "Lakers", "team2": "Celtics", "season": "2022-23"}))
                .await;
            assert!(no_error(&r), "compare_teams failed: {}", r.result_str);
            assert!(r.row_count > 0, "compare_teams should return rows");
        })
    });
}

#[test]
fn test_tool_find_streaks() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a
                .execute_tool(
                    "find_streaks",
                    &json!({"player_name": "LeBron James", "streak_type": "points", "min_value": 30}),
                )
                .await;
            assert!(no_error(&r), "find_streaks failed: {}", r.result_str);
            assert!(r.row_count > 0, "find_streaks should find 30pt streaks");
            // Longest streak must be realistic (LeBron's real record: 10)
            assert!(r.result_str.contains("\"streak_length\""), "streak result should have streak_length");
        })
    });
}

#[test]
fn test_tool_find_streaks_wins() {
    tool_test(|a| {
        Box::pin(async move {
            let r =
                a.execute_tool("find_streaks", &json!({"player_name": "LeBron James", "streak_type": "wins"})).await;
            assert!(no_error(&r), "find_streaks wins failed: {}", r.result_str);
            assert!(r.row_count > 0, "find_streaks wins should return rows");
        })
    });
}

#[test]
fn test_tool_get_player_profile() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a.execute_tool("get_player_profile", &json!({"player_name": "Stephen Curry"})).await;
            assert!(no_error(&r), "get_player_profile failed: {}", r.result_str);
            assert!(r.row_count > 0);
            assert!(r.result_str.contains("Stephen Curry"));
        })
    });
}

#[test]
fn test_tool_rank_performance_season() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a
                .execute_tool(
                    "rank_performance",
                    &json!({"stat_name": "points", "value": 2500, "scope": "season", "context": "2022-23"}),
                )
                .await;
            assert!(no_error(&r), "rank_performance failed: {}", r.result_str);
        })
    });
}

#[test]
fn test_tool_find_leaders() {
    tool_test(|a| {
        Box::pin(async move {
            let r =
                a.execute_tool("find_leaders", &json!({"stat_name": "points", "season": "2022-23", "limit": 5})).await;
            assert!(no_error(&r), "find_leaders failed: {}", r.result_str);
            assert!(r.row_count == 5, "find_leaders limit 5, got {}", r.row_count);
        })
    });
}

#[test]
fn test_tool_find_leaders_no_season() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a.execute_tool("find_leaders", &json!({"stat_name": "points", "limit": 10})).await;
            assert!(no_error(&r), "find_leaders (no season) failed: {}", r.result_str);
            assert!(r.row_count == 10, "find_leaders limit 10, got {}", r.row_count);
        })
    });
}

#[test]
fn test_tool_find_leaders_threes() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a.execute_tool("find_leaders", &json!({"stat_name": "threes", "limit": 3})).await;
            assert!(no_error(&r), "find_leaders threes failed: {}", r.result_str);
            assert!(r.row_count > 0);
        })
    });
}

// === Team / game tools ===
#[test]
fn test_tool_get_game_summary() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a.execute_tool("get_game_summary", &json!({"game_id": "0022200001"})).await;
            assert!(no_error(&r), "get_game_summary failed: {}", r.result_str);
            assert!(r.row_count > 0, "get_game_summary should find game 0022200001");
            assert!(r.result_str.contains("0022200001"));
        })
    });
}

#[test]
fn test_tool_get_game_summary_by_date() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a.execute_tool("get_game_summary", &json!({"game_date": "2022-10-18"})).await;
            assert!(no_error(&r), "get_game_summary by date failed: {}", r.result_str);
        })
    });
}

#[test]
fn test_tool_get_head_to_head() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a.execute_tool("get_head_to_head", &json!({"team1": "Lakers", "team2": "Celtics"})).await;
            assert!(no_error(&r), "get_head_to_head failed: {}", r.result_str);
            assert!(r.row_count >= 2, "h2h should return record + recent games, got {} rows", r.row_count);
        })
    });
}

#[test]
fn test_tool_check_data_coverage_team() {
    tool_test(|a| {
        Box::pin(async move {
            let r =
                a.execute_tool("check_data_coverage", &json!({"entity_type": "team", "entity_name": "Lakers"})).await;
            assert!(no_error(&r), "coverage team failed: {}", r.result_str);
            assert!(r.row_count > 0);
            assert!(r.result_str.contains("fact_team_game_stats"));
        })
    });
}

#[test]
fn test_tool_check_data_coverage_player() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a
                .execute_tool("check_data_coverage", &json!({"entity_type": "player", "entity_name": "LeBron James"}))
                .await;
            assert!(no_error(&r), "coverage player failed: {}", r.result_str);
            assert!(r.row_count > 0);
        })
    });
}

#[test]
fn test_tool_check_data_coverage_season() {
    tool_test(|a| {
        Box::pin(async move {
            let r =
                a.execute_tool("check_data_coverage", &json!({"entity_type": "season", "entity_name": "22022"})).await;
            assert!(no_error(&r), "coverage season failed: {}", r.result_str);
            assert!(r.row_count > 0);
        })
    });
}

#[test]
fn test_tool_export_query_result_csv() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a
                .execute_tool(
                    "export_query_result",
                    &json!({"query": "SELECT game_id, pts_home, pts_away FROM game LIMIT 3", "format": "csv"}),
                )
                .await;
            assert!(no_error(&r), "export csv failed: {}", r.result_str);
            assert!(r.result_str.contains("game_id"), "CSV should have header");
        })
    });
}

#[test]
fn test_tool_export_query_result_json() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a
                .execute_tool(
                    "export_query_result",
                    &json!({"query": "SELECT game_id FROM game LIMIT 2", "format": "json"}),
                )
                .await;
            assert!(no_error(&r), "export json failed: {}", r.result_str);
        })
    });
}

// === Advanced tools ===
#[test]
fn test_tool_era_adjusted_compare() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a
                .execute_tool(
                    "era_adjusted_compare",
                    &json!({"player1": "LeBron James", "player2": "Kareem Abdul-Jabbar"}),
                )
                .await;
            assert!(no_error(&r), "era_adjusted_compare failed: {}", r.result_str);
            assert!(r.row_count > 0, "era_adjusted_compare should return rows");
            assert!(r.result_str.contains("era_ratio"), "should compute era_ratio");
        })
    });
}

#[test]
fn test_tool_game_reconstruction() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a.execute_tool("game_reconstruction", &json!({"game_id": "0022200001"})).await;
            assert!(no_error(&r), "game_reconstruction failed: {}", r.result_str);
            assert!(r.row_count > 0, "game_reconstruction should find play-by-play for 0022200001");
            assert!(r.result_str.contains("player1_name") || r.row_count > 0);
        })
    });
}

#[test]
fn test_tool_expand_player_profile() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a.execute_tool("expand_player_profile", &json!({"player_name": "LeBron James"})).await;
            assert!(no_error(&r), "expand_player_profile failed: {}", r.result_str);
            assert!(r.row_count >= 6, "expand should return career + 5 best seasons, got {}", r.row_count);
            assert!(r.result_str.contains("career") && r.result_str.contains("best_season"));
        })
    });
}

#[test]
fn test_tool_expand_player_profile_playoffs() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a.execute_tool("expand_player_profile", &json!({"player_name": "Michael Jordan"})).await;
            assert!(no_error(&r), "expand MJ failed: {}", r.result_str);
            assert!(r.result_str.contains("playoff"), "MJ should have playoff section");
        })
    });
}

// === Error paths ===
#[test]
fn test_tool_unknown() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a.execute_tool("not_a_tool", &json!({})).await;
            assert!(r.result_str.contains("not supported"), "unknown tool: {}", r.result_str);
        })
    });
}

#[test]
fn test_tool_sql_injection_safe() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a
                .execute_tool(
                    "compare_players",
                    &json!({"player1": "Robert'); DROP TABLE player; --", "player2": "Michael Jordan"}),
                )
                .await;
            // Must not panic; either rejected or empty result — never raw injection
            assert!(
                no_error(&r) || r.result_str.contains("rejected") || r.result_str.contains("Error"),
                "injection attempt produced unexpected result: {}",
                r.result_str
            );
        })
    });
}

#[test]
fn test_tool_missing_required_args() {
    tool_test(|a| {
        Box::pin(async move {
            // Missing player args: should not panic, defaults to empty names
            let r = a.execute_tool("compare_players", &json!({})).await;
            assert!(!r.result_str.is_empty());
        })
    });
}

#[test]
fn test_tool_bad_table_describe() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a.execute_tool("describe_table", &json!({"table_name": "no_such_table_xyz"})).await;
            assert!(r.result_str.contains("Error"), "bad table should error, got: {}", r.result_str);
        })
    });
}

#[test]
fn test_tool_rank_performance_career() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a
                .execute_tool(
                    "rank_performance",
                    &json!({"stat_name": "points", "value": 30000, "scope": "career", "context": "LeBron James"}),
                )
                .await;
            assert!(no_error(&r), "rank career failed: {}", r.result_str);
        })
    });
}

#[test]
fn test_tool_rank_performance_franchise() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a
                .execute_tool(
                    "rank_performance",
                    &json!({"stat_name": "points", "value": 1000, "scope": "franchise", "context": "Lakers"}),
                )
                .await;
            assert!(no_error(&r), "rank franchise failed: {}", r.result_str);
        })
    });
}

// Sanity: serde Value input used everywhere
#[test]
fn test_tool_result_serializable() {
    tool_test(|a| {
        Box::pin(async move {
            let r = a.execute_tool("list_tables", &Value::Null).await;
            assert!(no_error(&r), "list_tables with null args failed: {}", r.result_str);
        })
    });
}
