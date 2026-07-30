#[path = "../src/db.rs"]
mod db;

use db::DbContext;

fn get_test_db() -> Option<DbContext> {
    let path = std::env::var("DATABASE_PATH").unwrap_or_else(|_| "../data/nba-data.duckdb".to_string());
    if std::path::Path::new(&path).exists() {
        DbContext::new(&path).ok()
    } else {
        println!("Database not found at {}, skipping live DuckDB test.", path);
        None
    }
}

#[tokio::test]
async fn test_db_context_read_only() -> anyhow::Result<()> {
    let db = match get_test_db() {
        Some(d) => d,
        None => return Ok(()),
    };

    // Test basic query
    let rows = db.run_sql("SELECT 1 as test_val;".to_string(), Some(1)).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["test_val"], 1);

    Ok(())
}

#[tokio::test]
async fn test_schema_discovery() -> anyhow::Result<()> {
    let db = match get_test_db() {
        Some(d) => d,
        None => return Ok(()),
    };

    // Test list_tables
    let tables = db.list_tables(Some("player%".to_string())).await?;
    assert!(!tables.is_empty());

    // Test search_tables
    let search_res = db.search_tables("clutch".to_string()).await?;
    assert!(!search_res.matched_tables.is_empty() || !search_res.matched_columns.is_empty());

    // Test describe_table
    let player_info = db.describe_table("player".to_string()).await?;
    assert_eq!(player_info.table_name, "player");
    assert!(!player_info.columns.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_explain_query() -> anyhow::Result<()> {
    let db = match get_test_db() {
        Some(d) => d,
        None => return Ok(()),
    };

    let explain_res = db.explain_query("SELECT * FROM player LIMIT 5;".to_string()).await?;
    assert!(!explain_res.is_empty());

    Ok(())
}
