use nba_agent::db;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let db_path = std::env::var("DATABASE_PATH").unwrap_or_else(|_| "../data/nba-data.duckdb".to_string());
    let bind_addr = std::env::var("BIND_ADDRESS").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port: u16 = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string()).parse().unwrap_or(3000);
    let sessions_path = std::env::var("SESSIONS_PATH").unwrap_or_else(|_| "data/sessions.json".to_string());

    tracing::info!("Opening NBA DuckDB database at {}", db_path);
    let db = db::DbContext::new(&db_path)?;
    let state = nba_agent::build_state(db, sessions_path).await?;

    let app = nba_agent::build_router(state);

    let addr = format!("{}:{}", bind_addr, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("NBA Database Agent server online at http://{}", addr);
    axum::serve(listener, app).await?;

    Ok(())
}
