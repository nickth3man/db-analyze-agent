use nba_agent::db;
use std::net::SocketAddr;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let db_path = "../data/nba-data.duckdb";
    tracing::info!("Opening NBA DuckDB database at {}", db_path);
    let db = db::DbContext::new(db_path)?;
    let state = nba_agent::build_state(db).await?;

    let app = nba_agent::build_router(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    tracing::info!("NBA Database Agent server online at http://localhost:3000");
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await?;
    Ok(())
}
