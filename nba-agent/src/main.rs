use nba_agent::db;

fn env_config() -> (String, String, u16, String) {
    let db_path = std::env::var("DATABASE_PATH").unwrap_or_else(|_| "../data/nba-data.duckdb".to_string());
    let bind_addr = std::env::var("BIND_ADDRESS").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port: u16 = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string()).parse().unwrap_or(3000);
    let sessions_path = std::env::var("SESSIONS_PATH").unwrap_or_else(|_| "data/sessions.json".to_string());
    (db_path, bind_addr, port, sessions_path)
}

/// Open the warehouse, build state, and assemble the router (testable without binding).
async fn build_app() -> anyhow::Result<axum::Router> {
    let (db_path, _, _, sessions_path) = env_config();
    tracing::info!("Opening NBA DuckDB database at {}", db_path);
    let db = db::DbContext::new(&db_path)?;
    let state = nba_agent::build_state(db, sessions_path).await?;
    Ok(nba_agent::build_router(state))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let (_, bind_addr, port, _) = env_config();
    let app = build_app().await?;

    let addr = format!("{}:{}", bind_addr, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("NBA Database Agent server online at http://{}", addr);
    axum::serve(listener, app).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_env_config_defaults() {
        let _guard = ENV_MUTEX.lock().unwrap();
        unsafe {
            std::env::remove_var("DATABASE_PATH");
            std::env::remove_var("BIND_ADDRESS");
            std::env::remove_var("PORT");
            std::env::remove_var("SESSIONS_PATH");
        }
        let (db, bind, port, sessions) = env_config();
        assert_eq!(db, "../data/nba-data.duckdb");
        assert_eq!(bind, "0.0.0.0");
        assert_eq!(port, 3000);
        assert_eq!(sessions, "data/sessions.json");
    }

    #[test]
    fn test_env_config_overrides() {
        let _guard = ENV_MUTEX.lock().unwrap();
        unsafe {
            std::env::set_var("DATABASE_PATH", "x.duckdb");
            std::env::set_var("BIND_ADDRESS", "127.0.0.1");
            std::env::set_var("PORT", "9999");
            std::env::set_var("SESSIONS_PATH", "s.json");
        }
        let (db, bind, port, sessions) = env_config();
        assert_eq!(db, "x.duckdb");
        assert_eq!(bind, "127.0.0.1");
        assert_eq!(port, 9999);
        assert_eq!(sessions, "s.json");
    }

    #[test]
    fn test_env_config_bad_port_falls_back() {
        let _guard = ENV_MUTEX.lock().unwrap();
        unsafe {
            std::env::set_var("PORT", "not-a-number");
        }
        let (_, _, port, _) = env_config();
        assert_eq!(port, 3000);
    }

    #[test]
    fn test_router_health_smoke() {
        // Full state + router construction against the real warehouse (skips when absent).
        let db_path = std::env::var("DATABASE_PATH").unwrap_or_else(|_| "../data/nba-data.duckdb".to_string());
        if !std::path::Path::new(&db_path).exists() {
            return;
        }
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let db = db::DbContext::new(&db_path).unwrap();
            let state = nba_agent::build_state(db, "data/sessions.json".to_string()).await.unwrap();
            let app = nba_agent::build_router(state);
            let res = app.oneshot(Request::builder().uri("/api/health").body(Body::empty()).unwrap()).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);
        });
    }
}
