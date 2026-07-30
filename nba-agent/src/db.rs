use duckdb::{AccessMode, Config, Connection, Result as DuckResult, types::ValueRef};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::sync::Arc;

#[derive(Clone)]
pub struct DbContext {
    conn: Arc<Mutex<Connection>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableSchemaInfo {
    pub table_name: String,
    pub columns: Vec<ColumnInfo>,
    pub sample_rows: Vec<Value>,
    pub total_rows: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResult {
    pub matched_tables: Vec<String>,
    pub matched_columns: Vec<(String, String)>, // (table, column)
}

impl DbContext {
    pub fn new(path: &str) -> DuckResult<Self> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(path, config)?;
        Ok(Self { conn: Arc::new(Mutex::new(conn)) })
    }

    /// Execute arbitrary SQL and return results capped at `max_rows` (default 50)
    pub async fn run_sql(&self, query: String, max_rows_opt: Option<usize>) -> anyhow::Result<Vec<Value>> {
        let conn_arc = self.conn.clone();
        let max_rows = max_rows_opt.unwrap_or(50);

        let results = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<Value>> {
            let conn = conn_arc.lock();
            let mut stmt = conn.prepare(&query)?;

            // 1. Collect row values
            let mut raw_rows: Vec<Vec<Value>> = Vec::new();
            let mut was_capped = false;
            {
                let mut rows = stmt.query([])?;
                while let Some(row) = rows.next()? {
                    if raw_rows.len() >= max_rows {
                        was_capped = true;
                        break;
                    }
                    let mut row_vals = Vec::new();
                    let mut idx = 0;
                    while let Ok(val_ref) = row.get_ref(idx) {
                        let val = match val_ref {
                            ValueRef::Null => Value::Null,
                            ValueRef::Boolean(b) => json!(b),
                            ValueRef::TinyInt(v) => json!(v),
                            ValueRef::SmallInt(v) => json!(v),
                            ValueRef::Int(v) => json!(v),
                            ValueRef::BigInt(v) => json!(v),
                            ValueRef::HugeInt(v) => json!(v.to_string()),
                            ValueRef::Float(f) => json!(f),
                            ValueRef::Double(d) => json!(d),
                            ValueRef::Text(t) => json!(String::from_utf8_lossy(t)),
                            ValueRef::Blob(b) => json!(format!("<blob len={}>", b.len())),
                            ValueRef::Timestamp(_u, t) => json!(t.to_string()),
                            ValueRef::Date32(d) => json!(d.to_string()),
                            ValueRef::Time64(_u, t) => json!(t.to_string()),
                            ValueRef::Decimal(d) => json!(d.to_string()),
                            _ => json!("<unsupported type>"),
                        };
                        row_vals.push(val);
                        idx += 1;
                    }
                    raw_rows.push(row_vals);
                }
            } // `rows` dropped here!

            // 2. Now `stmt` is unborrowed and executed; extract column names
            let col_count = stmt.column_count();
            let col_names: Vec<String> =
                (0..col_count).map(|i| stmt.column_name(i).map(|s| s.to_string()).unwrap_or_default()).collect();

            // 3. Assemble JSON objects
            let mut results = Vec::new();
            for row_vals in raw_rows {
                let mut map = Map::new();
                for (i, name) in col_names.iter().enumerate() {
                    let val = row_vals.get(i).cloned().unwrap_or(Value::Null);
                    map.insert(name.clone(), val);
                }
                results.push(Value::Object(map));
            }

            if was_capped {
                let mut notice_map = Map::new();
                notice_map.insert(
                    "_notice".to_string(),
                    json!(format!(
                        "Result capped at {} rows. Add explicit LIMIT or aggregate for complete data.",
                        max_rows
                    )),
                );
                results.push(Value::Object(notice_map));
            }

            Ok(results)
        })
        .await??;

        Ok(results)
    }

    /// List tables matching optional prefix or pattern
    pub async fn list_tables(&self, pattern_opt: Option<String>) -> anyhow::Result<Vec<String>> {
        let conn_arc = self.conn.clone();
        let pattern = pattern_opt.unwrap_or_else(|| "%".to_string());

        let tables = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<String>> {
            let conn = conn_arc.lock();
            let mut stmt = conn.prepare(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main' AND table_name LIKE ? ORDER BY table_name LIMIT 100;",
            )?;
            let mut rows = stmt.query([&pattern])?;
            let mut list = Vec::new();
            while let Some(row) = rows.next()? {
                let name: String = row.get(0)?;
                list.push(name);
            }
            Ok(list)
        })
        .await??;

        Ok(tables)
    }

    /// Search across table names and column names matching a keyword
    pub async fn search_tables(&self, keyword: String) -> anyhow::Result<SearchResult> {
        let conn_arc = self.conn.clone();
        let kw_lower = keyword.to_lowercase();

        let search_res = tokio::task::spawn_blocking(move || -> anyhow::Result<SearchResult> {
            let conn = conn_arc.lock();

            // Search table names
            let mut stmt = conn.prepare(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main' AND lower(table_name) LIKE ? ORDER BY table_name LIMIT 30;",
            )?;
            let pattern = format!("%{}%", kw_lower);
            let mut rows = stmt.query([&pattern])?;
            let mut matched_tables = Vec::new();
            while let Some(row) = rows.next()? {
                let name: String = row.get(0)?;
                matched_tables.push(name);
            }

            // Search column names
            let mut stmt_col = conn.prepare(
                "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema='main' AND lower(column_name) LIKE ? ORDER BY table_name, column_name LIMIT 50;",
            )?;
            let mut rows_col = stmt_col.query([&pattern])?;
            let mut matched_columns = Vec::new();
            while let Some(row) = rows_col.next()? {
                let t_name: String = row.get(0)?;
                let c_name: String = row.get(1)?;
                matched_columns.push((t_name, c_name));
            }

            Ok(SearchResult {
                matched_tables,
                matched_columns,
            })
        })
        .await??;

        Ok(search_res)
    }

    /// Describe a specific table (schema + 3 sample rows)
    pub async fn describe_table(&self, table_name: String) -> anyhow::Result<TableSchemaInfo> {
        let conn_arc = self.conn.clone();
        let tbl = table_name.clone();

        let info = tokio::task::spawn_blocking(move || -> anyhow::Result<TableSchemaInfo> {
            let conn = conn_arc.lock();

            let mut stmt = conn.prepare(
                "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema='main' AND table_name=? ORDER BY ordinal_position;",
            )?;
            let mut rows = stmt.query([&tbl])?;
            let mut columns = Vec::new();
            while let Some(row) = rows.next()? {
                columns.push(ColumnInfo {
                    name: row.get(0)?,
                    data_type: row.get(1)?,
                    is_nullable: row.get(2)?,
                });
            }

            // Sample 3 rows
            let sample_query = format!("SELECT * FROM \"{}\" LIMIT 3;", tbl.replace('"', "\"\""));
            let mut sample_stmt = conn.prepare(&sample_query)?;
            let mut raw_sample_rows = Vec::new();
            {
                let mut sample_rows_iter = sample_stmt.query([])?;
                while let Some(row) = sample_rows_iter.next()? {
                    let mut row_vals = Vec::new();
                    let mut idx = 0;
                    while let Ok(val_ref) = row.get_ref(idx) {
                        let val = match val_ref {
                            ValueRef::Null => Value::Null,
                            ValueRef::Boolean(b) => json!(b),
                            ValueRef::Int(v) => json!(v),
                            ValueRef::BigInt(v) => json!(v),
                            ValueRef::Float(f) => json!(f),
                            ValueRef::Double(d) => json!(d),
                            ValueRef::Text(t) => json!(String::from_utf8_lossy(t)),
                            _ => json!("<val>"),
                        };
                        row_vals.push(val);
                        idx += 1;
                    }
                    raw_sample_rows.push(row_vals);
                }
            } // `sample_rows_iter` dropped here!

            let col_count = sample_stmt.column_count();
            let col_names: Vec<String> = (0..col_count)
                .map(|i| sample_stmt.column_name(i).map(|s| s.to_string()).unwrap_or_default())
                .collect();

            let mut sample_rows = Vec::new();
            for row_vals in raw_sample_rows {
                let mut map = Map::new();
                for (i, name) in col_names.iter().enumerate() {
                    let val = row_vals.get(i).cloned().unwrap_or(Value::Null);
                    map.insert(name.clone(), val);
                }
                sample_rows.push(Value::Object(map));
            }

            Ok(TableSchemaInfo {
                table_name: tbl,
                columns,
                sample_rows,
                total_rows: None,
            })
        })
        .await??;

        Ok(info)
    }

    /// Run EXPLAIN to validate SQL syntax and get query plan without execution
    pub async fn explain_query(&self, query: String) -> anyhow::Result<String> {
        let explain_sql = format!("EXPLAIN {}", query);
        let rows = self.run_sql(explain_sql, Some(20)).await?;
        Ok(serde_json::to_string_pretty(&rows)?)
    }

    /// Get curated schema context overview for key entities
    pub async fn get_schema_summary(&self) -> anyhow::Result<String> {
        let key_tables = vec![
            "player",
            "team",
            "game",
            "common_player_info",
            "player_game_stats",
            "play_by_play",
            "line_score",
            "draft_history",
        ];

        let mut summary = String::from("Core Database Schema (Key Entities):\n");
        for tbl in key_tables {
            if let Ok(info) = self.describe_table(tbl.to_string()).await {
                summary.push_str(&format!("\n• Table `{}` ({} columns):\n  ", info.table_name, info.columns.len()));
                let col_strs: Vec<String> =
                    info.columns.iter().map(|c| format!("{}: {}", c.name, c.data_type)).collect();
                summary.push_str(&col_strs.join(", "));
                summary.push('\n');
            }
        }
        summary.push_str(
            "\nNote: Over 580 total tables exist. Use `list_tables`, `search_tables`, or `describe_table` to explore schema elements.",
        );
        Ok(summary)
    }
}
