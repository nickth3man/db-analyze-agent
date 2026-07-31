use duckdb::{AccessMode, Config, Connection, Result as DuckResult, types::ValueRef};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
pub struct DbContext {
    pool: r2d2::Pool<duckdb::DuckdbConnectionManager>,
    cache: moka::sync::Cache<String, Vec<Value>>,
    history: Arc<Mutex<Vec<DbHistoryEntry>>>,
    lifetime_query_count: Arc<AtomicUsize>,
    /// Default row cap for run_sql (env: ROW_CAP, default 50).
    row_cap: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbHistoryEntry {
    pub timestamp: u64,
    pub sql: String,
    pub row_count: usize,
    pub elapsed_ms: u64,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_category: Option<String>,
    #[serde(default)]
    pub cache_hit: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedColumn {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub is_fk_candidate: bool,
    pub referenced_table: Option<String>,
    pub sample_values: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedTable {
    pub table_name: String,
    pub row_count: i64,
    pub column_count: usize,
    pub columns: Vec<EnrichedColumn>,
    pub date_range: Option<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedSchema {
    pub tables: Vec<EnrichedTable>,
    pub total_tables: usize,
    pub fk_relationships: Vec<(String, String, String)>, // (from_table, from_col, to_table)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsightCard {
    pub id: String,
    pub title: String,
    pub value: String,
    pub subtitle: String,
    pub category: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsightsResponse {
    pub cards: Vec<InsightCard>,
    pub generated_at: String,
    pub total_queries: usize,
    pub successful: usize,
    pub total_tables: usize,
}

impl DbContext {
    pub fn new(path: &str) -> DuckResult<Self> {
        // Cap each DuckDB instance's buffer pool. DuckDB defaults to ~80% of
        // physical RAM per instance; with several concurrent instances (e.g.
        // parallel tests each opening the warehouse) RAM is oversubscribed and
        // the machine thrashes. Override via DUCKDB_MEMORY_LIMIT (e.g. "4GB").
        let memory_limit = std::env::var("DUCKDB_MEMORY_LIMIT").unwrap_or_else(|_| "2GB".to_string());
        let config = Config::default().access_mode(AccessMode::ReadOnly)?.max_memory(&memory_limit)?;
        let manager = duckdb::DuckdbConnectionManager::file_with_flags(path, config)?;
        let pool = r2d2::Pool::builder()
            .max_size(16)
            .build(manager)
            .map_err(|e| duckdb::Error::DuckDBFailure(duckdb::ffi::Error::new(1), Some(e.to_string())))?;
        let cache_max: u64 =
            std::env::var("CACHE_MAX_CAPACITY").unwrap_or_else(|_| "200".to_string()).parse().unwrap_or(200);
        let cache_ttl: u64 = std::env::var("CACHE_TTL_SECS").unwrap_or_else(|_| "60".to_string()).parse().unwrap_or(60);
        let cache = moka::sync::Cache::builder()
            .max_capacity(cache_max)
            .time_to_live(std::time::Duration::from_secs(cache_ttl))
            .build();
        let row_cap: usize = std::env::var("ROW_CAP").unwrap_or_else(|_| "50".to_string()).parse().unwrap_or(50);
        Ok(Self {
            pool,
            cache,
            history: Arc::new(Mutex::new(Vec::new())),
            lifetime_query_count: Arc::new(AtomicUsize::new(0)),
            row_cap,
        })
    }

    /// Execute arbitrary SQL and return results capped at `max_rows` (default 50).
    /// Results are cached with a 60-second TTL for repeated queries.
    pub async fn run_sql(&self, query: String, max_rows_opt: Option<usize>) -> anyhow::Result<Vec<Value>> {
        // Lifetime counter: every call counts, success or failure.
        self.lifetime_query_count.fetch_add(1, Ordering::Relaxed);

        // Validate: reject destructive SQL patterns
        if let Err(reason) = Self::validate_sql(&query) {
            self.record_history(&query, 0, 0, false, Some("validation".to_string()), false);
            return Err(anyhow::anyhow!("SQL rejected: {}", reason));
        }
        let query_saved = query.clone();
        let max_rows = max_rows_opt.unwrap_or(self.row_cap);
        let cache_key = format!("{}|{}", query.trim(), max_rows);
        // Check cache (moka handles TTL and concurrency internally)
        if let Some(cached) = self.cache.get(&cache_key) {
            self.record_history(&query_saved, cached.len(), 0, true, Some("cache_hit".to_string()), true);
            return Ok(cached);
        }

        let pool = self.pool.clone();
        let started = std::time::Instant::now();
        let results = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<Value>> {
            let conn = pool.get()?;
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
        .await;

        match results {
            Ok(Ok(rows)) => {
                // Cache successful results
                self.cache.insert(cache_key, rows.clone());
                self.record_history(&query_saved, rows.len(), started.elapsed().as_millis() as u64, true, None, false);
                Ok(rows)
            }
            Ok(Err(e)) => {
                let err_msg = e.to_string();
                let category = Self::classify_sql_error(&err_msg);
                self.record_history(
                    &query_saved,
                    0,
                    started.elapsed().as_millis() as u64,
                    false,
                    Some(category),
                    false,
                );
                Err(e)
            }
            Err(join_err) => {
                self.record_history(
                    &query_saved,
                    0,
                    started.elapsed().as_millis() as u64,
                    false,
                    Some("spawn_failure".to_string()),
                    false,
                );
                Err(join_err.into())
            }
        }
    }

    /// Record a query execution to the bounded history ring buffer.
    fn record_history(
        &self,
        sql: &str,
        row_count: usize,
        elapsed_ms: u64,
        success: bool,
        error_category: Option<String>,
        cache_hit: bool,
    ) {
        let entry = DbHistoryEntry {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            sql: sql.to_string(),
            row_count,
            elapsed_ms,
            success,
            error_category,
            cache_hit,
            model: None,
            session_id: None,
            tool_name: None,
        };
        let mut history = self.history.lock();
        history.push(entry);
        if history.len() > 200 {
            history.remove(0);
        }
    }

    /// Lifetime count of SQL executions (success + failure + cache hit).
    pub fn get_lifetime_query_count(&self) -> usize {
        self.lifetime_query_count.load(Ordering::Relaxed)
    }

    /// Classify a DuckDB error message into a short category string.
    fn classify_sql_error(err_msg: &str) -> String {
        if err_msg.contains("Candidate bindings") {
            "column_not_found".to_string()
        } else if err_msg.contains("does not exist") {
            "table_not_found".to_string()
        } else if err_msg.contains("Syntax error") {
            "syntax_error".to_string()
        } else if err_msg.contains("Type mismatch") {
            "type_mismatch".to_string()
        } else {
            "execution_error".to_string()
        }
    }

    /// Validate SQL query using AST parsing. Rejects destructive statements
    /// even when keywords appear inside comments or string literals.
    ///
    /// Allowed: SELECT, WITH (CTE), EXPLAIN, SHOW, DESCRIBE, PRAGMA (read-only)
    /// Rejected: DDL (DROP/ALTER/CREATE/TRUNCATE), DML writes (INSERT/UPDATE/DELETE/MERGE/REPLACE)
    pub fn validate_sql(query: &str) -> Result<(), String> {
        use sqlparser::ast::Statement;
        use sqlparser::dialect::DuckDbDialect;
        use sqlparser::parser::Parser;

        let trimmed = query.trim();
        if trimmed.is_empty() {
            return Err("Empty query".to_string());
        }

        // Fast-path: PRAGMA is safe if it's a read-only pragma
        let upper = trimmed.to_uppercase();
        if upper.starts_with("PRAGMA") {
            let read_only_pragmas = ["TABLE_INFO", "DATABASE_LIST", "TABLES", "COLUMNS"];
            if read_only_pragmas.iter().any(|p| upper.contains(p)) {
                return Ok(());
            }
            return Err("PRAGMA write statements are not allowed".to_string());
        }

        // Parse with DuckDB dialect
        let dialect = DuckDbDialect {};
        let statements = Parser::parse_sql(&dialect, trimmed).map_err(|e| format!("SQL parse error: {}", e))?;

        if statements.is_empty() {
            return Err("Empty query after parsing".to_string());
        }

        for stmt in &statements {
            match stmt {
                // Allowed read-only statements
                Statement::Query(_) => {} // SELECT, WITH
                Statement::Explain { .. } | Statement::ExplainTable { .. } => {}
                Statement::ShowTables { .. }
                | Statement::ShowSchemas { .. }
                | Statement::ShowDatabases { .. }
                | Statement::ShowColumns { .. } => {}
                // Everything else is rejected
                other => {
                    let stmt_name = format!("{:?}", other);
                    let kind = if stmt_name.contains("Drop")
                        || stmt_name.contains("Alter")
                        || stmt_name.contains("Create")
                        || stmt_name.contains("Truncate")
                    {
                        "DDL"
                    } else if stmt_name.contains("Insert")
                        || stmt_name.contains("Update")
                        || stmt_name.contains("Delete")
                        || stmt_name.contains("Merge")
                    {
                        "DML write"
                    } else {
                        "unsupported"
                    };
                    return Err(format!("{} statements are not allowed on this read-only agent", kind));
                }
            }
        }

        Ok(())
    }

    /// Parse DuckDB error messages for "candidate bindings" and auto-correct
    /// column name typos. Returns the fixed SQL query if a correction was made.
    pub fn auto_fix_sql(original_query: &str, error_msg: &str) -> Option<String> {
        // Extract the bad column name: "Referenced column \"COL\" not found"
        let bad_col = extract_between(error_msg, "Referenced column \"", "\" not found")?;

        // Extract candidates: "Candidate bindings: \"A\", \"B\", ..."
        let candidates_str = error_msg.find("Candidate bindings:").and_then(|pos| {
            let after = &error_msg[pos + "Candidate bindings:".len()..];
            // Take everything until the next line or end
            after.split('\n').next()
        })?;

        let candidates: Vec<&str> =
            candidates_str.split(',').map(|s| s.trim().trim_matches('"')).filter(|s| !s.is_empty()).collect();

        if candidates.is_empty() {
            return None;
        }

        // Replace the bad column name with the first candidate
        let fixed = replace_sql_ident(original_query, bad_col, candidates[0]);
        Some(fixed)
    }

    /// List tables matching optional prefix or pattern
    pub async fn list_tables(&self, pattern_opt: Option<String>) -> anyhow::Result<Vec<String>> {
        let pool = self.pool.clone();
        let pattern = pattern_opt.unwrap_or_else(|| "%".to_string());

        let tables = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<String>> {
            let conn = pool.get()?;
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
        let pool = self.pool.clone();
        let kw_lower = keyword.to_lowercase();

        let search_res = tokio::task::spawn_blocking(move || -> anyhow::Result<SearchResult> {
            let conn = pool.get()?;

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
        let pool = self.pool.clone();
        let tbl = table_name.clone();

        let info = tokio::task::spawn_blocking(move || -> anyhow::Result<TableSchemaInfo> {
            let conn = pool.get()?;

            let columns = Self::get_columns_from_info(&conn, &tbl)?;
            let sample_rows = Self::get_sample_rows_json(&conn, &tbl)?;

            Ok(TableSchemaInfo { table_name: tbl, columns, sample_rows, total_rows: None })
        })
        .await??;

        Ok(info)
    }

    /// Fetch column metadata from information_schema for a table.
    fn get_columns_from_info(conn: &Connection, table_name: &str) -> anyhow::Result<Vec<ColumnInfo>> {
        let mut stmt = conn.prepare(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema='main' AND table_name=? ORDER BY ordinal_position;",
        )?;
        let mut rows = stmt.query([table_name])?;
        let mut columns = Vec::new();
        while let Some(row) = rows.next()? {
            columns.push(ColumnInfo { name: row.get(0)?, data_type: row.get(1)?, is_nullable: row.get(2)? });
        }
        Ok(columns)
    }

    /// Fetch sample rows from a table and return them as JSON objects keyed by column name.
    fn get_sample_rows_json(conn: &Connection, table_name: &str) -> anyhow::Result<Vec<Value>> {
        let sample_query = format!("SELECT * FROM \"{}\" LIMIT 3;", table_name.replace('"', "\"\""));
        let mut sample_stmt = conn.prepare(&sample_query)?;

        // Collect raw values per row
        let mut raw_rows = Vec::new();
        {
            let mut iter = sample_stmt.query([])?;
            while let Some(row) = iter.next()? {
                let mut vals = Vec::new();
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
                    vals.push(val);
                    idx += 1;
                }
                raw_rows.push(vals);
            }
        } // `iter` dropped here!

        // Build JSON objects keyed by column name
        let col_names: Vec<String> = (0..sample_stmt.column_count())
            .map(|i| sample_stmt.column_name(i).map(String::from).unwrap_or_default())
            .collect();

        let sample_rows = raw_rows
            .into_iter()
            .map(|vals| {
                let mut map = Map::new();
                for (i, name) in col_names.iter().enumerate() {
                    let val = vals.get(i).cloned().unwrap_or(Value::Null);
                    map.insert(name.clone(), val);
                }
                Value::Object(map)
            })
            .collect();

        Ok(sample_rows)
    }

    /// Run EXPLAIN to validate SQL syntax and get query plan without execution
    pub async fn explain_query(&self, query: String) -> anyhow::Result<String> {
        let explain_sql = format!("EXPLAIN {}", query);
        let rows = self.run_sql(explain_sql, Some(20)).await?;
        Ok(serde_json::to_string_pretty(&rows)?)
    }

    /// Build enriched schema with row counts, FK hints, date ranges, and sample values.
    /// Uses DuckDB's catalog-estimated row counts for speed (no table scans).
    pub async fn build_enriched_schema(&self, table_filter: Option<&[&str]>) -> anyhow::Result<EnrichedSchema> {
        let pool = self.pool.clone();
        let filter: Vec<String> = table_filter.map(|f| f.iter().map(|s| s.to_string()).collect()).unwrap_or_default();

        tokio::task::spawn_blocking(move || -> anyhow::Result<EnrichedSchema> {
            let conn = pool.get()?;

            // Get all table names with estimated row counts from DuckDB catalog (no scan)
            let mut all_tables_stmt = conn.prepare(
                "SELECT table_name, estimated_size FROM duckdb_tables() WHERE schema_name='main' AND estimated_size >= 0 ORDER BY estimated_size DESC;"
            )?;
            let mut table_rows = all_tables_stmt.query([])?;
            let mut table_names: Vec<String> = Vec::new();
            let mut estimated_counts: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
            while let Some(row) = table_rows.next()? {
                let name: String = row.get(0)?;
                let est: i64 = row.get(1)?;
                estimated_counts.insert(name.clone(), est);
                table_names.push(name);
            }
            drop(table_rows);
            drop(all_tables_stmt);

            // Filter or take key tables if filter provided
            let target_tables: Vec<String> = if filter.is_empty() {
                table_names
            } else {
                filter.into_iter().filter(|t| table_names.contains(t)).collect()
            };

            let mut tables = Vec::new();
            let mut fk_relationships = Vec::new();

            for tbl_name in &target_tables {
                // Use estimated row count from catalog (fast, no scan)
                let count = estimated_counts.get(tbl_name).copied().unwrap_or(-1);

                // Column info
                let mut col_stmt = match conn.prepare(
                    "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema='main' AND table_name=? ORDER BY ordinal_position;"
                ) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!("Failed to prepare column query for table `{}`: {}", tbl_name, e);
                        continue;
                    }
                };
                let mut col_rows = match col_stmt.query([tbl_name.as_str()]) {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!("Failed to query columns for table `{}`: {}", tbl_name, e);
                        continue;
                    }
                };
                let mut columns = Vec::new();
                while let Some(row) = col_rows.next()? {
                    let col_name: String = row.get(0)?;
                    let data_type: String = row.get(1)?;
                    let is_nullable_str: String = row.get(2)?;

                    // Detect FK: ends with _id, contains _id_ (e.g. team_id_home), or ends with _code
                    let has_id = col_name.ends_with("_id") || col_name.contains("_id_");
                    let is_fk = has_id || col_name.ends_with("_code");
                    let referenced_table = if is_fk {
                        // Extract table name: everything before the last _id or _code
                        let base = if let Some(pos) = col_name.rfind("_id") {
                            &col_name[..pos]
                        } else {
                            col_name.trim_end_matches("_code")
                        };
                        Some(base.to_string())
                    } else {
                        None
                    };

                    // Sample distinct values (top 3) for key-like columns
                    let sample_values: Vec<String> = if is_fk || col_name.contains("name") || col_name.contains("type") {
                        let q = format!(
                            "SELECT DISTINCT \"{}\" FROM \"{}\" LIMIT 3;",
                            col_name.replace('"', "\"\""),
                            tbl_name.replace('"', "\"\"")
                        );
                        match conn.prepare(&q) {
                            Ok(mut s) => match s.query([]) {
                                Ok(mut rows_iter) => {
                                    let mut vals = Vec::new();
                                    while let Some(r) = rows_iter.next().ok().flatten() {
                                        let v: String = r.get(0).unwrap_or_else(|_| "<err>".to_string());
                                        vals.push(v);
                                    }
                                    vals
                                }
                                Err(e) => {
                                    tracing::warn!("Sample query failed for `{}.{}`: {}", tbl_name, col_name, e);
                                    vec![]
                                }
                            },
                            Err(e) => {
                                tracing::warn!("Sample prepare failed for `{}.{}`: {}", tbl_name, col_name, e);
                                vec![]
                            }
                        }
                    } else {
                        vec![]
                    };

                    // Track FK relationship
                    if let Some(ref rt) = referenced_table {
                        if target_tables.contains(rt) {
                            fk_relationships.push((tbl_name.clone(), col_name.clone(), rt.clone()));
                        }
                    }

                    columns.push(EnrichedColumn {
                        name: col_name,
                        data_type,
                        is_nullable: is_nullable_str == "YES",
                        is_fk_candidate: is_fk,
                        referenced_table,
                        sample_values,
                    });
                }
                drop(col_rows);
                drop(col_stmt);

                // Date range detection: look for date/timestamp columns
                let date_range = if let Some(date_col) = columns.iter().find(|c| {
                    c.name.contains("date") || c.name.contains("_at") || c.name.contains("time")
                }) {
                    let q = format!(
                        "SELECT MIN(\"{}\")::VARCHAR, MAX(\"{}\")::VARCHAR FROM \"{}\";",
                        date_col.name, date_col.name,
                        tbl_name.replace('"', "\"\"")
                    );
                    match conn.query_row(&q, [], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    }) {
                        Ok((min_s, max_s)) => Some((min_s, max_s)),
                        Err(e) => {
                            tracing::warn!("Date range query failed for `{}`: {}", tbl_name, e);
                            None
                        }
                    }
                } else {
                    None
                };

                tables.push(EnrichedTable {
                    table_name: tbl_name.clone(),
                    row_count: count,
                    column_count: columns.len(),
                    columns,
                    date_range,
                });
            }

            Ok(EnrichedSchema {
                total_tables: target_tables.len(),
                tables,
                fk_relationships,
            })
        })
        .await?
    }

    /// Format enriched schema into a concise summary for the LLM system prompt.
    /// Caps to top-20 tables by row count to keep the prompt manageable.
    pub fn format_enriched_schema(schema: &EnrichedSchema) -> String {
        fn fmt_num(n: i64) -> String {
            if n < 0 {
                return "?".to_string();
            }
            if n < 1000 {
                return n.to_string();
            }
            let s = n.to_string();
            let len = s.len();
            let mut out = String::with_capacity(len + len / 3);
            for (i, c) in s.chars().enumerate() {
                if i > 0 && (len - i) % 3 == 0 {
                    out.push(',');
                }
                out.push(c);
            }
            out
        }

        let mut out = String::new();
        out.push_str(&format!("Database Overview: {} tables\n\n", schema.total_tables));

        // Top tables by row count
        let mut sorted: Vec<&EnrichedTable> = schema.tables.iter().collect();
        sorted.sort_by_key(|t| -t.row_count);
        out.push_str("Largest tables by row count:\n");
        for t in sorted.iter().take(15) {
            out.push_str(&format!("  • `{}` — {} rows, {} cols\n", t.table_name, fmt_num(t.row_count), t.column_count));
        }

        // FK relationships (capped)
        if !schema.fk_relationships.is_empty() {
            out.push_str("\nKey foreign key relationships (JOIN paths):\n");
            for (from_t, from_c, to_t) in schema.fk_relationships.iter().take(20) {
                out.push_str(&format!("  • `{}.{}` → `{}`\n", from_t, from_c, to_t));
            }
        }

        // Detailed schemas: top 20 tables by row count
        out.push_str("\n--- Key Table Schemas (top 20 by row count) ---\n");
        let detailed: Vec<&&EnrichedTable> = sorted.iter().filter(|t| t.row_count > 0).take(20).collect();
        for t in &detailed {
            out.push_str(&format!(
                "\nTable `{}` ({} rows, {} columns):\n",
                t.table_name,
                fmt_num(t.row_count),
                t.column_count
            ));
            if let Some((min_d, max_d)) = &t.date_range {
                out.push_str(&format!("  Date range: {} → {}\n", min_d, max_d));
            }
            for col in &t.columns {
                let mut flags = String::new();
                if col.is_fk_candidate {
                    flags.push_str(" [FK");
                    if let Some(ref rt) = col.referenced_table {
                        flags.push_str(&format!("→{}", rt));
                    }
                    flags.push(']');
                }
                if !col.sample_values.is_empty() {
                    flags.push_str(&format!(" e.g.({})", col.sample_values.join(", ")));
                }
                out.push_str(&format!(
                    "  - {}: {}{}{}\n",
                    col.name,
                    col.data_type,
                    if col.is_nullable { " NULL?" } else { "" },
                    flags,
                ));
            }
        }

        out
    }

    /// Generate automated insight cards about the database.
    /// Each card runs a specific SQL query; failures are isolated per-card.
    pub async fn generate_insights(&self) -> InsightsResponse {
        let cards = vec![
            self.insight_card(
                "total_games",
                "Total Games",
                "stats",
                "SELECT COUNT(*) as val FROM game;",
                "All-time NBA games in database",
            )
            .await,
            self.insight_card(
                "total_players",
                "Total Players",
                "stats",
                "SELECT COUNT(*) as val FROM player;",
                "Unique players tracked",
            )
            .await,
            self.insight_card(
                "total_teams",
                "Total Teams",
                "stats",
                "SELECT COUNT(*) as val FROM team;",
                "Franchises and teams",
            )
            .await,
            self.insight_card(
                "season_range",
                "Season Coverage",
                "range",
                "SELECT MIN(season_id) as min_s, MAX(season_id) as max_s FROM game;",
                "NBA seasons in database",
            )
            .await,
            self.insight_card(
                "recent_season",
                "Most Recent Season",
                "range",
                "SELECT MAX(season_id) as val FROM game;",
                "Latest NBA season",
            )
            .await,
            self.insight_card(
                "largest_table",
                "Largest Table",
                "meta",
                "SELECT table_name, estimated_size FROM duckdb_tables() \
                 WHERE schema_name='main' AND estimated_size >= 0 \
                 ORDER BY estimated_size DESC LIMIT 1;",
                "Table with most rows",
            )
            .await,
            self.insight_card(
                "total_tables",
                "Database Tables",
                "meta",
                "SELECT COUNT(*) as val FROM information_schema.tables WHERE table_schema='main';",
                "Total analytical tables",
            )
            .await,
        ];

        let total = cards.len();
        let successful = cards.iter().filter(|c| c.error.is_none()).count();
        let total_tables = self.count_tables().await;
        InsightsResponse {
            cards,
            generated_at: format!(
                "{}",
                std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
            ),
            total_queries: total,
            successful,
            total_tables,
        }
    }

    async fn count_tables(&self) -> usize {
        self.run_sql(
            "SELECT COUNT(*) as val FROM information_schema.tables WHERE table_schema='main';".to_string(),
            Some(1),
        )
        .await
        .ok()
        .and_then(|rows| rows.first()?.get("val")?.as_i64())
        .unwrap_or(0) as usize
    }

    async fn insight_card(&self, id: &str, title: &str, category: &str, query: &str, subtitle: &str) -> InsightCard {
        match self.run_sql(query.to_string(), Some(1)).await {
            Ok(rows) if !rows.is_empty() => {
                let first = &rows[0];
                let value = if let Some(v) = first.get("val") {
                    if let Some(n) = v.as_i64() {
                        Self::format_num_comma(n)
                    } else {
                        v.to_string().trim_matches('"').to_string()
                    }
                } else if let Some(v) = first.get("name") {
                    v.to_string().trim_matches('"').to_string()
                } else if let Some(v) =
                    first.get("total_pts").or_else(|| first.get("total_threes")).or_else(|| first.get("games"))
                {
                    if let Some(n) = v.as_i64() {
                        Self::format_num_comma(n)
                    } else {
                        v.to_string().trim_matches('"').to_string()
                    }
                } else if let Some(v) = first.get("table_name") {
                    format!(
                        "{} ({} rows)",
                        v.to_string().trim_matches('"'),
                        first
                            .get("estimated_size")
                            .map(|e| Self::format_num_comma(e.as_i64().unwrap_or(0)))
                            .unwrap_or_default()
                    )
                } else if let Some(v) = first.get("min_s") {
                    format!(
                        "{} – {}",
                        v.to_string().trim_matches('"'),
                        first.get("max_s").map(|m| m.to_string().trim_matches('"').to_string()).unwrap_or_default()
                    )
                } else {
                    serde_json::to_string(&first).unwrap_or_else(|_| "—".to_string())
                };

                InsightCard {
                    id: id.to_string(),
                    title: title.to_string(),
                    value,
                    subtitle: subtitle.to_string(),
                    category: category.to_string(),
                    error: None,
                }
            }
            Ok(_) => InsightCard {
                id: id.to_string(),
                title: title.to_string(),
                value: "—".to_string(),
                subtitle: subtitle.to_string(),
                category: category.to_string(),
                error: Some("No rows returned".to_string()),
            },
            Err(e) => InsightCard {
                id: id.to_string(),
                title: title.to_string(),
                value: "—".to_string(),
                subtitle: subtitle.to_string(),
                category: category.to_string(),
                error: Some(format!("Query failed: {}", e)),
            },
        }
    }

    /// Format successful insight cards as a brief system prompt snippet.
    pub fn format_insights_for_prompt(insights: &InsightsResponse) -> String {
        let mut out = String::from("Database Insights (pre-computed):\n");
        for card in &insights.cards {
            if card.error.is_none() && card.value != "—" {
                out.push_str(&format!("  • {}: {} ({})\n", card.title, card.value, card.subtitle));
            }
        }
        out
    }

    fn format_num_comma(n: i64) -> String {
        if n < 0 {
            return "?".to_string();
        }
        if n < 1000 {
            return n.to_string();
        }
        let s = n.to_string();
        let len = s.len();
        let mut out = String::with_capacity(len + len / 3);
        for (i, c) in s.chars().enumerate() {
            if i > 0 && (len - i) % 3 == 0 {
                out.push(',');
            }
            out.push(c);
        }
        out
    }

    /// List recent query history entries (most recent first, capped at 50).
    pub fn list_history(&self) -> Vec<DbHistoryEntry> {
        self.history.lock().iter().rev().take(50).cloned().collect()
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

// -- Helper functions for auto_fix_sql --

fn extract_between<'a>(haystack: &'a str, prefix: &str, suffix: &str) -> Option<&'a str> {
    let start = haystack.find(prefix)? + prefix.len();
    let remaining = &haystack[start..];
    let end = remaining.find(suffix)?;
    Some(&remaining[..end])
}

use sqlparser::ast::{Expr, VisitMut, VisitorMut};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;

struct IdentReplacer<'a> {
    old_name: &'a str,
    new_name: &'a str,
}

impl VisitorMut for IdentReplacer<'_> {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Identifier(ident) => {
                if ident.value.eq_ignore_ascii_case(self.old_name) {
                    ident.value = self.new_name.to_string();
                }
            }
            Expr::CompoundIdentifier(idents) => {
                for ident in idents {
                    if ident.value.eq_ignore_ascii_case(self.old_name) {
                        ident.value = self.new_name.to_string();
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn replace_sql_ident_ast(sql: &str, old_name: &str, new_name: &str) -> Option<String> {
    let dialect = DuckDbDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).ok()?;
    let mut replacer = IdentReplacer { old_name, new_name };
    for stmt in &mut statements {
        let _ = stmt.visit(&mut replacer);
    }
    Some(statements.iter().map(|s| s.to_string()).collect::<Vec<_>>().join("; "))
}

/// Replace an SQL identifier (column name) in the query, handling quoting.
/// Uses sqlparser AST parsing first, falling back to heuristic string replacement if parsing fails.
fn replace_sql_ident(sql: &str, old_name: &str, new_name: &str) -> String {
    if let Some(fixed) = replace_sql_ident_ast(sql, old_name, new_name) {
        fixed
    } else {
        replace_sql_ident_heuristic(sql, old_name, new_name)
    }
}

fn replace_sql_ident_heuristic(sql: &str, old_name: &str, new_name: &str) -> String {
    let mut result = String::with_capacity(sql.len() + new_name.len());
    let bytes = sql.as_bytes();
    let old_bytes = old_name.as_bytes();
    let mut i = 0;
    let mut in_single_quote = false;

    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if in_single_quote && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                result.push('\'');
                result.push('\'');
                i += 2;
                continue;
            }
            in_single_quote = !in_single_quote;
            result.push('\'');
            i += 1;
            continue;
        }

        if in_single_quote {
            result.push(bytes[i] as char);
            i += 1;
            continue;
        }

        if i + old_bytes.len() <= bytes.len() && &bytes[i..i + old_bytes.len()] == old_bytes {
            let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_';
            let after_ok = i + old_bytes.len() >= bytes.len()
                || !bytes[i + old_bytes.len()].is_ascii_alphanumeric() && bytes[i + old_bytes.len()] != b'_';

            if before_ok && after_ok {
                result.push_str(new_name);
                i += old_bytes.len();
                continue;
            }
        }
        result.push(bytes[i] as char);
        i += 1;
    }

    result
}
