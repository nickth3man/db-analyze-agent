The project already has a strong foundation: natural-language NBA analysis, DuckDB schema discovery, SQL execution, generated charts, streaming events, session persistence, insight cards, query history, Markdown export, authentication, CORS, and rate limiting.   

## Highest-value features

| Priority | Feature                                 | What it adds                                                                                                                                                                                       |
| -------- | --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1        | **Answer evidence panel**               | Show every factual claim alongside the SQL query, tables used, row count, execution time, and supporting result rows. Users can verify answers instead of trusting generated prose.                |
| 2        | **Interactive result tables**           | Sorting, filtering, column hiding, pagination, copying cells, and downloading the full result as CSV or JSON. The existing 50-row cap could become a preview rather than the final dataset.        |
| 3        | **Real interactive charts**             | Render the existing `generate_chart` output as bar, line, scatter, and pie charts. Add tooltips, axis selection, downloadable PNG/SVG, and “view source data.”                                     |
| 4        | **Player/team comparison mode**         | A structured comparison screen for players, teams, seasons, careers, playoff runs, or selected date ranges. Include absolute totals, per-game values, per-possession values, and percentile ranks. |
| 5        | **Saved analyses**                      | Let users name, favorite, duplicate, search, and reopen conversations or individual queries. Sessions currently exist internally but have no visible management layer.                             |
| 6        | **Analysis templates**                  | One-click prompts for career highs, head-to-head results, streaks, playoff series, clutch performance, shooting splits, draft classes, awards, and team-season summaries.                          |
| 7        | **Schema explorer**                     | Browse all tables and columns visually, inspect samples, see estimated row counts, and follow inferred relationships through an entity graph. Much of the backend data for this already exists.    |
| 8        | **Data coverage and confidence badges** | Display season coverage, missing values, unresolved IDs, table freshness, and whether an answer came from complete or partial data.                                                                |
| 9        | **Follow-up action chips**              | Generate actions such as “Compare to playoffs,” “Break down by season,” “Show top 10,” “Chart this,” or “Explain the query,” rather than only plain-text suggested questions.                      |
| 10       | **Report builder**                      | Allow users to pin answers, tables, and charts into a report, reorder sections, add notes, and export Markdown, HTML, PDF, CSV, or a shareable link.                                               |

## NBA-specific analysis features

### Era-adjusted comparisons

Provide league-relative metrics so comparisons across decades are more meaningful:

* Points relative to league average
* Pace-adjusted statistics
* Efficiency relative to the season
* Percentile within season and position
* Awards and leaderboard placement

### Streak and sequence finder

Support questions such as:

* Longest 30-point streak
* Most consecutive wins after trailing at halftime
* Largest unanswered scoring run
* Most games with ten or more assists
* Best stretch over any 10, 20, or 50 games

This would benefit from a dedicated `find_streaks` tool instead of making the model construct gaps-and-islands SQL every time.

### Head-to-head explorer

Return:

* Overall record
* Regular season versus playoffs
* Home and away splits
* Average margin
* Player performance in those games
* Series-by-series results
* Highest-scoring and closest games

### Record and ranking engine

Create a reusable tool that ranks a result within:

* NBA history
* A franchise
* A season
* A playoff round
* A player’s career
* A selected date range

This lets the interface confidently say “third-most in franchise history” and show the surrounding entries.

### Game reconstruction

Use play-by-play and line-score data to produce:

* Lead changes
* Ties
* Largest lead
* Scoring runs
* Win-probability timeline
* Clutch possessions
* Final-minute sequence
* Key turning points

### Player profile generator

Automatically build a profile containing:

* Career summary
* Best seasons
* Career highs
* Playoff performance
* Team history
* Awards
* Shooting zones and splits
* Similar players
* Notable games

## Agent improvements that enable better features

### Dedicated analytical tools

The current agent mainly exposes general schema and SQL tools.  Add higher-level tools such as:

```text
compare_players
compare_teams
find_leaders
find_streaks
get_game_summary
get_player_profile
get_head_to_head
rank_performance
check_data_coverage
export_query_result
```

These make results more consistent and reduce repeated schema exploration.

### Query repair with user-visible diagnostics

The existing column-name auto-correction is useful.  Expand it to handle:

* Missing tables
* Ambiguous columns
* Type mismatches
* Incorrect joins
* Wrong date formats
* Unsupported functions

Show the original SQL, corrected SQL, and correction reason.

### Semantic metrics layer

Define canonical metrics in configuration:

```text
points_per_game
true_shooting_percentage
usage_percentage
net_rating
pace
win_percentage
clutch_minutes
playoff_game
```

The model would select defined metrics instead of rewriting formulas differently between conversations.

### Model fallback and retry

Add:

* Configurable model selection
* Fallback model
* Retry with exponential backoff
* Request timeout
* Token and cost tracking
* Maximum SQL execution time
* User cancellation

### Feedback and correction workflow

Each answer could support:

* Helpful/not helpful
* Incorrect statistic
* Wrong interpretation
* Missing context
* Retry analysis
* Edit and rerun SQL

Store the associated question, generated SQL, result, model, and feedback for evaluation.

## Important fixes before expanding

1. **Persist final assistant answers.** When a completion contains no tool calls, the final assistant message is placed in the trace but not appended to the stored message history. This can weaken follow-up context and cause exported sessions to omit final responses. 

2. **Make streaming genuinely live.** `ToolCallStarted` is currently emitted after `execute_tool(...).await` finishes, so the frontend does not receive the “started” event until the operation is already complete. 

3. **Correct query statistics.** `/api/stats` reports the length of the history list as `total_queries`, but history is capped and currently records only successful, uncached SQL executions. It is not a true lifetime query count.  

4. **Record failed queries and cache hits.** Add status, error category, cache-hit flag, model, session ID, tool name, and timestamps to the history.

5. **Replace string-based SQL validation with AST validation.** The current checks can mistake keywords inside comments or string literals for destructive SQL and do not provide a strict allowlist of safe statement types. 

6. **Make deployment configurable.** Move the database path, bind address, port, session path, model, row cap, iteration limit, and cache settings into environment variables or a configuration file. The database path and port are currently hard-coded. 

The strongest first release would combine **evidence-backed answers, interactive tables/charts, saved analyses, player/team comparison, and data-confidence indicators**.
