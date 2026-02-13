//! SQL Rewriter for Snowflake-specific syntax
//!
//! This module rewrites Snowflake-specific SQL constructs to DataFusion-compatible SQL.
//!
//! ## Supported Rewrites
//!
//! ### LATERAL FLATTEN
//! Converts `LATERAL FLATTEN(input => expr)` to a CROSS JOIN with _NUMBERS table:
//!
//! ```sql
//! -- Original
//! SELECT t.id, f.value
//! FROM table t, LATERAL FLATTEN(input => t.json_array) f;
//!
//! -- Rewritten
//! SELECT t.id, FLATTEN_ARRAY(t.json_array, _flatten_idx.idx) AS value
//! FROM table t
//! CROSS JOIN _numbers AS _flatten_idx
//! WHERE _flatten_idx.idx < ARRAY_SIZE(t.json_array);
//! ```

use regex::Regex;

/// Rewrite Snowflake-specific SQL to DataFusion-compatible SQL
pub fn rewrite(sql: &str) -> String {
    let mut result = sql.to_string();

    // Rewrite function names for DataFusion compatibility
    result = rewrite_function_names(&result);

    // Rewrite QUALIFY clause
    result = rewrite_qualify(&result);

    // Rewrite LATERAL FLATTEN
    result = rewrite_lateral_flatten(&result);

    // Rewrite PIVOT
    result = rewrite_pivot(&result);

    // Rewrite UNPIVOT
    result = rewrite_unpivot(&result);

    // Rewrite SAMPLE/TABLESAMPLE
    result = rewrite_sample(&result);

    // Rewrite RATIO_TO_REPORT
    result = rewrite_ratio_to_report(&result);

    result
}

/// Rewrite Snowflake function names to DataFusion equivalents
fn rewrite_function_names(sql: &str) -> String {
    let mut result = sql.to_string();

    // CURRENT_TIMESTAMP() -> now() (with parentheses first)
    let current_timestamp_parens = Regex::new(r"(?i)\bCURRENT_TIMESTAMP\s*\(\s*\)").unwrap();
    result = current_timestamp_parens
        .replace_all(&result, "now()")
        .to_string();

    // CURRENT_TIMESTAMP -> now() (without parentheses)
    // Replace remaining CURRENT_TIMESTAMP that weren't part of CURRENT_TIMESTAMP()
    let current_timestamp_pattern = Regex::new(r"(?i)\bCURRENT_TIMESTAMP\b").unwrap();
    result = current_timestamp_pattern
        .replace_all(&result, "now()")
        .to_string();

    // TRUNCATE(x, y) -> trunc(x, y) - for numeric truncation
    let truncate_pattern = Regex::new(r"(?i)\bTRUNCATE\s*\(").unwrap();
    result = truncate_pattern.replace_all(&result, "trunc(").to_string();

    // CEILING(x) -> ceil(x)
    let ceiling_pattern = Regex::new(r"(?i)\bCEILING\s*\(").unwrap();
    result = ceiling_pattern.replace_all(&result, "ceil(").to_string();

    // POW(x, y) -> power(x, y)
    let pow_pattern = Regex::new(r"(?i)\bPOW\s*\(").unwrap();
    result = pow_pattern.replace_all(&result, "power(").to_string();

    // LEN(x) -> length(x)
    let len_pattern = Regex::new(r"(?i)\bLEN\s*\(").unwrap();
    result = len_pattern.replace_all(&result, "length(").to_string();

    // NVL(x, y) is already implemented as UDF, but IFNULL is an alias
    let ifnull_pattern = Regex::new(r"(?i)\bIFNULL\s*\(").unwrap();
    result = ifnull_pattern.replace_all(&result, "NVL(").to_string();

    // ZEROIFNULL(x) -> COALESCE(x, 0)
    let zeroifnull_pattern = Regex::new(r"(?i)\bZEROIFNULL\s*\(([^)]+)\)").unwrap();
    result = zeroifnull_pattern
        .replace_all(&result, "COALESCE($1, 0)")
        .to_string();

    // NULLIFZERO(x) -> NULLIF(x, 0)
    let nullifzero_pattern = Regex::new(r"(?i)\bNULLIFZERO\s*\(([^)]+)\)").unwrap();
    result = nullifzero_pattern
        .replace_all(&result, "NULLIF($1, 0)")
        .to_string();

    // POSITION(substr IN str) -> charindex(substr, str)
    let position_pattern =
        Regex::new(r"(?i)\bPOSITION\s*\(\s*([^)]+?)\s+IN\s+([^)]+?)\s*\)").unwrap();
    result = position_pattern
        .replace_all(&result, "charindex($1, $2)")
        .to_string();

    // Bracket notation: col['key'] or col[0] -> get(col, 'key') or get(col, 0)
    // Match identifier followed by ['string'] or [number]
    let bracket_string_pattern = Regex::new(r"(\w+)\['([^']+)'\]").unwrap();
    result = bracket_string_pattern
        .replace_all(&result, "get($1, '$2')")
        .to_string();

    let bracket_number_pattern = Regex::new(r"(\w+)\[(\d+)\]").unwrap();
    result = bracket_number_pattern
        .replace_all(&result, "get($1, $2)")
        .to_string();

    // CURRENT_USER() -> current_user (DataFusion treats this as a keyword without parentheses)
    let current_user_pattern = Regex::new(r"(?i)\bCURRENT_USER\s*\(\s*\)").unwrap();
    result = current_user_pattern
        .replace_all(&result, "current_user")
        .to_string();

    // CURRENT_DATABASE() -> current_database (DataFusion treats this as a keyword without parentheses)
    let current_database_pattern = Regex::new(r"(?i)\bCURRENT_DATABASE\s*\(\s*\)").unwrap();
    result = current_database_pattern
        .replace_all(&result, "current_database")
        .to_string();

    // CURRENT_SCHEMA() -> current_schema (DataFusion treats this as a keyword without parentheses)
    let current_schema_pattern = Regex::new(r"(?i)\bCURRENT_SCHEMA\s*\(\s*\)").unwrap();
    result = current_schema_pattern
        .replace_all(&result, "current_schema")
        .to_string();

    result
}

/// Rewrite QUALIFY clause to CTE + WHERE
///
/// Pattern: `SELECT ... FROM ... QUALIFY condition`
/// Becomes: `WITH _qualify AS (SELECT ... FROM ...) SELECT * FROM _qualify WHERE condition`
///
/// Example:
/// ```sql
/// SELECT id, name, ROW_NUMBER() OVER (ORDER BY id) as rn FROM t QUALIFY rn = 1
/// ```
/// Becomes:
/// ```sql
/// WITH _qualify AS (SELECT id, name, ROW_NUMBER() OVER (ORDER BY id) as rn FROM t) SELECT * FROM _qualify WHERE rn = 1
/// ```
fn rewrite_qualify(sql: &str) -> String {
    // Pattern to match: ... QUALIFY condition [ORDER BY ...] [LIMIT ...]
    // QUALIFY must come after FROM/WHERE/GROUP BY/HAVING but before ORDER BY/LIMIT
    let qualify_pattern = Regex::new(
        r"(?is)(SELECT\s+.+?FROM\s+.+?)\s+QUALIFY\s+(.+?)(?:\s+(ORDER\s+BY.+?|LIMIT.+?))?$",
    )
    .unwrap();

    if let Some(captures) = qualify_pattern.captures(sql) {
        let select_part = captures.get(1).map_or("", |m| m.as_str()).trim();
        let qualify_condition = captures.get(2).map_or("", |m| m.as_str()).trim();
        let trailing_clauses = captures.get(3).map_or("", |m| m.as_str()).trim();

        // Build the CTE query
        let result = format!(
            "WITH _qualify AS ({select_part}) SELECT * FROM _qualify WHERE {qualify_condition}"
        );

        // Append ORDER BY/LIMIT if present
        if !trailing_clauses.is_empty() {
            return format!("{result} {trailing_clauses}");
        }
        return result;
    }

    sql.to_string()
}

/// FLATTEN options parsed from SQL
#[derive(Debug, Default)]
struct FlattenOptions {
    input: String,
    path: Option<String>,
    outer: bool,
    recursive: bool,
    mode: String, // ARRAY, OBJECT, or BOTH
}

impl FlattenOptions {
    fn parse(args_str: &str) -> Self {
        let mut opts = FlattenOptions {
            mode: "BOTH".to_string(),
            ..Default::default()
        };

        // Parse comma-separated key => value pairs
        // Handle nested parentheses by tracking depth
        let mut current_key = String::new();
        let mut current_value = String::new();
        let mut in_value = false;
        let mut paren_depth = 0;

        for c in args_str.chars() {
            match c {
                '(' => {
                    paren_depth += 1;
                    if in_value {
                        current_value.push(c);
                    }
                }
                ')' => {
                    if paren_depth > 0 {
                        paren_depth -= 1;
                        if in_value {
                            current_value.push(c);
                        }
                    }
                }
                '=' if paren_depth == 0 => {
                    // Skip the '>' after '='
                    in_value = true;
                }
                '>' if in_value && current_value.is_empty() => {
                    // Skip the '>' that comes after '='
                }
                ',' if paren_depth == 0 => {
                    // End of current key-value pair
                    opts.set_option(current_key.trim(), current_value.trim());
                    current_key.clear();
                    current_value.clear();
                    in_value = false;
                }
                _ => {
                    if in_value {
                        current_value.push(c);
                    } else {
                        current_key.push(c);
                    }
                }
            }
        }

        // Handle last key-value pair
        if !current_key.is_empty() {
            opts.set_option(current_key.trim(), current_value.trim());
        }

        opts
    }

    fn set_option(&mut self, key: &str, value: &str) {
        let key_lower = key.to_lowercase();
        let value_trimmed = value.trim();

        match key_lower.as_str() {
            "input" => {
                self.input = value_trimmed.to_string();
            }
            "path" => {
                // Remove quotes from path
                let path = value_trimmed
                    .trim_start_matches('\'')
                    .trim_end_matches('\'')
                    .to_string();
                if !path.is_empty() {
                    self.path = Some(path);
                }
            }
            "outer" => {
                self.outer = value_trimmed.eq_ignore_ascii_case("true");
            }
            "recursive" => {
                self.recursive = value_trimmed.eq_ignore_ascii_case("true");
            }
            "mode" => {
                let mode = value_trimmed
                    .trim_start_matches('\'')
                    .trim_end_matches('\'')
                    .to_uppercase();
                if ["ARRAY", "OBJECT", "BOTH"].contains(&mode.as_str()) {
                    self.mode = mode;
                }
            }
            _ => {}
        }
    }

    /// Get the effective input expression (applying path if specified)
    fn effective_input(&self) -> String {
        if let Some(ref path) = self.path {
            format!("GET_PATH({}, '{}')", self.input, path)
        } else {
            self.input.clone()
        }
    }
}

/// Rewrite LATERAL FLATTEN to CROSS JOIN with _NUMBERS
///
/// Pattern: `LATERAL FLATTEN(input => expr [, path => 'path'] [, outer => true]) [AS alias]`
/// Becomes: `CROSS JOIN _numbers AS _flatten_idx` with WHERE clause
///
/// Supported options:
/// - input: Required. The array or object to flatten.
/// - path: Optional. JSON path to navigate before flattening.
/// - outer: Optional. If TRUE, return one row even for empty input (default: FALSE).
/// - mode: Optional. ARRAY, OBJECT, or BOTH (default: BOTH).
fn rewrite_lateral_flatten(sql: &str) -> String {
    // Pattern to match: , LATERAL FLATTEN(...) [AS] alias
    // Capture the entire argument list inside parentheses
    let lateral_flatten_pattern =
        Regex::new(r"(?i),\s*LATERAL\s+FLATTEN\s*\(([^)]+(?:\([^)]*\)[^)]*)*)\)\s*(?:AS\s+)?(\w+)")
            .unwrap();

    if !lateral_flatten_pattern.is_match(sql) {
        return sql.to_string();
    }

    let mut result = sql.to_string();
    let mut flatten_count = 0;

    // Find all LATERAL FLATTEN matches
    let matches: Vec<_> = lateral_flatten_pattern
        .captures_iter(sql)
        .map(|cap| {
            let full_match = cap.get(0).unwrap();
            let args_str = cap.get(1).unwrap().as_str();
            let alias = cap.get(2).unwrap().as_str();
            let opts = FlattenOptions::parse(args_str);
            (
                full_match.start(),
                full_match.end(),
                opts,
                alias.to_string(),
            )
        })
        .collect();

    // Process matches in reverse order to preserve positions
    for (start, end, opts, alias) in matches.into_iter().rev() {
        flatten_count += 1;
        let idx_alias = format!("_flatten_idx_{flatten_count}");
        let effective_input = opts.effective_input();

        // Replace LATERAL FLATTEN with CROSS JOIN (or LEFT JOIN for outer)
        let join_type = if opts.outer {
            "LEFT JOIN"
        } else {
            "CROSS JOIN"
        };
        let replacement = format!(" {join_type} _numbers AS {idx_alias}");
        result.replace_range(start..end, &replacement);

        // Add WHERE/ON clause for array bounds
        result = add_flatten_where_clause(&result, &effective_input, &idx_alias, opts.outer);

        // Replace alias.value references with FLATTEN_ARRAY calls
        result = replace_flatten_column_refs(&result, &alias, &effective_input, &idx_alias);
    }

    result
}

/// Add WHERE clause for FLATTEN array bounds
fn add_flatten_where_clause(sql: &str, input_expr: &str, idx_alias: &str, outer: bool) -> String {
    // For outer joins, we need a condition that allows at least one row for empty arrays
    // For regular joins, we just need idx < ARRAY_SIZE
    let condition = if outer {
        // For OUTER: allow idx = 0 even for empty arrays, or idx < array_size for non-empty
        format!("({idx_alias}.idx = 0 OR {idx_alias}.idx < COALESCE(ARRAY_SIZE({input_expr}), 0))")
    } else {
        format!("{idx_alias}.idx < ARRAY_SIZE({input_expr})")
    };

    // Check if WHERE clause already exists
    let where_pattern = Regex::new(r"(?i)\bWHERE\b").unwrap();

    if where_pattern.is_match(sql) {
        // Add condition to existing WHERE clause
        let result = where_pattern.replace(sql, &format!("WHERE {condition} AND "));
        result.to_string()
    } else {
        // Find position to insert WHERE clause (before GROUP BY, ORDER BY, LIMIT, or end)
        let insert_pattern = Regex::new(r"(?i)\s*(GROUP\s+BY|ORDER\s+BY|LIMIT|HAVING|$)").unwrap();

        if let Some(mat) = insert_pattern.find(sql) {
            let pos = mat.start();
            let mut result = sql.to_string();
            result.insert_str(pos, &format!(" WHERE {condition} "));
            result
        } else {
            format!("{sql} WHERE {condition}")
        }
    }
}

/// Replace FLATTEN output column references
///
/// Snowflake FLATTEN outputs: SEQ, KEY, PATH, INDEX, VALUE, THIS
/// We support: VALUE, INDEX
fn replace_flatten_column_refs(
    sql: &str,
    alias: &str,
    input_expr: &str,
    idx_alias: &str,
) -> String {
    let mut result = sql.to_string();

    // Replace alias.value with FLATTEN_ARRAY(input_expr, idx)
    let value_pattern = Regex::new(&format!(r"(?i)\b{}\.(value)\b", regex::escape(alias))).unwrap();
    result = value_pattern
        .replace_all(
            &result,
            format!("FLATTEN_ARRAY({input_expr}, {idx_alias}.idx)"),
        )
        .to_string();

    // Replace alias.index with idx
    let index_pattern = Regex::new(&format!(r"(?i)\b{}\.(index)\b", regex::escape(alias))).unwrap();
    result = index_pattern
        .replace_all(&result, format!("{idx_alias}.idx"))
        .to_string();

    // Replace alias.this with the original input expression
    let this_pattern = Regex::new(&format!(r"(?i)\b{}\.(this)\b", regex::escape(alias))).unwrap();
    result = this_pattern.replace_all(&result, input_expr).to_string();

    result
}

/// Rewrite PIVOT clause to CASE WHEN expressions
///
/// Pattern:
/// ```sql
/// SELECT ... FROM table
/// PIVOT (agg_func(value_col) FOR pivot_col IN ('val1', 'val2', ...))
/// [AS alias]
/// ```
///
/// Becomes:
/// ```sql
/// SELECT ..., agg_func(CASE WHEN pivot_col = 'val1' THEN value_col END) AS "val1", ...
/// FROM table
/// GROUP BY non_pivoted_columns
/// ```
fn rewrite_pivot(sql: &str) -> String {
    // Pattern to match PIVOT clause:
    // PIVOT (AGG(col) FOR column IN (val1, val2, ...)) [AS alias]
    let pivot_pattern = Regex::new(
        r"(?is)\bPIVOT\s*\(\s*(\w+)\s*\(\s*(\w+)\s*\)\s+FOR\s+(\w+)\s+IN\s*\(([^)]+)\)\s*\)\s*(?:AS\s+(\w+))?"
    ).unwrap();

    if !pivot_pattern.is_match(sql) {
        return sql.to_string();
    }

    let captures = pivot_pattern.captures(sql).unwrap();
    let agg_func = captures.get(1).unwrap().as_str();
    let value_col = captures.get(2).unwrap().as_str();
    let pivot_col = captures.get(3).unwrap().as_str();
    let values_str = captures.get(4).unwrap().as_str();
    let _alias = captures.get(5).map(|m| m.as_str());

    // Parse the IN values
    let values: Vec<String> = values_str
        .split(',')
        .map(|v| v.trim().trim_matches('\'').trim_matches('"').to_string())
        .collect();

    // Build the CASE WHEN expressions for each pivot value
    let case_expressions: Vec<String> = values
        .iter()
        .map(|val| {
            format!("{agg_func}(CASE WHEN {pivot_col} = '{val}' THEN {value_col} END) AS \"{val}\"")
        })
        .collect();

    // Replace the PIVOT clause with the generated expressions
    // We need to add the expressions to the SELECT clause and add GROUP BY
    let mut result = sql.to_string();

    // Find the SELECT clause and add the generated columns
    let select_pattern = Regex::new(r"(?is)SELECT\s+(.+?)\s+FROM").unwrap();
    if let Some(sel_cap) = select_pattern.captures(&result) {
        let select_cols = sel_cap.get(1).unwrap().as_str();

        // Check if it's SELECT * - we can't expand * easily, so we'll just append
        let new_select = if select_cols.trim() == "*" {
            // For SELECT *, we can't easily determine non-pivot columns
            // Just include the pivot expressions
            case_expressions.join(", ")
        } else {
            format!("{}, {}", select_cols, case_expressions.join(", "))
        };

        let select_replacement = format!("SELECT {new_select} FROM");
        result = select_pattern
            .replace(&result, select_replacement)
            .to_string();
    }

    // Remove the PIVOT clause
    result = pivot_pattern.replace(&result, "").to_string();

    // Add GROUP BY if not already present (for aggregation)
    // Find non-aggregated columns from SELECT clause to add to GROUP BY
    let group_by_pattern = Regex::new(r"(?i)\bGROUP\s+BY\b").unwrap();
    if !group_by_pattern.is_match(&result) {
        // Extract columns that aren't part of the aggregate (simplified approach)
        // For now, we'll look for columns before FROM that aren't aggregates
        let pre_select = Regex::new(r"(?is)SELECT\s+(.+?)\s+FROM").unwrap();
        if let Some(cap) = pre_select.captures(&result) {
            let cols_str = cap.get(1).unwrap().as_str();
            let group_cols: Vec<&str> = cols_str
                .split(',')
                .filter(|c| {
                    let c_upper = c.to_uppercase();
                    !c_upper.contains("SUM(")
                        && !c_upper.contains("COUNT(")
                        && !c_upper.contains("AVG(")
                        && !c_upper.contains("MAX(")
                        && !c_upper.contains("MIN(")
                        && !c_upper.contains("CASE WHEN")
                })
                .map(|c| c.trim())
                .filter(|c| !c.is_empty())
                .collect();

            if !group_cols.is_empty() {
                // Find where to insert GROUP BY (before ORDER BY, LIMIT, or end)
                let insert_pattern = Regex::new(r"(?i)\s*(ORDER\s+BY|LIMIT|$)").unwrap();
                if let Some(mat) = insert_pattern.find(&result) {
                    let pos = mat.start();
                    result.insert_str(pos, &format!(" GROUP BY {} ", group_cols.join(", ")));
                }
            }
        }
    }

    result
}

/// Rewrite UNPIVOT clause to UNION ALL
///
/// Pattern:
/// ```sql
/// SELECT ... FROM table
/// UNPIVOT (value_col FOR name_col IN (col1, col2, ...))
/// [AS alias]
/// ```
///
/// Becomes:
/// ```sql
/// SELECT ..., 'col1' AS name_col, col1 AS value_col FROM table
/// UNION ALL
/// SELECT ..., 'col2' AS name_col, col2 AS value_col FROM table
/// ...
/// ```
fn rewrite_unpivot(sql: &str) -> String {
    // Pattern to match UNPIVOT clause:
    // UNPIVOT (value_col FOR name_col IN (col1, col2, ...)) [AS alias]
    let unpivot_pattern = Regex::new(
        r"(?is)\bUNPIVOT\s*\(\s*(\w+)\s+FOR\s+(\w+)\s+IN\s*\(([^)]+)\)\s*\)\s*(?:AS\s+(\w+))?",
    )
    .unwrap();

    if !unpivot_pattern.is_match(sql) {
        return sql.to_string();
    }

    let captures = unpivot_pattern.captures(sql).unwrap();
    let value_col = captures.get(1).unwrap().as_str();
    let name_col = captures.get(2).unwrap().as_str();
    let columns_str = captures.get(3).unwrap().as_str();
    let _alias = captures.get(4).map(|m| m.as_str());

    // Parse the IN columns
    let columns: Vec<String> = columns_str
        .split(',')
        .map(|c| c.trim().to_string())
        .collect();

    // Get the base SELECT ... FROM table part
    let base_pattern = Regex::new(r"(?is)(SELECT\s+.+?\s+FROM\s+\w+)").unwrap();
    let base_match = base_pattern.captures(sql);

    if base_match.is_none() {
        return sql.to_string();
    }

    let base = base_match.unwrap().get(1).unwrap().as_str();

    // Extract the table name
    let table_pattern = Regex::new(r"(?i)FROM\s+(\w+)").unwrap();
    let table_name = table_pattern
        .captures(base)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str())
        .unwrap_or("t");

    // Extract SELECT columns (excluding the pivot columns)
    let select_pattern = Regex::new(r"(?is)SELECT\s+(.+?)\s+FROM").unwrap();
    let select_cols = select_pattern
        .captures(base)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().trim())
        .unwrap_or("*");

    // Filter out the unpivot columns from select if SELECT *
    let filtered_select = if select_cols == "*" {
        "*".to_string()
    } else {
        let cols: Vec<&str> = select_cols
            .split(',')
            .map(|c| c.trim())
            .filter(|c| !columns.contains(&c.to_string()))
            .collect();
        cols.join(", ")
    };

    // Build UNION ALL queries for each column
    let union_queries: Vec<String> = columns
        .iter()
        .map(|col| {
            if filtered_select.is_empty() || filtered_select == "*" {
                format!(
                    "SELECT '{col}' AS {name_col}, {col} AS {value_col} FROM {table_name}"
                )
            } else {
                format!(
                    "SELECT {filtered_select}, '{col}' AS {name_col}, {col} AS {value_col} FROM {table_name}"
                )
            }
        })
        .collect();

    // Get any trailing clauses (WHERE, ORDER BY, etc.)
    // Use the same pattern as unpivot_pattern to correctly find the end
    let trailing_pattern = Regex::new(
        r"(?is)\bUNPIVOT\s*\(\s*\w+\s+FOR\s+\w+\s+IN\s*\([^)]+\)\s*\)\s*(?:AS\s+\w+)?\s*(.*)$",
    )
    .unwrap();
    let trailing = trailing_pattern
        .captures(sql)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().trim())
        .unwrap_or("");

    // Combine with UNION ALL
    let mut result = union_queries.join(" UNION ALL ");

    // Add trailing clauses if any
    if !trailing.is_empty() {
        result = format!("{result} {trailing}");
    }

    result
}

/// Rewrite SAMPLE/TABLESAMPLE clause to subquery with ORDER BY RANDOM() and LIMIT
///
/// Patterns:
/// ```sql
/// SELECT * FROM table SAMPLE (10)        -- 10% of rows
/// SELECT * FROM table SAMPLE ROW (100)   -- 100 rows
/// SELECT * FROM table TABLESAMPLE (50)   -- 50% of rows
/// ```
///
/// Note: Percentage-based sampling is approximated using COUNT(*) and calculated LIMIT.
/// For row-based sampling, we use ORDER BY RANDOM() LIMIT n.
fn rewrite_sample(sql: &str) -> String {
    // Pattern 1: SAMPLE ROW (n) - sample n rows
    let sample_row_pattern = Regex::new(r"(?i)\bSAMPLE\s+ROW\s*\(\s*(\d+)\s*\)").unwrap();

    // Pattern 2: SAMPLE (n) or TABLESAMPLE (n) - sample n% of rows
    let sample_pct_pattern =
        Regex::new(r"(?i)\b(?:SAMPLE|TABLESAMPLE)\s*\(\s*(\d+(?:\.\d+)?)\s*\)").unwrap();

    // Handle SAMPLE ROW (n) - exact row count
    if let Some(captures) = sample_row_pattern.captures(sql) {
        let row_count = captures.get(1).unwrap().as_str();

        // Replace SAMPLE ROW (n) with ORDER BY RANDOM() LIMIT n
        let mut result = sql.to_string();
        result = sample_row_pattern.replace(&result, "").to_string();

        // Check if ORDER BY already exists
        let order_by_pattern = Regex::new(r"(?i)\bORDER\s+BY\b").unwrap();
        let limit_pattern = Regex::new(r"(?i)\bLIMIT\b").unwrap();

        if !order_by_pattern.is_match(&result) {
            // Add ORDER BY RANDOM() and LIMIT
            if !limit_pattern.is_match(&result) {
                result = format!("{} ORDER BY RANDOM() LIMIT {}", result.trim(), row_count);
            }
        } else {
            // ORDER BY exists, just add LIMIT if not present
            if !limit_pattern.is_match(&result) {
                result = format!("{} LIMIT {}", result.trim(), row_count);
            }
        }

        return result;
    }

    // Handle SAMPLE (n%) or TABLESAMPLE (n%) - percentage sampling
    // This is trickier because we need to know total row count.
    // We'll convert to a CTE that calculates the limit.
    if let Some(captures) = sample_pct_pattern.captures(sql) {
        let percentage: f64 = captures.get(1).unwrap().as_str().parse().unwrap_or(0.0);

        if percentage <= 0.0 || percentage >= 100.0 {
            return sql.to_string();
        }

        // For percentage sampling, we use a statistical approach:
        // ORDER BY RANDOM() LIMIT (SELECT CAST(COUNT(*) * pct / 100 AS INT) FROM table)
        // But DataFusion doesn't support subqueries in LIMIT directly.
        // Instead, we'll estimate using random filtering: WHERE RANDOM() < percentage/100

        let mut result = sql.to_string();
        result = sample_pct_pattern.replace(&result, "").to_string();

        // Add WHERE clause for random filtering
        let pct_decimal = percentage / 100.0;
        let where_condition = format!("RANDOM() < {pct_decimal}");

        let where_pattern = Regex::new(r"(?i)\bWHERE\b").unwrap();
        if where_pattern.is_match(&result) {
            // Add to existing WHERE
            result = where_pattern
                .replace(&result, &format!("WHERE {where_condition} AND "))
                .to_string();
        } else {
            // Find where to insert WHERE (after FROM table, before ORDER BY/LIMIT/etc)
            let insert_pattern =
                Regex::new(r"(?i)(FROM\s+\w+(?:\s+\w+)?)\s*(ORDER\s+BY|GROUP\s+BY|LIMIT|HAVING|$)")
                    .unwrap();
            if let Some(cap) = insert_pattern.captures(&result) {
                let from_part = cap.get(1).unwrap().as_str();
                let trailing = cap.get(2).map(|m| m.as_str()).unwrap_or("");
                result = insert_pattern
                    .replace(
                        &result,
                        &format!("{from_part} WHERE {where_condition} {trailing}"),
                    )
                    .to_string();
            }
        }

        return result;
    }

    sql.to_string()
}

/// Rewrite RATIO_TO_REPORT(expr) OVER (...) to expr / SUM(expr) OVER (...)
///
/// Snowflake's RATIO_TO_REPORT calculates the ratio of a value to the sum of all values
/// in the partition.
fn rewrite_ratio_to_report(sql: &str) -> String {
    // Pattern: RATIO_TO_REPORT(expr) OVER (partition_clause)
    // Replace with: (expr) / SUM(expr) OVER (partition_clause)
    let pattern =
        Regex::new(r"(?i)\bRATIO_TO_REPORT\s*\(\s*([^)]+)\s*\)\s*OVER\s*\(([^)]*)\)").unwrap();

    pattern
        .replace_all(sql, |caps: &regex::Captures| {
            let expr = caps[1].trim();
            let over_clause = caps[2].trim();
            format!("(({expr}) * 1.0 / SUM({expr}) OVER ({over_clause}))")
        })
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_lateral_flatten() {
        let sql = "SELECT * FROM users";
        assert_eq!(rewrite(sql), sql);
    }

    #[test]
    fn test_simple_lateral_flatten() {
        let sql = "SELECT t.id, f.value FROM table1 t, LATERAL FLATTEN(input => t.json_array) AS f";
        let rewritten = rewrite(sql);

        assert!(rewritten.contains("CROSS JOIN _numbers"));
        assert!(rewritten.contains("FLATTEN_ARRAY(t.json_array"));
        assert!(rewritten.contains("ARRAY_SIZE(t.json_array)"));
    }

    #[test]
    fn test_lateral_flatten_without_as() {
        let sql = "SELECT t.id, f.value FROM table1 t, LATERAL FLATTEN(input => t.data) f";
        let rewritten = rewrite(sql);

        assert!(rewritten.contains("CROSS JOIN _numbers"));
        assert!(rewritten.contains("FLATTEN_ARRAY(t.data"));
    }

    #[test]
    fn test_lateral_flatten_with_where() {
        let sql =
            "SELECT t.id, f.value FROM table1 t, LATERAL FLATTEN(input => t.arr) f WHERE t.id > 0";
        let rewritten = rewrite(sql);

        // Should add condition to existing WHERE
        assert!(rewritten.contains("WHERE"));
        assert!(rewritten.contains("ARRAY_SIZE(t.arr)"));
        assert!(rewritten.contains("t.id > 0"));
    }

    #[test]
    fn test_flatten_index_reference() {
        let sql = "SELECT f.index, f.value FROM table1 t, LATERAL FLATTEN(input => t.arr) f";
        let rewritten = rewrite(sql);

        // f.index should be replaced with idx alias
        assert!(rewritten.contains(".idx"));
        assert!(rewritten.contains("FLATTEN_ARRAY"));
    }

    #[test]
    fn test_replace_flatten_column_refs() {
        let sql = "SELECT f.value, f.index, f.this FROM test";
        let result = replace_flatten_column_refs(sql, "f", "t.data", "_flatten_idx_1");

        assert!(result.contains("FLATTEN_ARRAY(t.data, _flatten_idx_1.idx)"));
        assert!(result.contains("_flatten_idx_1.idx"));
        assert!(result.contains("t.data")); // f.this replaced with input_expr
    }

    #[test]
    fn test_current_timestamp_rewrite() {
        let sql = "SELECT CURRENT_TIMESTAMP";
        let rewritten = rewrite(sql);
        assert_eq!(rewritten, "SELECT now()");

        let sql_parens = "SELECT CURRENT_TIMESTAMP()";
        let rewritten_parens = rewrite(sql_parens);
        assert_eq!(rewritten_parens, "SELECT now()");
    }

    #[test]
    fn test_truncate_rewrite() {
        let sql = "SELECT TRUNCATE(3.14159, 2)";
        let rewritten = rewrite(sql);
        assert_eq!(rewritten, "SELECT trunc(3.14159, 2)");
    }

    #[test]
    fn test_ceiling_rewrite() {
        let sql = "SELECT CEILING(3.14)";
        let rewritten = rewrite(sql);
        assert_eq!(rewritten, "SELECT ceil(3.14)");
    }

    #[test]
    fn test_pow_rewrite() {
        let sql = "SELECT POW(2, 8)";
        let rewritten = rewrite(sql);
        assert_eq!(rewritten, "SELECT power(2, 8)");
    }

    #[test]
    fn test_len_rewrite() {
        let sql = "SELECT LEN('hello')";
        let rewritten = rewrite(sql);
        assert_eq!(rewritten, "SELECT length('hello')");
    }

    #[test]
    fn test_ifnull_rewrite() {
        let sql = "SELECT IFNULL(col, 0) FROM t";
        let rewritten = rewrite(sql);
        assert_eq!(rewritten, "SELECT NVL(col, 0) FROM t");
    }

    #[test]
    fn test_zeroifnull_rewrite() {
        let sql = "SELECT ZEROIFNULL(col) FROM t";
        let rewritten = rewrite(sql);
        assert_eq!(rewritten, "SELECT COALESCE(col, 0) FROM t");
    }

    #[test]
    fn test_nullifzero_rewrite() {
        let sql = "SELECT NULLIFZERO(col) FROM t";
        let rewritten = rewrite(sql);
        assert_eq!(rewritten, "SELECT NULLIF(col, 0) FROM t");
    }

    #[test]
    fn test_position_rewrite() {
        let sql = "SELECT POSITION('bar' IN 'foobar') FROM t";
        let rewritten = rewrite(sql);
        assert_eq!(rewritten, "SELECT charindex('bar', 'foobar') FROM t");
    }

    #[test]
    fn test_qualify_basic() {
        let sql = "SELECT id, ROW_NUMBER() OVER (ORDER BY id) as rn FROM t QUALIFY rn = 1";
        let rewritten = rewrite(sql);
        assert_eq!(
            rewritten,
            "WITH _qualify AS (SELECT id, ROW_NUMBER() OVER (ORDER BY id) as rn FROM t) SELECT * FROM _qualify WHERE rn = 1"
        );
    }

    #[test]
    fn test_qualify_with_order_by() {
        let sql =
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) as rn FROM t QUALIFY rn = 1 ORDER BY id";
        let rewritten = rewrite(sql);
        assert_eq!(
            rewritten,
            "WITH _qualify AS (SELECT id, ROW_NUMBER() OVER (ORDER BY id) as rn FROM t) SELECT * FROM _qualify WHERE rn = 1 ORDER BY id"
        );
    }

    #[test]
    fn test_no_qualify() {
        let sql = "SELECT id, name FROM t WHERE id > 1";
        let rewritten = rewrite(sql);
        // Should not be modified (other than function rewrites which don't apply here)
        assert_eq!(rewritten, "SELECT id, name FROM t WHERE id > 1");
    }

    #[test]
    fn test_bracket_string_key() {
        let sql = "SELECT data['name'] FROM t";
        let rewritten = rewrite(sql);
        assert_eq!(rewritten, "SELECT get(data, 'name') FROM t");
    }

    #[test]
    fn test_bracket_number_index() {
        let sql = "SELECT arr[0] FROM t";
        let rewritten = rewrite(sql);
        assert_eq!(rewritten, "SELECT get(arr, 0) FROM t");
    }

    #[test]
    fn test_flatten_with_path() {
        let sql = "SELECT t.id, f.value FROM table1 t, LATERAL FLATTEN(input => t.data, path => 'items') f";
        let rewritten = rewrite(sql);

        // Should use GET_PATH to navigate to 'items' before flattening
        assert!(rewritten.contains("CROSS JOIN _numbers"));
        assert!(rewritten.contains("GET_PATH(t.data, 'items')"));
    }

    #[test]
    fn test_flatten_with_outer() {
        let sql =
            "SELECT t.id, f.value FROM table1 t, LATERAL FLATTEN(input => t.arr, outer => true) f";
        let rewritten = rewrite(sql);

        // Should use LEFT JOIN instead of CROSS JOIN
        assert!(rewritten.contains("LEFT JOIN _numbers"));
        assert!(rewritten.contains("COALESCE(ARRAY_SIZE(t.arr), 0)"));
    }

    #[test]
    fn test_flatten_options_parse() {
        let opts = FlattenOptions::parse("input => t.data, path => 'items.values', outer => true");
        assert_eq!(opts.input, "t.data");
        assert_eq!(opts.path, Some("items.values".to_string()));
        assert!(opts.outer);
    }

    #[test]
    fn test_flatten_options_mode() {
        let opts = FlattenOptions::parse("input => t.data, mode => 'ARRAY'");
        assert_eq!(opts.input, "t.data");
        assert_eq!(opts.mode, "ARRAY");
    }

    #[test]
    fn test_pivot_basic() {
        let sql =
            "SELECT product FROM sales PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))";
        let rewritten = rewrite(sql);

        // Should contain CASE WHEN expressions for each quarter
        assert!(rewritten.contains("CASE WHEN quarter = 'Q1' THEN amount END"));
        assert!(rewritten.contains("CASE WHEN quarter = 'Q2' THEN amount END"));
        assert!(rewritten.contains("AS \"Q1\""));
        assert!(rewritten.contains("AS \"Q2\""));
        // Should have SUM around the CASE WHEN
        assert!(rewritten.contains("SUM(CASE WHEN"));
        // Should have GROUP BY
        assert!(rewritten.contains("GROUP BY"));
    }

    #[test]
    fn test_pivot_no_match() {
        let sql = "SELECT * FROM sales";
        let rewritten = rewrite(sql);
        assert_eq!(rewritten, "SELECT * FROM sales");
    }

    #[test]
    fn test_unpivot_basic() {
        let sql = "SELECT * FROM quarterly_sales UNPIVOT (amount FOR quarter IN (Q1, Q2, Q3, Q4))";
        let rewritten = rewrite(sql);

        // Should contain UNION ALL
        assert!(rewritten.contains("UNION ALL"));
        // Should contain individual selects for each column
        assert!(rewritten.contains("'Q1' AS quarter"));
        assert!(rewritten.contains("'Q2' AS quarter"));
        assert!(rewritten.contains("Q1 AS amount"));
        assert!(rewritten.contains("Q2 AS amount"));
    }

    #[test]
    fn test_unpivot_no_match() {
        let sql = "SELECT * FROM sales";
        let rewritten = rewrite(sql);
        assert_eq!(rewritten, "SELECT * FROM sales");
    }

    #[test]
    fn test_sample_row_basic() {
        let sql = "SELECT * FROM users SAMPLE ROW (100)";
        let rewritten = rewrite(sql);

        // Should have ORDER BY RANDOM() and LIMIT
        assert!(rewritten.contains("ORDER BY RANDOM()"));
        assert!(rewritten.contains("LIMIT 100"));
        // SAMPLE ROW should be removed
        assert!(!rewritten.contains("SAMPLE ROW"));
    }

    #[test]
    fn test_sample_percentage_basic() {
        let sql = "SELECT * FROM users SAMPLE (10)";
        let rewritten = rewrite(sql);

        // Should have WHERE clause with RANDOM()
        assert!(rewritten.contains("RANDOM() < 0.1"));
        // SAMPLE should be removed
        assert!(!rewritten.contains("SAMPLE (10)"));
    }

    #[test]
    fn test_tablesample_basic() {
        let sql = "SELECT * FROM users TABLESAMPLE (50)";
        let rewritten = rewrite(sql);

        // Should have WHERE clause with RANDOM()
        assert!(rewritten.contains("RANDOM() < 0.5"));
        // TABLESAMPLE should be removed
        assert!(!rewritten.contains("TABLESAMPLE"));
    }

    #[test]
    fn test_sample_with_existing_where() {
        let sql = "SELECT * FROM users SAMPLE (25) WHERE active = true";
        let rewritten = rewrite(sql);

        // Should add to existing WHERE
        assert!(rewritten.contains("RANDOM() < 0.25 AND"));
        assert!(rewritten.contains("active = true"));
    }

    #[test]
    fn test_sample_no_match() {
        let sql = "SELECT * FROM users";
        let rewritten = rewrite(sql);
        assert_eq!(rewritten, "SELECT * FROM users");
    }

    #[test]
    fn test_ratio_to_report_basic() {
        let sql =
            "SELECT id, RATIO_TO_REPORT(amount) OVER (PARTITION BY region) as ratio FROM sales";
        let rewritten = rewrite(sql);
        assert!(rewritten.contains("((amount) * 1.0 / SUM(amount) OVER (PARTITION BY region))"));
    }

    #[test]
    fn test_ratio_to_report_no_partition() {
        let sql = "SELECT id, RATIO_TO_REPORT(value) OVER () as ratio FROM data";
        let rewritten = rewrite(sql);
        assert!(rewritten.contains("((value) * 1.0 / SUM(value) OVER ())"));
    }

    #[test]
    fn test_current_user_rewrite() {
        let sql = "SELECT CURRENT_USER()";
        let rewritten = rewrite(sql);
        // DataFusion treats current_user as a keyword without parentheses
        assert_eq!(rewritten, "SELECT current_user");
    }

    #[test]
    fn test_current_database_rewrite() {
        let sql = "SELECT CURRENT_DATABASE()";
        let rewritten = rewrite(sql);
        // DataFusion treats current_database as a keyword without parentheses
        assert_eq!(rewritten, "SELECT current_database");
    }

    #[test]
    fn test_current_schema_rewrite() {
        let sql = "SELECT CURRENT_SCHEMA()";
        let rewritten = rewrite(sql);
        // DataFusion treats current_schema as a keyword without parentheses
        assert_eq!(rewritten, "SELECT current_schema");
    }
}
