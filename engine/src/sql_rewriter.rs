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

    // Rewrite LATERAL FLATTEN
    result = rewrite_lateral_flatten(&result);

    result
}

/// Rewrite LATERAL FLATTEN to CROSS JOIN with _NUMBERS
///
/// Pattern: `LATERAL FLATTEN(input => expr) [AS alias]`
/// Becomes: `CROSS JOIN _numbers AS _flatten_idx` with WHERE clause
fn rewrite_lateral_flatten(sql: &str) -> String {
    // Pattern to match: , LATERAL FLATTEN(input => expr) [AS] alias
    // or: FROM ... , LATERAL FLATTEN(input => expr) [AS] alias
    let lateral_flatten_pattern =
        Regex::new(r"(?i),\s*LATERAL\s+FLATTEN\s*\(\s*input\s*=>\s*([^)]+)\s*\)\s*(?:AS\s+)?(\w+)")
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
            let input_expr = cap.get(1).unwrap().as_str().trim();
            let alias = cap.get(2).unwrap().as_str();
            (
                full_match.start(),
                full_match.end(),
                input_expr.to_string(),
                alias.to_string(),
            )
        })
        .collect();

    // Process matches in reverse order to preserve positions
    for (start, end, input_expr, alias) in matches.into_iter().rev() {
        flatten_count += 1;
        let idx_alias = format!("_flatten_idx_{}", flatten_count);

        // Replace LATERAL FLATTEN with CROSS JOIN
        let replacement = format!(" CROSS JOIN _numbers AS {}", idx_alias);
        result.replace_range(start..end, &replacement);

        // Add WHERE clause for array bounds
        result = add_flatten_where_clause(&result, &input_expr, &idx_alias);

        // Replace alias.value references with FLATTEN_ARRAY calls
        result = replace_flatten_column_refs(&result, &alias, &input_expr, &idx_alias);
    }

    result
}

/// Add WHERE clause for FLATTEN array bounds
fn add_flatten_where_clause(sql: &str, input_expr: &str, idx_alias: &str) -> String {
    let condition = format!("{}.idx < ARRAY_SIZE({})", idx_alias, input_expr);

    // Check if WHERE clause already exists
    let where_pattern = Regex::new(r"(?i)\bWHERE\b").unwrap();

    if where_pattern.is_match(sql) {
        // Add condition to existing WHERE clause
        let result = where_pattern.replace(sql, &format!("WHERE {} AND ", condition));
        result.to_string()
    } else {
        // Find position to insert WHERE clause (before GROUP BY, ORDER BY, LIMIT, or end)
        let insert_pattern = Regex::new(r"(?i)\s*(GROUP\s+BY|ORDER\s+BY|LIMIT|HAVING|$)").unwrap();

        if let Some(mat) = insert_pattern.find(sql) {
            let pos = mat.start();
            let mut result = sql.to_string();
            result.insert_str(pos, &format!(" WHERE {} ", condition));
            result
        } else {
            format!("{} WHERE {}", sql, condition)
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
            format!("FLATTEN_ARRAY({}, {}.idx)", input_expr, idx_alias),
        )
        .to_string();

    // Replace alias.index with idx
    let index_pattern = Regex::new(&format!(r"(?i)\b{}\.(index)\b", regex::escape(alias))).unwrap();
    result = index_pattern
        .replace_all(&result, format!("{}.idx", idx_alias))
        .to_string();

    // Replace alias.this with the original input expression
    let this_pattern = Regex::new(&format!(r"(?i)\b{}\.(this)\b", regex::escape(alias))).unwrap();
    result = this_pattern.replace_all(&result, input_expr).to_string();

    result
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
}
