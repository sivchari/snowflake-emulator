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
            "WITH _qualify AS ({}) SELECT * FROM _qualify WHERE {}",
            select_part, qualify_condition
        );

        // Append ORDER BY/LIMIT if present
        if !trailing_clauses.is_empty() {
            return format!("{} {}", result, trailing_clauses);
        }
        return result;
    }

    sql.to_string()
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
}
