use crate::error::Result;
use sqlparser::ast;
use sqlparser::dialect::SnowflakeDialect;
use sqlparser::parser;
use sqlparser::tokenizer;

pub fn query_to_statment(query: &str) -> Result<ast::Statement> {
    let dialect = SnowflakeDialect {};
    let mut tokenizer = tokenizer::Tokenizer::new(&dialect, query);
    let tokens = tokenizer.tokenize()?;
    let mut parser = parser::Parser::new(tokens, &dialect);
    let stmt = parser.parse_statement()?;
    Ok(stmt)
}

#[cfg(test)]
mod test {
    use super::*;
    use sqlparser::{ast, keywords::NO};

    #[test]
    fn parse_select1() {
        let query = "SELECT 1;";
        let stmt = query_to_statment(query).unwrap();
        let expected = ast::Statement::Query(Box::new(ast::Query {
            with: None,
            body: Box::new(ast::SetExpr::Select(Box::new(ast::Select {
                distinct: false,
                top: None,
                projection: vec![ast::SelectItem::UnnamedExpr(ast::Expr::Value(
                    ast::Value::Number("1".to_string(), false),
                ))],
                into: None,
                from: vec![],
                lateral_views: vec![],
                selection: None,
                group_by: vec![],
                cluster_by: vec![],
                distribute_by: vec![],
                sort_by: vec![],
                having: None,
                qualify: None,
            }))),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
            lock: None,
        }));
        assert_eq!(expected, stmt);
    }

    #[test]
    fn parse_simple() {
        let query = r#"SELECT 1, a, "b", 'c', true FROM t;"#;
        let stmt = query_to_statment(query).unwrap();
        let expected = ast::Statement::Query(Box::new(ast::Query {
            with: None,
            body: Box::new(ast::SetExpr::Select(Box::new(ast::Select {
                distinct: false,
                top: None,
                projection: vec![
                    ast::SelectItem::UnnamedExpr(ast::Expr::Value(ast::Value::Number(
                        "1".to_string(),
                        false,
                    ))),
                    ast::SelectItem::UnnamedExpr(ast::Expr::Identifier(ast::Ident {
                        value: "a".to_string(),
                        quote_style: None,
                    })),
                    ast::SelectItem::UnnamedExpr(ast::Expr::Identifier(ast::Ident {
                        value: "b".to_string(),
                        quote_style: Some('"'),
                    })),
                    ast::SelectItem::UnnamedExpr(ast::Expr::Value(ast::Value::SingleQuotedString(
                        "c".to_string(),
                    ))),
                    ast::SelectItem::UnnamedExpr(ast::Expr::Value(ast::Value::Boolean(true))),
                ],
                into: None,
                from: vec![ast::TableWithJoins {
                    relation: ast::TableFactor::Table {
                        name: ast::ObjectName(vec![ast::Ident {
                            value: "t".to_string(),
                            quote_style: None,
                        }]),
                        alias: None,
                        args: None,
                        with_hints: vec![],
                    },
                    joins: vec![],
                }],
                lateral_views: vec![],
                selection: None,
                group_by: vec![],
                cluster_by: vec![],
                distribute_by: vec![],
                sort_by: vec![],
                having: None,
                qualify: None,
            }))),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
            lock: None,
        }));
        assert_eq!(expected, stmt);
    }
}
