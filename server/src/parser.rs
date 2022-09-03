use crate::error::Result;
use sqlparser::ast;
use sqlparser::dialect::SnowflakeDialect;
use sqlparser::parser;
use sqlparser::tokenizer;

pub fn parse_to_statment(query: &str) -> Result<ast::Statement> {
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
    use sqlparser::ast;

    #[test]
    fn parse_simple() {
        let query = "SELECT 1;";
        let stmt = parse_to_statment(query).unwrap();
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
}
