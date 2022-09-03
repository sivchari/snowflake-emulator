use crate::error::{Error, Result};
use sqlparser::ast;

pub fn statement_to_plan(stmt: &ast::Statement) -> Result<&Box<ast::Select>> {
    match stmt {
        ast::Statement::Query(ref query) => query_to_plan(query),
        _ => Err(Error::NotImplemented(
            "the type is not supported, now".to_string(),
        )),
    }
}

pub fn query_to_plan(query: &ast::Query) -> Result<&Box<ast::Select>> {
    match query.body.as_ref() {
        ast::SetExpr::Select(ref select) => Ok(select),
        _ => {
            return Err(Error::NotImplemented(
                "the type is not supported, now".to_string(),
            ))
        }
    }
}
