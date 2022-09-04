use crate::error::{Error, Result};
use crate::expr;
use crate::value;
use sqlparser::ast;

#[derive(Debug)]
pub enum Plan {
    Select { exprs: Vec<expr::Expr> },
}

pub fn statement_to_plan(stmt: &ast::Statement) -> Result<Plan> {
    match stmt {
        ast::Statement::Query(ref query) => query_to_plan(query),
        _ => Err(Error::NotImplemented(
            "the type is not supported, now".to_string(),
        )),
    }
}

pub fn query_to_plan(query: &ast::Query) -> Result<Plan> {
    match query.body.as_ref() {
        ast::SetExpr::Select(ref select) => {
            let mut exprs: Vec<expr::Expr> = Vec::new();
            for p in select.projection.iter() {
                match p {
                    ast::SelectItem::UnnamedExpr(ref expr) => match expr {
                        ast::Expr::Value(value) => match value {
                            ast::Value::Number(ref n, ref l) => {
                                let value = value::Value::Number(n.to_string(), *l);
                                exprs.push(expr::Expr::Value(value))
                            }
                            _ => {
                                return Err(Error::NotImplemented(
                                    "this expr is not supported".to_string(),
                                ))
                            }
                        },
                        _ => {
                            return Err(Error::NotImplemented(
                                "this expr is not supported".to_string(),
                            ))
                        }
                    },
                    _ => {
                        return Err(Error::NotImplemented(
                            "this expr is not supported".to_string(),
                        ))
                    }
                }
            }
            Ok(Plan::Select { exprs })
        }
        _ => {
            return Err(Error::NotImplemented(
                "the type is not supported, now".to_string(),
            ))
        }
    }
}
