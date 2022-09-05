use crate::error::{Error, Result};
use crate::value::*;
use sqlparser::ast;

#[derive(Debug, PartialEq, Eq)]
pub enum Expr {
    Value(Value),
    Identifier { value: String, column_name: String },
}

pub fn ast_to_expr(expr: &ast::Expr) -> Result<Expr> {
    match expr {
        ast::Expr::Value(value) => match value {
            ast::Value::Number(ref n, ref l) => Ok(Expr::Value(Value::Number(n.to_string(), *l))),
            ast::Value::SingleQuotedString(ref s) => Ok(Expr::Value(Value::String(s.to_string()))),
            ast::Value::Boolean(ref b) => Ok(Expr::Value(Value::Boolean(*b))),
            _ => {
                return Err(Error::NotImplemented(
                    "this expr is not supported".to_string(),
                ))
            }
        },
        ast::Expr::Identifier(ref i) => {
            let value = i.value.to_string();
            let column_name = match i.quote_style {
                Some(_) => i.value.to_string(),
                None => "".to_string(),
            };
            Ok(Expr::Identifier { value, column_name })
        }
        _ => {
            return Err(Error::NotImplemented(
                "this expr is not supported".to_string(),
            ))
        }
    }
}
