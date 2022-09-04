use crate::value::*;

#[derive(Debug, PartialEq, Eq)]
pub enum Expr {
    Value(Value),
}
