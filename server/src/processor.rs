use crate::error::Result;
use crate::expr;
use crate::plan;
use crate::value;

// TODO: impl Iterator
#[derive(Debug)]
pub struct Rows {
    pub rows: Vec<String>,
}

impl plan::Plan {
    pub fn execute_plan(&self) -> Result<Rows> {
        match self {
            plan::Plan::Select { ref exprs } => {
                let mut rows: Vec<String> = Vec::new();
                for expr in exprs.iter() {
                    match expr {
                        expr::Expr::Value(ref value) => match value {
                            value::Value::Number(ref n, ref _l) => {
                                rows.push(n.to_owned());
                            }
                        },
                    }
                }
                Ok(Rows { rows })
            }
        }
    }
}
