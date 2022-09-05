use crate::error::Result;
use crate::expr;
use crate::plan;
use crate::value;

// TODO: impl Iterator
#[derive(Debug)]
pub struct Rows {
    pub rowsets: Vec<Vec<String>>,
    pub rowtypes: Vec<RowType>,
}

#[derive(Debug)]
pub struct RowType {
    pub name: String,
    pub r#type: String,
}

// rowtypes: [int, string, bool]
// rowsets: [["1", "a", "true"], ["2", "b", "false"]]
impl plan::Plan {
    pub fn execute_plan(&self) -> Result<Rows> {
        let mut rowsets: Vec<Vec<String>> = Vec::new();
        match self {
            plan::Plan::Select { ref exprs } => {
                let mut rows: Vec<String> = Vec::new();
                let mut rowtypes: Vec<RowType> = Vec::new();
                for expr in exprs.iter() {
                    match expr {
                        expr::Expr::Value(ref value) => match value {
                            value::Value::Number(ref n, ref l) => {
                                rows.push(n.to_string());
                                match *l {
                                    true => rowtypes.push(RowType {
                                        name: n.to_string(),
                                        r#type: "float".to_string(),
                                    }),
                                    false => rowtypes.push(RowType {
                                        name: n.to_string(),
                                        r#type: "number".to_string(),
                                    }),
                                }
                            }
                            value::Value::String(ref s) => {
                                rows.push(s.to_string());
                                rowtypes.push(RowType {
                                    name: s.to_string(),
                                    r#type: "string".to_string(),
                                });
                            }
                            value::Value::Boolean(ref b) => {
                                rows.push(b.to_string());
                                rowtypes.push(RowType {
                                    name: b.to_string(),
                                    r#type: "boolean".to_string(),
                                });
                            }
                        },
                        expr::Expr::Identifier {
                            ref value,
                            ref column_name,
                        } => {
                            rows.push(value.to_string());
                            let name = match column_name.as_str() {
                                "" => value.to_string(),
                                _ => column_name.to_string(),
                            };
                            rowtypes.push(RowType {
                                name,
                                r#type: "string".to_string(),
                            });
                        }
                    }
                }
                rowsets.push(rows);
                Ok(Rows { rowsets, rowtypes })
            }
        }
    }
}
