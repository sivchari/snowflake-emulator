use std::fmt;

#[derive(Debug, PartialEq, Eq)]
pub enum Value {
    // e.g.
    // 1 -> Number("1", false)
    // 1.5 -> Number("1.5", true)
    Number(String, bool),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Number(ref n, l) => write!(f, "{}{long}", n, long = if *l { "L" } else { "" }),
        }
    }
}
