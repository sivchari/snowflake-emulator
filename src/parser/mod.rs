use sqlparser::dialect::SnowflakeDialect;
use sqlparser::parser::Parser;

pub fn parse(query: &str) {
    println!("{:?}", query);
    let dialect = SnowflakeDialect {};
    let ast = Parser::parse_sql(&dialect, query).unwrap();
    println!("{:?}", ast)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_simple() {
        let query = "SELECT 1;";
        parse(query)
    }
}
