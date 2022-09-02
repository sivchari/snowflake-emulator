use sqlparser::dialect::SnowflakeDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;

pub fn parse(query: &str) -> String {
    let dialect = SnowflakeDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, query);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(tokens, &dialect);
    let stmt = parser.parse_statement().unwrap();
    stmt.to_string()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_simple() {
        let query = "SELECT 1;";
        let stmt = parse(query);
        println!("{:?}", stmt);
    }
}
