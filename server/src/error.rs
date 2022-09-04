use sqlparser::parser;
use sqlparser::tokenizer;
use std::fmt;
// use std::num;

#[derive(Debug)]
pub enum Error {
    NotImplemented(String),
    // TODO: refactor
    ParserError(parser::ParserError),
    TokenizerError(tokenizer::TokenizerError),
    // ParseIntError(num::ParseIntError),
}

impl From<parser::ParserError> for Error {
    fn from(error: parser::ParserError) -> Self {
        Error::ParserError(error)
    }
}

impl From<tokenizer::TokenizerError> for Error {
    fn from(error: tokenizer::TokenizerError) -> Self {
        Error::TokenizerError(error)
    }
}

// impl From<num::ParseIntError> for Error {
//     fn from(error: num::ParseIntError) -> Self {
//         Error::ParseIntError(error)
//     }
// }

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::NotImplemented(msg) => write!(f, "Not implemented error {}", msg),
            Error::ParserError(msg) => write!(f, "ParserError {}", msg),
            Error::TokenizerError(msg) => write!(f, "TokenizerError {}", msg),
            // Error::ParseIntError(msg) => write!(f, "ParseIntError {}", msg),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
