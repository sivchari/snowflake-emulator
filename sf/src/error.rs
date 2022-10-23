use sqlparser::parser;
use sqlparser::tokenizer;
use std::fmt;

#[derive(Debug)]
pub enum Error {
    NotImplemented(String),
    ParserError(parser::ParserError),
    TokenizerError(tokenizer::TokenizerError),
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

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::NotImplemented(msg) => write!(f, "Not implemented error {}", msg),
            Error::ParserError(msg) => write!(f, "ParserError {}", msg),
            Error::TokenizerError(msg) => write!(f, "TokenizerError {}", msg),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
