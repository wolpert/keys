use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("HTTP request failed: {0}")]
    RequestFailed(#[from] reqwest::Error),

    #[error("URL parse error: {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Unexpected HTTP status: {0}")]
    UnexpectedStatus(u16),

    #[error("Hex decode error: {0}")]
    HexDecodeError(#[from] hex::FromHexError),

    #[error("Invalid key format")]
    InvalidKeyFormat,
}

pub type Result<T> = std::result::Result<T, ClientError>;
