use thiserror::Error;

#[derive(Error, Debug)]
pub enum VioletError {
    #[error("Invalid key size: {0} bytes (expected 32)")]
    InvalidKeySize(usize),

    #[error("Invalid nonce size: {0} bytes")]
    InvalidNonceSize(usize),

    #[error("Invalid tag size: {0} bytes")]
    InvalidTagSize(usize),

    #[error("Encryption failed: {0}")]
    EncryptionFailed(String),

    #[error("Decryption failed: {0}")]
    DecryptionFailed(String),

    #[error("Crypto error: {0}")]
    CryptoError(String),

    #[error("Base64 decode error: {0}")]
    Base64Error(#[from] base64::DecodeError),

    #[error("Invalid algorithm: {0}")]
    InvalidAlgorithm(String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Hex decode error: {0}")]
    HexError(#[from] hex::FromHexError),
}

pub type Result<T> = std::result::Result<T, VioletError>;
