use serde::{Deserialize, Serialize};
use crate::error::{Result, VioletError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Algorithm {
    #[serde(rename = "AES-256-GCM")]
    Aes256Gcm,
    #[serde(rename = "AES-256-GCM-SIV")]
    Aes256GcmSiv,
}

impl Algorithm {
    pub fn as_str(&self) -> &'static str {
        match self {
            Algorithm::Aes256Gcm => "AES-256-GCM",
            Algorithm::Aes256GcmSiv => "AES-256-GCM-SIV",
        }
    }

    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "AES-256-GCM" => Ok(Algorithm::Aes256Gcm),
            "AES-256-GCM-SIV" => Ok(Algorithm::Aes256GcmSiv),
            _ => Err(VioletError::InvalidAlgorithm(s.to_string())),
        }
    }
}

impl Default for Algorithm {
    fn default() -> Self {
        Algorithm::Aes256Gcm
    }
}

// Constants
pub const DEK_SIZE: usize = 32; // 256 bits
pub const GCM_NONCE_SIZE: usize = 12; // 96 bits (recommended)
pub const GCM_SIV_NONCE_SIZE: usize = 12; // 96 bits
pub const GCM_TAG_SIZE: usize = 16; // 128 bits

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_algorithm_as_str() {
        assert_eq!(Algorithm::Aes256Gcm.as_str(), "AES-256-GCM");
        assert_eq!(Algorithm::Aes256GcmSiv.as_str(), "AES-256-GCM-SIV");
    }

    #[test]
    fn test_algorithm_from_str() {
        assert_eq!(Algorithm::from_str("AES-256-GCM").unwrap(), Algorithm::Aes256Gcm);
        assert_eq!(Algorithm::from_str("AES-256-GCM-SIV").unwrap(), Algorithm::Aes256GcmSiv);
        assert!(Algorithm::from_str("INVALID").is_err());
    }

    #[test]
    fn test_algorithm_default() {
        assert_eq!(Algorithm::default(), Algorithm::Aes256Gcm);
    }
}
