use serde::{Deserialize, Serialize};

/// Key response from the Keys server API
///
/// Matches the Java Key interface in api/src/main/java/com/codeheadsystems/api/keys/v1/Key.java
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Key {
    /// UUID identifier for the key
    pub uuid: String,

    /// Hex-encoded key data (64 characters for 256-bit key)
    pub key: String,
}

impl Key {
    /// Decode the hex-encoded key to bytes
    ///
    /// The Keys server returns keys as hex strings (e.g., "a1b2c3d4..."),
    /// which must be converted to bytes for cryptographic use.
    pub fn as_bytes(&self) -> Result<Vec<u8>, hex::FromHexError> {
        hex::decode(&self.key)
    }

    /// Get the key size in bytes
    pub fn size_bytes(&self) -> usize {
        self.key.len() / 2 // Hex uses 2 characters per byte
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_as_bytes() {
        let key = Key {
            uuid: "test-uuid".to_string(),
            key: "0123456789abcdef".to_string(), // 8 bytes
        };

        let bytes = key.as_bytes().unwrap();
        assert_eq!(bytes.len(), 8);
        assert_eq!(bytes, vec![0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef]);
    }

    #[test]
    fn test_key_size_bytes() {
        let key = Key {
            uuid: "test".to_string(),
            key: "00".repeat(32), // 32 bytes = 64 hex chars
        };

        assert_eq!(key.size_bytes(), 32);
    }

    #[test]
    fn test_serialization() {
        let key = Key {
            uuid: "uuid-123".to_string(),
            key: "deadbeef".to_string(),
        };

        let json = serde_json::to_string(&key).unwrap();
        let deserialized: Key = serde_json::from_str(&json).unwrap();

        assert_eq!(key, deserialized);
    }

    #[test]
    fn test_invalid_hex() {
        let key = Key {
            uuid: "test".to_string(),
            key: "not-hex".to_string(),
        };

        assert!(key.as_bytes().is_err());
    }
}
