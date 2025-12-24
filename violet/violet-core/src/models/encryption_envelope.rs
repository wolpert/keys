use serde::{Deserialize, Serialize};

/// Represents an encrypted data package containing the ciphertext,
/// encrypted data encryption key (DEK), and metadata needed for decryption.
///
/// This structure matches the Java EncryptionEnvelope API definition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EncryptionEnvelope {
    /// UUID of the master key (KEK) from Keys server
    pub key_id: String,

    /// Base64-encoded ciphertext (encrypted plaintext)
    pub encrypted_data: String,

    /// Base64-encoded encrypted DEK (DEK encrypted with KEK)
    pub encrypted_key: String,

    /// Base64-encoded initialization vector / nonce
    pub iv: String,

    /// Algorithm identifier ("AES-256-GCM" or "AES-256-GCM-SIV")
    pub algorithm: String,

    /// Base64-encoded authentication tag (may be empty for some algorithms)
    #[serde(default)]
    pub auth_tag: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialization() {
        let envelope = EncryptionEnvelope {
            key_id: "test-uuid-1234".to_string(),
            encrypted_data: "Y2lwaGVydGV4dA==".to_string(),
            encrypted_key: "ZW5jcnlwdGVkLWRlaw==".to_string(),
            iv: "bm9uY2U=".to_string(),
            algorithm: "AES-256-GCM".to_string(),
            auth_tag: "dGFn".to_string(),
        };

        let json = serde_json::to_string(&envelope).unwrap();
        let deserialized: EncryptionEnvelope = serde_json::from_str(&json).unwrap();

        assert_eq!(envelope, deserialized);
    }

    #[test]
    fn test_empty_auth_tag() {
        let json = r#"{"keyId":"test","encryptedData":"data","encryptedKey":"key","iv":"iv","algorithm":"AES-256-GCM"}"#;
        let envelope: EncryptionEnvelope = serde_json::from_str(json).unwrap();
        assert_eq!(envelope.auth_tag, "");
    }
}
