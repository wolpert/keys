use crate::crypto::{aes_gcm, aes_gcm_siv, types::{Algorithm, DEK_SIZE}};
use crate::error::{Result, VioletError};
use crate::models::encryption_envelope::EncryptionEnvelope;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use rand::RngCore;

/// Envelope encryptor implementing two-layer encryption pattern
///
/// Workflow:
/// 1. Generate random 256-bit DEK (Data Encryption Key)
/// 2. Encrypt data with DEK using chosen algorithm (AES-GCM or AES-GCM-SIV)
/// 3. Encrypt DEK with KEK (Key Encryption Key) from server
/// 4. Return EncryptionEnvelope with all components
pub struct EnvelopeEncryptor {
    algorithm: Algorithm,
}

impl EnvelopeEncryptor {
    pub fn new(algorithm: Algorithm) -> Self {
        Self { algorithm }
    }

    /// Encrypt plaintext using envelope encryption
    ///
    /// # Arguments
    /// * `plaintext` - Data to encrypt
    /// * `kek` - 32-byte master key from Keys server (Key Encryption Key)
    /// * `key_id` - UUID of the KEK for later retrieval
    ///
    /// # Returns
    /// EncryptionEnvelope with base64-encoded components
    pub fn encrypt(
        &self,
        plaintext: &[u8],
        kek: &[u8],
        key_id: String,
    ) -> Result<EncryptionEnvelope> {
        if kek.len() != DEK_SIZE {
            return Err(VioletError::InvalidKeySize(kek.len()));
        }

        // Step 1: Generate random DEK
        let mut dek = vec![0u8; DEK_SIZE];
        rand::thread_rng().fill_bytes(&mut dek);

        // Step 2: Encrypt plaintext with DEK
        let (ciphertext, data_iv, data_tag) = match self.algorithm {
            Algorithm::Aes256Gcm => aes_gcm::encrypt(plaintext, &dek)?,
            Algorithm::Aes256GcmSiv => aes_gcm_siv::encrypt(plaintext, &dek)?,
        };

        // Step 3: Encrypt DEK with KEK (always use AES-GCM for DEK encryption)
        let (encrypted_dek, dek_iv, dek_tag) = aes_gcm::encrypt(&dek, kek)?;

        // Store DEK encryption components concatenated: nonce || ciphertext || tag
        // This allows us to decrypt the DEK later without additional storage
        let mut dek_package = Vec::with_capacity(dek_iv.len() + encrypted_dek.len() + dek_tag.len());
        dek_package.extend_from_slice(&dek_iv);
        dek_package.extend_from_slice(&encrypted_dek);
        dek_package.extend_from_slice(&dek_tag);

        // Step 4: Build envelope
        Ok(EncryptionEnvelope {
            key_id,
            encrypted_data: BASE64.encode(&ciphertext),
            encrypted_key: BASE64.encode(&dek_package),
            iv: BASE64.encode(&data_iv),
            algorithm: self.algorithm.as_str().to_string(),
            auth_tag: BASE64.encode(&data_tag),
        })
    }

    /// Decrypt envelope
    ///
    /// # Arguments
    /// * `envelope` - EncryptionEnvelope to decrypt
    /// * `kek` - 32-byte master key from Keys server
    ///
    /// # Returns
    /// Decrypted plaintext
    pub fn decrypt(&self, envelope: &EncryptionEnvelope, kek: &[u8]) -> Result<Vec<u8>> {
        if kek.len() != DEK_SIZE {
            return Err(VioletError::InvalidKeySize(kek.len()));
        }

        // Decode base64 fields
        let encrypted_dek_with_overhead = BASE64.decode(&envelope.encrypted_key)?;
        let ciphertext = BASE64.decode(&envelope.encrypted_data)?;
        let iv = BASE64.decode(&envelope.iv)?;
        let auth_tag = BASE64.decode(&envelope.auth_tag)?;

        // Step 1: Decrypt DEK with KEK (AES-GCM appends nonce+tag, so we need to split)
        // The encrypted_dek contains: ciphertext + tag (nonce is included in the aes_gcm::encrypt output)
        // Actually, looking at our aes_gcm::encrypt implementation, it returns (ciphertext, nonce, tag) separately
        // But when we encrypted the DEK above, we only stored the ciphertext part
        // So encrypted_dek_with_overhead contains the full ciphertext+tag from aes_gcm::encrypt

        // For decryption, we need to extract: actual_encrypted_dek, dek_nonce, dek_tag
        // The aes-gcm crate's encrypt appends the tag, and we split it in our aes_gcm::encrypt
        // But we only saved the ciphertext part when building the envelope!

        // Wait, I need to reconsider this. Let me check the encrypt method again.
        // In encrypt(), we call aes_gcm::encrypt(&dek, kek) which returns (encrypted_dek, _dek_iv, _dek_tag)
        // Then we only store BASE64.encode(&encrypted_dek) in encrypted_key.
        // This means we lost the IV and tag for DEK decryption!

        // FIX: We need to store the DEK's IV and tag as well, OR we need to concatenate them.
        // For simplicity, let's concatenate IV + ciphertext + tag in the encrypted_key field.
        // But that changes the encrypt() method.

        // Actually, let me rethink the design. The EncryptionEnvelope has one IV field.
        // That IV is for the data encryption, not DEK encryption.
        // For DEK encryption, we could use a different IV, but we need to store it.

        // Standard practice: The encrypted_key field should contain everything needed to decrypt the DEK.
        // So encrypted_key = IV || encrypted_DEK || tag (all concatenated)

        // I'll fix this by modifying the encrypt method to concatenate.

        // For now, let me implement a version that handles this correctly.
        // The encrypted_dek_with_overhead should contain: nonce(12) + ciphertext(32) + tag(16) = 60 bytes

        // Let me parse the algorithm
        let algorithm = Algorithm::from_str(&envelope.algorithm)?;

        // For the DEK, we'll use a simpler approach:
        // Store nonce || ciphertext || tag in encrypted_key

        // Extract nonce, ciphertext, tag from encrypted_dek_with_overhead
        if encrypted_dek_with_overhead.len() < 12 + 16 {
            return Err(VioletError::CryptoError("Invalid encrypted DEK length".into()));
        }

        let dek_nonce = &encrypted_dek_with_overhead[..12];
        let dek_data_end = encrypted_dek_with_overhead.len() - 16;
        let dek_ciphertext = &encrypted_dek_with_overhead[12..dek_data_end];
        let dek_tag = &encrypted_dek_with_overhead[dek_data_end..];

        let dek = aes_gcm::decrypt(dek_ciphertext, kek, dek_nonce, dek_tag)?;

        if dek.len() != DEK_SIZE {
            return Err(VioletError::CryptoError(format!("Invalid DEK size: {}", dek.len())));
        }

        // Step 2: Decrypt plaintext with DEK
        let plaintext = match algorithm {
            Algorithm::Aes256Gcm => aes_gcm::decrypt(&ciphertext, &dek, &iv, &auth_tag)?,
            Algorithm::Aes256GcmSiv => aes_gcm_siv::decrypt(&ciphertext, &dek, &iv, &auth_tag)?,
        };

        Ok(plaintext)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_envelope_encryption_gcm() {
        let kek = [42u8; 32];
        let plaintext = b"Sensitive data that needs protection";

        let encryptor = EnvelopeEncryptor::new(Algorithm::Aes256Gcm);
        let envelope = encryptor.encrypt(plaintext, &kek, "test-key-123".to_string()).unwrap();

        // Verify envelope structure
        assert_eq!(envelope.key_id, "test-key-123");
        assert_eq!(envelope.algorithm, "AES-256-GCM");
        assert!(!envelope.encrypted_data.is_empty());
        assert!(!envelope.encrypted_key.is_empty());
        assert!(!envelope.iv.is_empty());
        assert!(!envelope.auth_tag.is_empty());

        // Decrypt
        let decrypted = encryptor.decrypt(&envelope, &kek).unwrap();
        assert_eq!(plaintext, &decrypted[..]);
    }

    #[test]
    fn test_envelope_encryption_gcm_siv() {
        let kek = [99u8; 32];
        let plaintext = b"Another secret message";

        let encryptor = EnvelopeEncryptor::new(Algorithm::Aes256GcmSiv);
        let envelope = encryptor.encrypt(plaintext, &kek, "key-uuid-456".to_string()).unwrap();

        assert_eq!(envelope.algorithm, "AES-256-GCM-SIV");

        let decrypted = encryptor.decrypt(&envelope, &kek).unwrap();
        assert_eq!(plaintext, &decrypted[..]);
    }

    #[test]
    fn test_decrypt_with_wrong_kek() {
        let kek1 = [1u8; 32];
        let kek2 = [2u8; 32];
        let plaintext = b"secret";

        let encryptor = EnvelopeEncryptor::new(Algorithm::Aes256Gcm);
        let envelope = encryptor.encrypt(plaintext, &kek1, "test".to_string()).unwrap();

        let result = encryptor.decrypt(&envelope, &kek2);
        assert!(result.is_err());
    }

    #[test]
    fn test_serialization_roundtrip() {
        let kek = [77u8; 32];
        let plaintext = b"Test message for JSON roundtrip";

        let encryptor = EnvelopeEncryptor::new(Algorithm::Aes256Gcm);
        let envelope = encryptor.encrypt(plaintext, &kek, "uuid-789".to_string()).unwrap();

        // Serialize to JSON
        let json = serde_json::to_string(&envelope).unwrap();

        // Deserialize from JSON
        let deserialized: EncryptionEnvelope = serde_json::from_str(&json).unwrap();

        // Decrypt using deserialized envelope
        let decrypted = encryptor.decrypt(&deserialized, &kek).unwrap();
        assert_eq!(plaintext, &decrypted[..]);
    }

    #[test]
    fn test_invalid_kek_size() {
        let encryptor = EnvelopeEncryptor::new(Algorithm::Aes256Gcm);
        let result = encryptor.encrypt(b"test", &[0u8; 16], "test".to_string());
        assert!(matches!(result, Err(VioletError::InvalidKeySize(16))));
    }
}
