use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use crate::crypto::types::{GCM_NONCE_SIZE, GCM_TAG_SIZE};
use crate::error::{Result, VioletError};
use rand::RngCore;

/// Encrypt data with AES-256-GCM
///
/// Returns: (ciphertext, nonce, tag)
///
/// Note: AES-GCM in the `aes-gcm` crate appends the tag to ciphertext,
/// but we need to separate it for the EncryptionEnvelope format
pub fn encrypt(plaintext: &[u8], key: &[u8]) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    if key.len() != 32 {
        return Err(VioletError::InvalidKeySize(key.len()));
    }

    // Generate random nonce
    let mut nonce_bytes = vec![0u8; GCM_NONCE_SIZE];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    // Create cipher
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|_| VioletError::CryptoError("Invalid key".into()))?;

    // Encrypt
    let ciphertext_with_tag = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| VioletError::EncryptionFailed(e.to_string()))?;

    // Split ciphertext and tag
    let tag_start = ciphertext_with_tag.len() - GCM_TAG_SIZE;
    let ciphertext = ciphertext_with_tag[..tag_start].to_vec();
    let tag = ciphertext_with_tag[tag_start..].to_vec();

    Ok((ciphertext, nonce_bytes, tag))
}

/// Decrypt data with AES-256-GCM
pub fn decrypt(
    ciphertext: &[u8],
    key: &[u8],
    nonce: &[u8],
    tag: &[u8],
) -> Result<Vec<u8>> {
    if key.len() != 32 {
        return Err(VioletError::InvalidKeySize(key.len()));
    }
    if nonce.len() != GCM_NONCE_SIZE {
        return Err(VioletError::InvalidNonceSize(nonce.len()));
    }
    if tag.len() != GCM_TAG_SIZE {
        return Err(VioletError::InvalidTagSize(tag.len()));
    }

    // Reconstruct ciphertext with tag
    let mut ciphertext_with_tag = Vec::with_capacity(ciphertext.len() + tag.len());
    ciphertext_with_tag.extend_from_slice(ciphertext);
    ciphertext_with_tag.extend_from_slice(tag);

    let nonce_obj = Nonce::from_slice(nonce);
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|_| VioletError::CryptoError("Invalid key".into()))?;

    let plaintext = cipher
        .decrypt(nonce_obj, ciphertext_with_tag.as_ref())
        .map_err(|e| VioletError::DecryptionFailed(e.to_string()))?;

    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let key = [0u8; 32];
        let plaintext = b"Hello, World!";

        let (ciphertext, nonce, tag) = encrypt(plaintext, &key).unwrap();
        let decrypted = decrypt(&ciphertext, &key, &nonce, &tag).unwrap();

        assert_eq!(plaintext, &decrypted[..]);
    }

    #[test]
    fn test_invalid_key_size() {
        let result = encrypt(b"test", &[0u8; 16]);
        assert!(matches!(result, Err(VioletError::InvalidKeySize(16))));
    }

    #[test]
    fn test_invalid_nonce_size() {
        let result = decrypt(&[0u8; 10], &[0u8; 32], &[0u8; 10], &[0u8; 16]);
        assert!(matches!(result, Err(VioletError::InvalidNonceSize(10))));
    }

    #[test]
    fn test_invalid_tag_size() {
        let result = decrypt(&[0u8; 10], &[0u8; 32], &[0u8; 12], &[0u8; 10]);
        assert!(matches!(result, Err(VioletError::InvalidTagSize(10))));
    }

    #[test]
    fn test_decrypt_with_wrong_key() {
        let key1 = [1u8; 32];
        let key2 = [2u8; 32];
        let plaintext = b"secret";

        let (ciphertext, nonce, tag) = encrypt(plaintext, &key1).unwrap();
        let result = decrypt(&ciphertext, &key2, &nonce, &tag);

        assert!(result.is_err());
    }
}
