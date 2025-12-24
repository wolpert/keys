use aes_gcm_siv::{
    aead::{Aead, KeyInit},
    Aes256GcmSiv, Nonce,
};
use crate::crypto::types::{GCM_SIV_NONCE_SIZE, GCM_TAG_SIZE};
use crate::error::{Result, VioletError};
use rand::RngCore;

/// Encrypt data with AES-256-GCM-SIV
///
/// Returns: (ciphertext, nonce, tag)
///
/// Note: GCM-SIV is nonce-misuse resistant, making it safer when
/// nonce uniqueness cannot be guaranteed
pub fn encrypt(plaintext: &[u8], key: &[u8]) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    if key.len() != 32 {
        return Err(VioletError::InvalidKeySize(key.len()));
    }

    // Generate random nonce
    let mut nonce_bytes = vec![0u8; GCM_SIV_NONCE_SIZE];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    // Create cipher
    let cipher = Aes256GcmSiv::new_from_slice(key)
        .map_err(|_| VioletError::CryptoError("Invalid key".into()))?;

    // Encrypt
    let ciphertext_with_tag = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| VioletError::EncryptionFailed(e.to_string()))?;

    // GCM-SIV also appends tag, separate it
    let tag_start = ciphertext_with_tag.len() - GCM_TAG_SIZE;
    let ciphertext = ciphertext_with_tag[..tag_start].to_vec();
    let tag = ciphertext_with_tag[tag_start..].to_vec();

    Ok((ciphertext, nonce_bytes, tag))
}

/// Decrypt data with AES-256-GCM-SIV
pub fn decrypt(
    ciphertext: &[u8],
    key: &[u8],
    nonce: &[u8],
    tag: &[u8],
) -> Result<Vec<u8>> {
    if key.len() != 32 {
        return Err(VioletError::InvalidKeySize(key.len()));
    }
    if nonce.len() != GCM_SIV_NONCE_SIZE {
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
    let cipher = Aes256GcmSiv::new_from_slice(key)
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
    fn test_decrypt_with_wrong_key() {
        let key1 = [1u8; 32];
        let key2 = [2u8; 32];
        let plaintext = b"secret";

        let (ciphertext, nonce, tag) = encrypt(plaintext, &key1).unwrap();
        let result = decrypt(&ciphertext, &key2, &nonce, &tag);

        assert!(result.is_err());
    }

    #[test]
    fn test_nonce_misuse_resistance() {
        // GCM-SIV is designed to be safer with nonce reuse
        // This test verifies we can encrypt with same nonce (though not recommended)
        let key = [1u8; 32];
        let plaintext = b"test";

        let (_, nonce, _) = encrypt(plaintext, &key).unwrap();

        // Using fixed nonce (simulating nonce reuse)
        let nonce_obj = Nonce::from_slice(&nonce);
        let cipher = Aes256GcmSiv::new_from_slice(&key).unwrap();

        let ct1 = cipher.encrypt(nonce_obj, plaintext.as_ref()).unwrap();
        let ct2 = cipher.encrypt(nonce_obj, plaintext.as_ref()).unwrap();

        // With deterministic nonce, ciphertexts should be identical
        assert_eq!(ct1, ct2);
    }
}
