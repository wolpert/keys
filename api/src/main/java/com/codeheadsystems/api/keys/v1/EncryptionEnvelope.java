package com.codeheadsystems.api.keys.v1;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

/**
 * The interface Encryption envelope.
 * <p>
 * Represents an encrypted data package containing the ciphertext,
 * encrypted data encryption key (DEK), and metadata needed for decryption.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableEncryptionEnvelope.class)
@JsonDeserialize(builder = ImmutableEncryptionEnvelope.Builder.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface EncryptionEnvelope {

  /**
   * Key ID that was used to encrypt the data encryption key.
   *
   * @return the key ID
   */
  @JsonProperty("keyId")
  String keyId();

  /**
   * The encrypted data (ciphertext) as a base64-encoded string.
   *
   * @return the encrypted data
   */
  @JsonProperty("encryptedData")
  String encryptedData();

  /**
   * The encrypted data encryption key (DEK) as a base64-encoded string.
   * This key is encrypted with the master key identified by keyId.
   *
   * @return the encrypted DEK
   */
  @JsonProperty("encryptedKey")
  String encryptedKey();

  /**
   * Initialization vector (IV) as a base64-encoded string.
   * Required for certain encryption algorithms.
   *
   * @return the initialization vector
   */
  @JsonProperty("iv")
  String iv();

  /**
   * The encryption algorithm used (e.g., "AES-256-GCM", "AES-256-CBC").
   *
   * @return the algorithm name
   */
  @JsonProperty("algorithm")
  String algorithm();

  /**
   * Authentication tag for authenticated encryption modes (e.g., GCM).
   * Optional, base64-encoded string.
   *
   * @return the authentication tag, if applicable
   */
  @JsonProperty("authTag")
  @Value.Default
  default String authTag() {
    return "";
  }

}
