use crate::error::{ClientError, Result};
use crate::models::Key;
use reqwest::blocking::Client;
use reqwest::StatusCode;
use url::Url;

/// HTTP client for the Keys server API
///
/// Communicates with the Java Dropwizard Keys server to create and retrieve
/// cryptographic keys for envelope encryption.
pub struct KeysClient {
    base_url: Url,
    client: Client,
}

impl KeysClient {
    /// Create a new Keys client
    ///
    /// # Arguments
    /// * `base_url` - Base URL of the Keys server (e.g., "http://localhost:8080")
    ///
    /// # Example
    /// ```no_run
    /// use violet_client::client::KeysClient;
    ///
    /// let client = KeysClient::new("http://localhost:8080").unwrap();
    /// ```
    pub fn new(base_url: impl AsRef<str>) -> Result<Self> {
        let base_url = Url::parse(base_url.as_ref())?;
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        Ok(Self { base_url, client })
    }

    /// Create a new 256-bit key on the server
    ///
    /// Calls POST /v1/keys/ on the Keys server.
    ///
    /// # Returns
    /// A Key struct containing the UUID and hex-encoded key data
    ///
    /// # Example
    /// ```no_run
    /// # use violet_client::client::KeysClient;
    /// # let client = KeysClient::new("http://localhost:8080").unwrap();
    /// let key = client.create_key().unwrap();
    /// println!("Created key: {}", key.uuid);
    /// ```
    pub fn create_key(&self) -> Result<Key> {
        let url = self.base_url.join("/v1/keys/")?;

        tracing::debug!("Creating new key at: {}", url);

        let response = self
            .client
            .post(url)
            .header("Content-Type", "application/json")
            .send()?;

        match response.status() {
            StatusCode::CREATED => {
                let key: Key = response.json()?;
                tracing::info!("Created key with UUID: {}", key.uuid);
                Ok(key)
            }
            status => {
                tracing::error!("Unexpected status creating key: {}", status);
                Err(ClientError::UnexpectedStatus(status.as_u16()))
            }
        }
    }

    /// Get an existing key by UUID
    ///
    /// Calls GET /v1/keys/{uuid} on the Keys server.
    ///
    /// # Arguments
    /// * `uuid` - The UUID of the key to retrieve
    ///
    /// # Returns
    /// The Key struct if found
    ///
    /// # Errors
    /// Returns `ClientError::KeyNotFound` if the key doesn't exist
    ///
    /// # Example
    /// ```no_run
    /// # use violet_client::client::KeysClient;
    /// # let client = KeysClient::new("http://localhost:8080").unwrap();
    /// let key = client.get_key("some-uuid-here").unwrap();
    /// ```
    pub fn get_key(&self, uuid: &str) -> Result<Key> {
        let url = self.base_url.join(&format!("/v1/keys/{}", uuid))?;

        tracing::debug!("Getting key: {}", uuid);

        let response = self.client.get(url).send()?;

        match response.status() {
            StatusCode::OK => {
                let key: Key = response.json()?;
                tracing::debug!("Retrieved key: {}", key.uuid);
                Ok(key)
            }
            StatusCode::NOT_FOUND => {
                tracing::warn!("Key not found: {}", uuid);
                Err(ClientError::KeyNotFound(uuid.to_string()))
            }
            status => {
                tracing::error!("Unexpected status getting key {}: {}", uuid, status);
                Err(ClientError::UnexpectedStatus(status.as_u16()))
            }
        }
    }

    /// Delete a key (currently a stub on the server)
    ///
    /// Calls DELETE /v1/keys/{uuid} on the Keys server.
    ///
    /// Note: The current server implementation returns 204 No Content but doesn't
    /// actually delete the key.
    pub fn delete_key(&self, uuid: &str) -> Result<()> {
        let url = self.base_url.join(&format!("/v1/keys/{}", uuid))?;

        tracing::debug!("Deleting key: {}", uuid);

        let response = self.client.delete(url).send()?;

        match response.status() {
            StatusCode::NO_CONTENT => {
                tracing::info!("Deleted key: {}", uuid);
                Ok(())
            }
            StatusCode::NOT_FOUND => {
                tracing::warn!("Key not found for deletion: {}", uuid);
                Err(ClientError::KeyNotFound(uuid.to_string()))
            }
            status => {
                tracing::error!("Unexpected status deleting key {}: {}", uuid, status);
                Err(ClientError::UnexpectedStatus(status.as_u16()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = KeysClient::new("http://localhost:8080");
        assert!(client.is_ok());
    }

    #[test]
    fn test_client_invalid_url() {
        let client = KeysClient::new("not a url");
        assert!(client.is_err());
    }

    // Integration tests (require running Keys server)
    #[test]
    #[ignore]
    fn test_create_and_get_key() {
        let client = KeysClient::new("http://localhost:8080").unwrap();

        // Create key
        let key = client.create_key().unwrap();
        assert!(!key.uuid.is_empty());
        assert_eq!(key.key.len(), 64); // 32 bytes in hex = 64 chars

        // Get key
        let retrieved = client.get_key(&key.uuid).unwrap();
        assert_eq!(key.uuid, retrieved.uuid);
        assert_eq!(key.key, retrieved.key);

        // Verify we can decode to bytes
        let bytes = retrieved.as_bytes().unwrap();
        assert_eq!(bytes.len(), 32); // 256 bits = 32 bytes
    }

    #[test]
    #[ignore]
    fn test_get_nonexistent_key() {
        let client = KeysClient::new("http://localhost:8080").unwrap();
        let result = client.get_key("nonexistent-uuid");
        assert!(matches!(result, Err(ClientError::KeyNotFound(_))));
    }
}
