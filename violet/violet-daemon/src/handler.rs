use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use violet_client::KeysClient;
use violet_core::{Algorithm, EnvelopeEncryptor};
use crate::protocol::{Request, Response, Operation};

pub struct RequestHandler {
    server_url: String,
}

impl RequestHandler {
    pub fn new(server_url: &str) -> Self {
        Self {
            server_url: server_url.to_string(),
        }
    }

    pub async fn handle(&self, request: Request) -> Response {
        match request.operation {
            Operation::Encrypt => self.handle_encrypt(request).await,
            Operation::Decrypt => self.handle_decrypt(request).await,
        }
    }

    async fn handle_encrypt(&self, request: Request) -> Response {
        // Extract request data
        let plaintext = match BASE64.decode(&request.data.plaintext) {
            Ok(pt) => pt,
            Err(e) => return Response::error(format!("Invalid base64: {}", e)),
        };

        let algorithm = request.data.algorithm.unwrap_or_default();

        // Get or create key
        let client = match KeysClient::new(&self.server_url) {
            Ok(c) => c,
            Err(e) => return Response::error(format!("Client error: {}", e)),
        };

        let (kek_id, kek_bytes) = if let Some(kid) = request.data.key_id {
            match client.get_key(&kid) {
                Ok(key) => {
                    let bytes = match key.as_bytes() {
                        Ok(b) => b,
                        Err(e) => return Response::error(format!("Key decode error: {}", e)),
                    };
                    (key.uuid, bytes)
                }
                Err(e) => return Response::error(format!("Failed to get key: {}", e)),
            }
        } else {
            match client.create_key() {
                Ok(key) => {
                    let bytes = match key.as_bytes() {
                        Ok(b) => b,
                        Err(e) => return Response::error(format!("Key decode error: {}", e)),
                    };
                    (key.uuid, bytes)
                }
                Err(e) => return Response::error(format!("Failed to create key: {}", e)),
            }
        };

        // Encrypt
        let encryptor = EnvelopeEncryptor::new(algorithm);
        match encryptor.encrypt(&plaintext, &kek_bytes, kek_id) {
            Ok(envelope) => Response::success_encrypt(envelope),
            Err(e) => Response::error(format!("Encryption failed: {}", e)),
        }
    }

    async fn handle_decrypt(&self, request: Request) -> Response {
        let envelope = match request.data.envelope {
            Some(env) => env,
            None => return Response::error("Missing envelope in decrypt request".into()),
        };

        // Get KEK
        let client = match KeysClient::new(&self.server_url) {
            Ok(c) => c,
            Err(e) => return Response::error(format!("Client error: {}", e)),
        };

        let key = match client.get_key(&envelope.key_id) {
            Ok(k) => k,
            Err(e) => return Response::error(format!("Failed to get key: {}", e)),
        };

        let kek_bytes = match key.as_bytes() {
            Ok(b) => b,
            Err(e) => return Response::error(format!("Key decode error: {}", e)),
        };

        // Decrypt
        let algorithm = match Algorithm::from_str(&envelope.algorithm) {
            Ok(a) => a,
            Err(e) => return Response::error(format!("Invalid algorithm: {}", e)),
        };

        let encryptor = EnvelopeEncryptor::new(algorithm);
        match encryptor.decrypt(&envelope, &kek_bytes) {
            Ok(plaintext) => {
                let encoded = BASE64.encode(&plaintext);
                Response::success_decrypt(encoded)
            }
            Err(e) => Response::error(format!("Decryption failed: {}", e)),
        }
    }
}
