pub mod crypto;
pub mod error;
pub mod models;

// Re-export commonly used types
pub use error::{Result, VioletError};
pub use models::encryption_envelope::EncryptionEnvelope;
pub use crypto::envelope::EnvelopeEncryptor;
pub use crypto::types::Algorithm;
