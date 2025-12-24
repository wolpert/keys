use serde::{Deserialize, Serialize};
use violet_core::{Algorithm, EncryptionEnvelope};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Encrypt,
    Decrypt,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub operation: Operation,
    pub data: RequestData,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestData {
    // Encrypt fields
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub plaintext: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<Algorithm>,

    // Decrypt fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub envelope: Option<EncryptionEnvelope>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub success: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<ResponseResult>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ResponseResult {
    Encrypt { envelope: EncryptionEnvelope },
    Decrypt { plaintext: String },
}

impl Response {
    pub fn success_encrypt(envelope: EncryptionEnvelope) -> Self {
        Self {
            success: true,
            result: Some(ResponseResult::Encrypt { envelope }),
            error: None,
        }
    }

    pub fn success_decrypt(plaintext: String) -> Self {
        Self {
            success: true,
            result: Some(ResponseResult::Decrypt { plaintext }),
            error: None,
        }
    }

    pub fn error(message: String) -> Self {
        Self {
            success: false,
            result: None,
            error: Some(message),
        }
    }
}
