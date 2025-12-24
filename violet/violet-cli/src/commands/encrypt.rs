use anyhow::{Context, Result};
use std::io::{self, Read, Write};
use std::fs::File;
use violet_core::{Algorithm, EnvelopeEncryptor};
use violet_client::KeysClient;

pub async fn execute(
    server_url: &str,
    input: &str,
    output: &str,
    key_id: Option<&str>,
    algorithm: Algorithm,
) -> Result<()> {
    // Read input
    tracing::debug!("Reading plaintext from: {}", input);
    let plaintext = read_input(input)
        .context("Failed to read input")?;

    tracing::info!("Read {} bytes of plaintext", plaintext.len());

    // Create Keys client
    let client = KeysClient::new(server_url)
        .context("Failed to create Keys client")?;

    // Get or create key
    let (kek_id, kek_bytes) = if let Some(kid) = key_id {
        // Use existing key
        tracing::info!("Using existing key: {}", kid);
        let key = client.get_key(kid)
            .context("Failed to get key from server")?;
        let bytes = key.as_bytes()
            .context("Failed to decode key")?;
        (key.uuid, bytes)
    } else {
        // Create new key
        tracing::info!("Creating new key on server");
        let key = client.create_key()
            .context("Failed to create new key")?;
        let bytes = key.as_bytes()
            .context("Failed to decode key")?;
        tracing::info!("Created new key: {}", key.uuid);
        (key.uuid, bytes)
    };

    // Encrypt
    tracing::info!("Encrypting with algorithm: {}", algorithm.as_str());
    let encryptor = EnvelopeEncryptor::new(algorithm);
    let envelope = encryptor.encrypt(&plaintext, &kek_bytes, kek_id)
        .context("Encryption failed")?;

    // Serialize to JSON
    let json = serde_json::to_string_pretty(&envelope)
        .context("Failed to serialize envelope")?;

    // Write output
    tracing::debug!("Writing envelope to: {}", output);
    write_output(output, json.as_bytes())
        .context("Failed to write output")?;

    tracing::info!("Encryption successful");
    Ok(())
}

fn read_input(path: &str) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    if path == "-" {
        tracing::debug!("Reading from stdin");
        io::stdin().read_to_end(&mut buffer)?;
    } else {
        tracing::debug!("Reading from file: {}", path);
        File::open(path)?.read_to_end(&mut buffer)?;
    }
    Ok(buffer)
}

fn write_output(path: &str, data: &[u8]) -> Result<()> {
    if path == "-" {
        tracing::debug!("Writing to stdout");
        io::stdout().write_all(data)?;
    } else {
        tracing::debug!("Writing to file: {}", path);
        File::create(path)?.write_all(data)?;
    }
    Ok(())
}
