use anyhow::{Context, Result};
use std::io::{self, Read, Write};
use std::fs::File;
use violet_core::{EncryptionEnvelope, EnvelopeEncryptor, Algorithm};
use violet_client::KeysClient;

pub async fn execute(
    server_url: &str,
    input: &str,
    output: &str,
) -> Result<()> {
    // Read envelope JSON
    tracing::debug!("Reading envelope from: {}", input);
    let envelope_json = read_input(input)
        .context("Failed to read input")?;

    let envelope: EncryptionEnvelope = serde_json::from_slice(&envelope_json)
        .context("Failed to parse envelope JSON")?;

    tracing::info!("Decrypting envelope for key: {}", envelope.key_id);
    tracing::info!("Algorithm: {}", envelope.algorithm);

    // Get KEK from server
    let client = KeysClient::new(server_url)
        .context("Failed to create Keys client")?;

    let key = client.get_key(&envelope.key_id)
        .context("Failed to get key from server")?;

    let kek_bytes = key.as_bytes()
        .context("Failed to decode key")?;

    // Decrypt
    let algorithm = Algorithm::from_str(&envelope.algorithm)
        .context("Invalid algorithm in envelope")?;
    let encryptor = EnvelopeEncryptor::new(algorithm);

    let plaintext = encryptor.decrypt(&envelope, &kek_bytes)
        .context("Decryption failed")?;

    tracing::info!("Decrypted {} bytes of plaintext", plaintext.len());

    // Write output
    tracing::debug!("Writing plaintext to: {}", output);
    write_output(output, &plaintext)
        .context("Failed to write output")?;

    tracing::info!("Decryption successful");
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
