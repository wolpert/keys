# Violet

Envelope encryption tool that integrates with the Keys server for cryptographic key management.

## Overview

Violet implements envelope encryption with two-layer security:
1. **Data Encryption**: Your data is encrypted with a random 256-bit Data Encryption Key (DEK)
2. **Key Encryption**: The DEK is encrypted with a master key (KEK) from the Keys server

This architecture provides:
- Secure key management via the Keys server
- Support for key rotation without re-encrypting data
- Multiple encryption algorithms (AES-256-GCM and AES-256-GCM-SIV)
- Both CLI and daemon modes for flexibility

## Installation

```bash
cargo build --release
```

The binary will be at `target/release/violet`.

## Prerequisites

The Keys server must be running. Start it with:

```bash
cd /home/wolpert/projects/keys
./gradlew :keys-server:run --args="server config.yml"
```

Default server URL: `http://localhost:8080`

## Usage

### CLI Mode

#### Encrypt Data

```bash
# Encrypt from stdin, output to stdout
echo "secret data" | violet encrypt > envelope.json

# Encrypt a file
violet encrypt -i plaintext.txt -o envelope.json

# Use a specific key (instead of creating a new one)
violet encrypt -i file.txt -o envelope.json -k existing-key-uuid

# Use AES-256-GCM-SIV algorithm
violet encrypt -i file.txt -o envelope.json --algorithm aes-256-gcm-siv
```

#### Decrypt Data

```bash
# Decrypt from stdin
cat envelope.json | violet decrypt > plaintext.txt

# Decrypt from file
violet decrypt -i envelope.json -o plaintext.txt
```

#### Full Example

```bash
# Start the Keys server (in another terminal)
cd /home/wolpert/projects/keys
./gradlew :keys-server:run --args="server config.yml"

# Encrypt some data
echo "Hello, World!" | ./target/release/violet encrypt > envelope.json

# Decrypt it back
./target/release/violet decrypt -i envelope.json
# Output: Hello, World!
```

### Daemon Mode

Run Violet as a Unix socket daemon for high-performance IPC:

```bash
# Start daemon
violet daemon --socket /tmp/violet.sock

# In another terminal, send encrypt request
echo '{"operation":"encrypt","data":{"plaintext":"SGVsbG8=","algorithm":"AES-256-GCM"}}' | nc -U /tmp/violet.sock

# Send decrypt request
echo '{"operation":"decrypt","data":{"envelope":{...}}}' | nc -U /tmp/violet.sock
```

## Configuration

Environment variables:

- `VIOLET_SERVER_URL`: Keys server URL (default: `http://localhost:8080`)
- `VIOLET_SOCKET_PATH`: Daemon socket path (default: `/tmp/violet.sock`)
- `VIOLET_LOG_LEVEL`: Logging level - `trace`, `debug`, `info`, `warn`, `error` (default: `info`)

Examples:

```bash
# Use different server
export VIOLET_SERVER_URL=http://prod-server:8080
violet encrypt -i file.txt -o envelope.json

# Enable debug logging
export VIOLET_LOG_LEVEL=debug
violet encrypt -i file.txt -o envelope.json
```

## Architecture

### Project Structure

```
violet/
├── violet-core/         # Crypto primitives and envelope encryption
├── violet-client/       # HTTP client for Keys server API
├── violet-cli/          # CLI application
└── violet-daemon/       # Unix socket daemon library
```

### Encryption Flow

1. **Encrypt**:
   - Generate random 256-bit DEK
   - Encrypt plaintext with DEK (using AES-GCM or AES-GCM-SIV)
   - Get KEK from Keys server (or create new one)
   - Encrypt DEK with KEK (using AES-256-GCM)
   - Return EncryptionEnvelope (JSON) with all components

2. **Decrypt**:
   - Parse EncryptionEnvelope JSON
   - Get KEK from Keys server using keyId
   - Decrypt DEK using KEK
   - Decrypt ciphertext using DEK
   - Return plaintext

### EncryptionEnvelope Format

```json
{
  "keyId": "uuid-of-master-key",
  "encryptedData": "base64-encoded-ciphertext",
  "encryptedKey": "base64-encoded-encrypted-dek",
  "iv": "base64-encoded-initialization-vector",
  "algorithm": "AES-256-GCM",
  "authTag": "base64-encoded-authentication-tag"
}
```

## Supported Algorithms

### AES-256-GCM (Default)

- **Use**: General-purpose authenticated encryption
- **Performance**: Excellent (hardware-accelerated on most CPUs)
- **Nonce**: Must be unique for each encryption
- **Tag**: 128-bit authentication tag

### AES-256-GCM-SIV

- **Use**: Nonce-misuse resistant variant
- **Performance**: Good (slightly slower than GCM)
- **Nonce**: Safer if nonces might be reused
- **Tag**: 128-bit authentication tag

Choose AES-GCM for most use cases. Use AES-GCM-SIV if nonce uniqueness cannot be guaranteed.

## Development

### Running Tests

```bash
# Test core crypto library
cargo test --package violet-core

# Test HTTP client
cargo test --package violet-client

# Run all tests
cargo test

# Run integration tests (requires Keys server running)
cargo test -- --ignored
```

### Building

```bash
# Debug build
cargo build

# Release build (optimized)
cargo build --release

# Build specific package
cargo build --package violet-core
```

## Security Notes

1. **Keys are never logged**: All key material is kept in memory only
2. **Fresh nonces**: New random nonce generated for each encryption
3. **Authentication**: All algorithms use authenticated encryption (AEAD)
4. **Secure RNG**: Uses OS random source via `rand::thread_rng()`
5. **Server communication**: Keys transmitted over HTTP (use HTTPS in production)

## Troubleshooting

### "Connection refused" error

Make sure the Keys server is running:
```bash
./gradlew :keys-server:run --args="server config.yml"
```

### "Key not found" error

The key UUID in the envelope doesn't exist on the server. This can happen if:
- The server was restarted (in-memory database)
- Wrong server URL
- Key was deleted

### Invalid hex decode errors

The Keys server returns keys as hex strings. Make sure you're using a compatible server version.

## Examples

### Encrypt Multiple Files

```bash
for file in *.txt; do
    violet encrypt -i "$file" -o "$file.encrypted.json"
done
```

### Use Same Key for Multiple Files

```bash
# Create a key and save its ID
KEY_ID=$(violet encrypt -i first.txt -o first.encrypted.json 2>&1 | grep "Created key:" | awk '{print $NF}')

# Reuse the key for other files
violet encrypt -i second.txt -o second.encrypted.json -k "$KEY_ID"
violet encrypt -i third.txt -o third.encrypted.json -k "$KEY_ID"
```

### Decrypt All Encrypted Files

```bash
for file in *.encrypted.json; do
    violet decrypt -i "$file" -o "${file%.encrypted.json}.decrypted.txt"
done
```

## License

Apache-2.0

## Contributing

This project is part of the keys server example demonstrating Dropwizard best practices.
