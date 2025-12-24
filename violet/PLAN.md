# Violet Implementation Plan

## Step 1: Setup Workspace
- [x] Create workspace Cargo.toml with 4 members
- [x] Create directory structure for all crates
- [x] Add workspace.dependencies to Cargo.toml
- [x] Create basic .gitignore

## Step 2: Implement Core Crypto (violet-core)
- [x] Create violet-core/Cargo.toml with dependencies
- [x] Implement crypto/types.rs (Algorithm enum, constants)
- [x] Implement error.rs (VioletError with thiserror)
- [x] Implement models/encryption_envelope.rs (with serde)
- [x] Implement crypto/aes_gcm.rs (encrypt/decrypt functions)
- [x] Implement crypto/aes_gcm_siv.rs (encrypt/decrypt functions)
- [x] Implement crypto/envelope.rs (EnvelopeEncryptor)
- [x] Write unit tests for each module
- [x] Run `cargo test` in violet-core (19 tests passed!)

## Step 3: Implement Keys Client (violet-client)
- [x] Create violet-client/Cargo.toml with dependencies
- [x] Implement models.rs (Key struct with hex decoding)
- [x] Implement error.rs (ClientError)
- [x] Implement client.rs (KeysClient with create_key and get_key)
- [x] Write tests (6 passed, 2 integration tests #[ignore])
- [x] Write integration tests (marked #[ignore])

## Step 4: Implement CLI (violet-cli)
- [x] Create violet-cli/Cargo.toml with dependencies
- [x] Implement main.rs with clap (CLI structure, logging setup)
- [x] Implement commands/encrypt.rs
- [x] Implement commands/decrypt.rs
- [x] CLI compiles and builds successfully

## Step 5: Implement Daemon (violet-daemon)
- [x] Create violet-daemon/Cargo.toml with dependencies
- [x] Implement protocol.rs (Request, Response types)
- [x] Implement handler.rs (RequestHandler)
- [x] Implement server.rs (DaemonServer with UnixListener)
- [x] Implement commands/daemon.rs in violet-cli
- [x] Daemon compiles and builds successfully

## Step 6: Testing & Documentation
- [x] Create comprehensive README.md with usage examples
- [x] Add cargo doc comments to public APIs
- [x] All unit tests passing (19 tests in violet-core, 6 in violet-client)
