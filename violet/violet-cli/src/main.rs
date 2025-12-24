use clap::{Parser, Subcommand};
use violet_core::Algorithm;
use anyhow::Result;

mod commands;

#[derive(Parser)]
#[command(name = "violet")]
#[command(about = "Envelope encryption CLI using the Keys server", long_about = None)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Keys server base URL
    #[arg(long, env = "VIOLET_SERVER_URL", default_value = "http://localhost:8080")]
    server_url: String,

    /// Logging level
    #[arg(long, env = "VIOLET_LOG_LEVEL", default_value = "info")]
    log_level: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Encrypt plaintext data
    Encrypt {
        /// Input file (use '-' for stdin)
        #[arg(short, long, default_value = "-")]
        input: String,

        /// Output file (use '-' for stdout)
        #[arg(short, long, default_value = "-")]
        output: String,

        /// Key ID to use (if not provided, creates new key)
        #[arg(short, long)]
        key_id: Option<String>,

        /// Algorithm to use
        #[arg(short, long, value_enum, default_value = "aes-256-gcm")]
        algorithm: AlgorithmArg,
    },

    /// Decrypt encrypted envelope
    Decrypt {
        /// Input envelope JSON file (use '-' for stdin)
        #[arg(short, long, default_value = "-")]
        input: String,

        /// Output file for plaintext (use '-' for stdout)
        #[arg(short, long, default_value = "-")]
        output: String,
    },

    /// Run as Unix socket daemon
    Daemon {
        /// Socket path
        #[arg(short, long, env = "VIOLET_SOCKET_PATH", default_value = "/tmp/violet.sock")]
        socket: String,
    },
}

#[derive(clap::ValueEnum, Clone, Copy)]
enum AlgorithmArg {
    #[value(name = "aes-256-gcm")]
    Aes256Gcm,
    #[value(name = "aes-256-gcm-siv")]
    Aes256GcmSiv,
}

impl From<AlgorithmArg> for Algorithm {
    fn from(arg: AlgorithmArg) -> Self {
        match arg {
            AlgorithmArg::Aes256Gcm => Algorithm::Aes256Gcm,
            AlgorithmArg::Aes256GcmSiv => Algorithm::Aes256GcmSiv,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(&cli.log_level)
        .init();

    tracing::info!("Violet CLI starting");

    match cli.command {
        Commands::Encrypt { input, output, key_id, algorithm } => {
            commands::encrypt::execute(
                &cli.server_url,
                &input,
                &output,
                key_id.as_deref(),
                algorithm.into(),
            ).await?;
        }
        Commands::Decrypt { input, output } => {
            commands::decrypt::execute(&cli.server_url, &input, &output).await?;
        }
        Commands::Daemon { socket } => {
            commands::daemon::execute(&cli.server_url, &socket).await?;
        }
    }

    Ok(())
}
