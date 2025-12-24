use tokio::net::{UnixListener, UnixStream};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use std::path::Path;
use anyhow::Result;
use crate::handler::RequestHandler;
use crate::protocol::{Request, Response};

pub struct DaemonServer {
    socket_path: String,
    server_url: String,
}

impl DaemonServer {
    pub fn new(socket_path: String, server_url: String) -> Self {
        Self {
            socket_path,
            server_url,
        }
    }

    pub async fn run(&self) -> Result<()> {
        // Remove existing socket if present
        let path = Path::new(&self.socket_path);
        if path.exists() {
            std::fs::remove_file(path)?;
        }

        let listener = UnixListener::bind(&self.socket_path)?;
        tracing::info!("Daemon listening on {}", self.socket_path);

        // Handle Ctrl+C for graceful shutdown
        let socket_path_clone = self.socket_path.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            tracing::info!("Shutting down...");
            let _ = std::fs::remove_file(&socket_path_clone);
            std::process::exit(0);
        });

        loop {
            let (stream, _) = listener.accept().await?;
            let server_url = self.server_url.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, server_url).await {
                    tracing::error!("Connection handler error: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(stream: UnixStream, server_url: String) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    while reader.read_line(&mut line).await? > 0 {
        let request: Request = match serde_json::from_str(&line) {
            Ok(req) => req,
            Err(e) => {
                let error_response = Response::error(format!("Invalid request: {}", e));
                let json = serde_json::to_string(&error_response)?;
                writer.write_all(json.as_bytes()).await?;
                writer.write_all(b"\n").await?;
                line.clear();
                continue;
            }
        };

        let handler = RequestHandler::new(&server_url);
        let response = handler.handle(request).await;

        let json = serde_json::to_string(&response)?;
        writer.write_all(json.as_bytes()).await?;
        writer.write_all(b"\n").await?;

        line.clear();
    }

    Ok(())
}
