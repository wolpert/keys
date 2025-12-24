use anyhow::Result;
use violet_daemon::DaemonServer;

pub async fn execute(
    server_url: &str,
    socket: &str,
) -> Result<()> {
    tracing::info!("Starting Violet daemon on socket: {}", socket);
    tracing::info!("Keys server: {}", server_url);

    let server = DaemonServer::new(socket.to_string(), server_url.to_string());
    server.run().await?;

    Ok(())
}
