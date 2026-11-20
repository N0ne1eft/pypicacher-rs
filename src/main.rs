use std::sync::Arc;

use anyhow::{Context, Result};
use axum::serve::ListenerExt;
use clap::Parser;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::info;
use tracing_subscriber::EnvFilter;

use pypicacher_rs::config::{CliArgs, Config};
use pypicacher_rs::server::{AppState, build_router};

#[tokio::main]
async fn main() -> Result<()> {
    let args = CliArgs::parse();
    let config = Config::from_cli(args)?;

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config.log_level.clone()));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .init();

    let listen_addr = config.listen;
    let state = AppState::build(config).await?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let flusher = state.cache.spawn_background_flusher(shutdown_rx);

    let router = build_router(Arc::clone(&state));
    let listener = TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("failed to bind {listen_addr}"))?
        .tap_io(|tcp_stream| {
            let _ = tcp_stream.set_nodelay(true);
        });

    info!(
        listen = %listen_addr,
        upstream = %state.config.upstream,
        max_cache_size = state.config.max_cache_size,
        cache_dir = %state.config.cache_dir.display(),
        "pypicacher-rs listening"
    );

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("HTTP server exited with error")?;

    info!("shutting down; flushing cached access times to disk");
    let _ = shutdown_tx.send(true);
    let _ = flusher.await;
    info!("shutdown complete");

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sig.recv().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
