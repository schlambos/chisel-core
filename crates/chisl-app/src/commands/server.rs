//! `chislcore` (no subcommand): the main HTTP server.

use std::process::ExitCode;
use std::time::Instant;

use anyhow::Result;
use tokio::net::TcpListener;
use tracing::{info, warn};

use chisl_app::{AppServices, create_router};

use crate::bootstrap::ServerEnvironment;

/// Start the HTTP server with fully constructed services.
pub async fn run_server(env: ServerEnvironment, services: AppServices) -> Result<ExitCode> {
    let boot = Instant::now();

    let has_users = services.user_repo.has_users().await?;
    if !has_users {
        info!("No configured users detected — initial setup required via /api/auth/status");
    }

    let router = create_router(&services).await;
    let addr = env.config.socket_addr();
    let listener = TcpListener::bind(&addr).await?;
    if !listener.local_addr()?.ip().is_loopback() {
        warn!(
            %addr,
            "AionCore is binding to a non-loopback address. \
             The entire API (including the sidecar proxy) will be reachable \
             from the network. Use --host 127.0.0.1 for local-only access."
        );
    }
    info!(elapsed_ms = boot.elapsed().as_millis(), "Server listening on {addr}");

    // Kick off the idle-ACP-agent reaper. `start_idle_scanner` returns
    // immediately with a `JoinHandle`; the scanner task polls every 60 s
    // and kills ACP agents whose `status == Finished` + last_activity
    // exceeds the default 5-minute idle threshold. The watch channel
    // propagates graceful-shutdown so the scanner exits on SIGINT/SIGTERM.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let idle_scanner_handle =
        chisl_ai_agent::start_idle_scanner(services.worker_task_manager.clone(), shutdown_rx, None, None);

    let lsp_service = services.lsp_service.clone();

    let app = router.into_make_service_with_connect_info::<std::net::SocketAddr>();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            let _ = shutdown_tx.send(true);
        })
        .await?;

    // Gracefully tear down all LSP sessions (sends kill signals to running
    // language-server children, clears the session map). Placed alongside
    // the other child-reaping calls so LSP processes are torn down during
    // graceful shutdown rather than left for `kill_on_drop` backstop.
    lsp_service.shutdown_all().await;

    // Reap any background processes the OpenCode plugin may
    // have started. This is a backstop — the per-conversation
    // close path (`manager/remote/agent.rs`) already kills
    // processes owned by closing conversations, and
    // `Builder::clean_cli` sets `kill_on_drop(true)` so a
    // dropped monitor task SIGKILLs its child. We call this
    // before closing the DB to ensure the audit log captures
    // any final `bg.stop` records.
    let killed = chisl_ai_agent::manager::remote::plugin::bg::kill_all_bg_processes().await;
    if killed > 0 {
        info!(killed, "background processes killed on graceful shutdown");
    }

    // Kill any local `opencode serve` instances spawned by the
    // Phase 4 process manager. Each instance's Builder set
    // `kill_on_drop(true)` so this is technically a backstop
    // too, but doing it explicitly lets us log the count and
    // makes the shutdown path easier to reason about.
    let local_killed = chisl_ai_agent::manager::local_opencode::kill_all_local_opencode().await;
    if local_killed > 0 {
        info!(local_killed, "local OpenCode instances killed on graceful shutdown");
    }

    // Wait for the scanner to observe the shutdown watch value and
    // return; at worst this blocks for the current 60 s tick.
    if let Err(e) = idle_scanner_handle.await {
        warn!(error = %e, "idle scanner join failed");
    }

    services.database.close().await;
    info!("Server shut down gracefully");

    // Prevent the log guard from being dropped before final log flush.
    drop(env);

    Ok(ExitCode::SUCCESS)
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {
            info!("Received SIGINT, shutting down...");
        }
        () = terminate => {
            info!("Received SIGTERM, shutting down...");
        }
    }
}
