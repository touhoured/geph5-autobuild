//! `geph` — a Mullvad-like command-line client for Geph5.
//!
//! `geph manager` runs the privileged supervisor that owns a child `geph5-client`
//! process; all other subcommands are thin clients that talk to it over loopback
//! TCP.

mod cli;
mod client;
mod manager;
mod platform;
mod supervisor;
mod vpn;

use clap::Parser;

use crate::cli::{Cli, Command};

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Manager => run_manager_command(),
        // Background (un)registration runs directly, without contacting the
        // manager; it installs the platform autostart service, so it needs
        // root/Administrator like the manager.
        Command::RegisterManager => {
            platform::require_manager_privilege()?;
            platform::register_manager()
        }
        Command::UnregisterManager => {
            platform::require_manager_privilege()?;
            platform::unregister_manager()
        }
        // Internal helper: the manager re-invokes us dropped to the desktop user
        // to apply proxy settings. Runs directly, without contacting the manager.
        Command::ApplyProxy { mode, url } => {
            let connected = matches!(mode.as_str(), "on" | "true" | "1");
            platform::apply_proxy_in_process(connected, url.as_deref().unwrap_or_default())
        }
        other => client::run(other),
    }
}

fn run_manager_command() -> anyhow::Result<()> {
    init_manager_logging();
    std::panic::set_hook(Box::new(|panic_info| {
        tracing::error!(panic = ?panic_info, "geph manager panicked");
    }));

    let result = (|| {
        platform::require_manager_privilege()?;
        geph5_rt::block_on(manager::run_manager())
    })();
    if let Err(error) = &result {
        tracing::error!(error = %format!("{error:#}"), "geph manager exited with an error");
    }
    result
}

fn init_manager_logging() {
    #[cfg(target_os = "windows")]
    match windows_manager_log_writer() {
        Ok(writer) => {
            let _ = tracing_subscriber::fmt()
                .with_ansi(false)
                .with_env_filter(manager_env_filter())
                .with_writer(writer)
                .try_init();
            return;
        }
        Err(error) => {
            eprintln!("could not open the Geph manager log: {error}");
        }
    }

    let _ = tracing_subscriber::fmt()
        .with_env_filter(manager_env_filter())
        .try_init();
}

fn manager_env_filter() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("geph=debug,warn"))
}

#[cfg(target_os = "windows")]
fn windows_manager_log_writer() -> std::io::Result<std::sync::Mutex<std::fs::File>> {
    use std::fs::OpenOptions;

    const MAX_LOG_BYTES: u64 = 8 * 1024 * 1024;

    let path = platform::manager_log_path();
    std::fs::create_dir_all(
        path.parent()
            .expect("the Windows manager log path has a parent"),
    )?;
    if std::fs::metadata(&path)
        .map(|metadata| metadata.len() >= MAX_LOG_BYTES)
        .unwrap_or(false)
    {
        let previous = path.with_extension("log.old");
        match std::fs::remove_file(&previous) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => eprintln!("could not remove {}: {error}", previous.display()),
        }
        if let Err(error) = std::fs::rename(&path, &previous) {
            eprintln!("could not rotate {}: {error}", path.display());
        }
    }
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    Ok(std::sync::Mutex::new(file))
}
