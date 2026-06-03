//! `rhss` CLI entry point.
//!
//! All command surface lives under `src/cli/`. This file is just the clap
//! dispatcher.

use clap::Parser;
use tracing::error;
use tracing_subscriber::{fmt, EnvFilter};

use rhss::cli;

fn main() {
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .with_ansi(true)
        .init();

    let parsed = cli::Cli::parse();
    if let Err(e) = cli::run(parsed) {
        error!("{e}");
        std::process::exit(1);
    }
}
