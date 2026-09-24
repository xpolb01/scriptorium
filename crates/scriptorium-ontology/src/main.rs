//! `scriptorium-ontology`: git-backed federated ontology (experimental).
//!
//! A separate binary (and Cargo workspace) from `scriptorium`, so the
//! federation dependencies never change how the main crates are built.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use scriptorium_ontology::cli::{self, OntologyAction, OntologyCommand};

#[derive(Debug, Parser)]
#[command(
    name = "scriptorium-ontology",
    version,
    about = "Git-backed federated ontology (experimental): sub-ontologies as GraphQL federation subgraphs"
)]
struct Cli {
    /// Repository root (default: the git repository containing the current directory).
    #[arg(short = 'C', long = "repo", global = true, value_name = "PATH")]
    repo: Option<PathBuf>,

    #[command(subcommand)]
    action: OntologyAction,
}

#[tokio::main]
async fn main() -> ExitCode {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_writer(std::io::stderr)
        .try_init();
    let cli = Cli::parse();
    let result = async {
        let root = cli::repo_root(cli.repo)?;
        cli::run(OntologyCommand { action: cli.action }, &root).await
    }
    .await;
    match result {
        Ok(code) => code,
        Err(err) => {
            eprintln!("{err:?}");
            ExitCode::from(2)
        }
    }
}
