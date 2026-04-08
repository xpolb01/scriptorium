//! Top-level error type for `scriptorium-core`.
//!
//! Variants will grow as modules are added. The CLI wraps this with `miette`
//! for pretty rendering; library consumers can match on the variant.

use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("vault root does not exist: {0}")]
    VaultMissing(PathBuf),

    #[error("path is outside the vault root: {0}")]
    PathEscape(PathBuf),

    #[error("frontmatter parse failed in {path}: {message}")]
    Frontmatter { path: PathBuf, message: String },

    #[error("invalid wikilink: {0}")]
    InvalidWikilink(String),

    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}
