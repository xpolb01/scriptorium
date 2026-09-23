//! Error type for the ontology crate.

/// Errors raised while reading, composing, or serving an ontology.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("git: {0}")]
    Git(#[from] git2::Error),

    #[error("i/o error at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("revision `{rev}` could not be resolved: {message}")]
    Revision { rev: String, message: String },

    #[error("invalid {what} at {path}: {message}")]
    Invalid {
        what: &'static str,
        path: String,
        message: String,
    },

    #[error("store `{store}`: {message}")]
    Store { store: String, message: String },

    #[error("sub-ontology `{sub_ontology}` cannot be served: {message}")]
    Serve {
        sub_ontology: String,
        message: String,
    },
}

impl Error {
    pub fn io(path: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }

    pub fn serve(sub_ontology: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Serve {
            sub_ontology: sub_ontology.into(),
            message: message.into(),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
