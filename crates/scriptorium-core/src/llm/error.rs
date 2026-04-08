//! Errors produced by [`LlmProvider`](super::LlmProvider) implementations.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum LlmError {
    #[error("network error calling {provider}: {message}")]
    Network { provider: String, message: String },

    #[error("{provider} returned {status}: {message}")]
    Api {
        provider: String,
        status: u16,
        message: String,
    },

    #[error("{provider} request timed out after {seconds}s")]
    Timeout { provider: String, seconds: u64 },

    #[error("response did not validate against schema: {0}")]
    SchemaValidation(String),

    #[error("response could not be parsed: {0}")]
    InvalidResponse(String),

    #[error("operation not supported by {provider}: {operation}")]
    Unsupported { provider: String, operation: String },

    #[error("mock provider: no handler produced a response for this request")]
    MockUnhandled,

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl LlmError {
    pub fn network(provider: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Network {
            provider: provider.into(),
            message: message.into(),
        }
    }

    pub fn api(provider: impl Into<String>, status: u16, message: impl Into<String>) -> Self {
        Self::Api {
            provider: provider.into(),
            status,
            message: message.into(),
        }
    }

    pub fn unsupported(provider: impl Into<String>, operation: impl Into<String>) -> Self {
        Self::Unsupported {
            provider: provider.into(),
            operation: operation.into(),
        }
    }
}
