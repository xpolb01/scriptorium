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

    /// A structured reply that is not one complete JSON value of the
    /// requested shape. `reply` is kept for failure records but never
    /// displayed or debug-printed: it can quote vault text.
    #[error("structured reply rejected: {reason}")]
    InvalidStructuredReply {
        reason: String,
        reply: RedactedReply,
        usage: super::Usage,
        model: String,
    },

    #[error("response could not be parsed: {0}")]
    InvalidResponse(String),

    #[error(
        "{provider} truncated the response at the max_tokens ceiling \
         ({output_tokens}/{max_tokens}); the structured output is incomplete \
         and cannot be parsed. Raise max_tokens on the CompletionRequest or \
         shorten the source."
    )]
    Truncated {
        provider: String,
        output_tokens: u64,
        max_tokens: u32,
    },

    #[error("operation not supported by {provider}: {operation}")]
    Unsupported { provider: String, operation: String },

    #[error("mock provider: no handler produced a response for this request")]
    MockUnhandled,

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Model output carried for diagnostics only: `Debug` and `Display` show its
/// size, never its text. Read it with [`RedactedReply::expose`].
#[derive(Clone, Default, PartialEq, Eq)]
pub struct RedactedReply(String);

impl RedactedReply {
    pub fn new(text: impl Into<String>) -> Self {
        Self(text.into())
    }

    /// The reply text, for writing a failure record inside the vault.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for RedactedReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{} bytes redacted>", self.0.len())
    }
}

impl std::fmt::Display for RedactedReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{} bytes redacted>", self.0.len())
    }
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
