//! macOS Keychain integration for API key storage and retrieval.
//!
//! Stores API keys in the user's login keychain via the `security` CLI
//! tool, which is available on every macOS installation. Falls back
//! gracefully on non-macOS platforms (returns `None`).
//!
//! Service names follow the pattern `scriptorium-<provider>` (e.g.
//! `scriptorium-anthropic`, `scriptorium-google`).

use std::process::Command;

/// Retrieve a key from the macOS keychain.
///
/// Returns `None` if: not on macOS, `security` CLI unavailable, key not
/// found, or any other error. Never panics or blocks on UI prompts
/// (uses `-w` flag which returns the password directly).
pub fn get_key(service: &str) -> Option<String> {
    if !cfg!(target_os = "macos") {
        return None;
    }
    let output = Command::new("security")
        .args(["find-generic-password", "-s", service, "-w"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let key = String::from_utf8(output.stdout).ok()?;
    let trimmed = key.trim().to_string();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

/// Store a key in the macOS keychain, creating or updating it.
///
/// Returns `true` on success, `false` on failure or non-macOS.
pub fn set_key(service: &str, key: &str) -> bool {
    if !cfg!(target_os = "macos") {
        return false;
    }
    let user = std::env::var("USER").unwrap_or_else(|_| "scriptorium".into());
    // -U flag updates if exists, creates if not.
    let status = Command::new("security")
        .args([
            "add-generic-password",
            "-a",
            &user,
            "-s",
            service,
            "-w",
            key,
            "-U",
        ])
        .status();
    matches!(status, Ok(s) if s.success())
}

/// Delete a key from the macOS keychain.
///
/// Returns `true` if deleted, `false` if not found or non-macOS.
pub fn delete_key(service: &str) -> bool {
    if !cfg!(target_os = "macos") {
        return false;
    }
    let status = Command::new("security")
        .args(["delete-generic-password", "-s", service])
        .status();
    matches!(status, Ok(s) if s.success())
}

/// Keychain service names for each provider.
pub mod services {
    pub const ANTHROPIC: &str = "scriptorium-anthropic";
    pub const OPENAI: &str = "scriptorium-openai";
    pub const GOOGLE: &str = "scriptorium-google";
}

/// Try to resolve an API key: env var first, then keychain.
pub fn resolve_key(env_var: &str, keychain_service: &str) -> Option<String> {
    // 1. Environment variable (highest priority).
    if let Ok(val) = std::env::var(env_var) {
        if !val.is_empty() {
            return Some(val);
        }
    }
    // 2. macOS Keychain fallback.
    get_key(keychain_service)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_key_prefers_env_var() {
        // Set an env var and verify it takes priority.
        std::env::set_var("SCRIPTORIUM_TEST_KEY_QA", "from-env");
        let result = resolve_key("SCRIPTORIUM_TEST_KEY_QA", "nonexistent-service");
        assert_eq!(result.as_deref(), Some("from-env"));
        std::env::remove_var("SCRIPTORIUM_TEST_KEY_QA");
    }

    #[test]
    fn resolve_key_returns_none_for_missing() {
        std::env::remove_var("SCRIPTORIUM_TEST_MISSING_KEY");
        let result = resolve_key("SCRIPTORIUM_TEST_MISSING_KEY", "nonexistent-service-xyz");
        // On macOS without the key in keychain, this returns None.
        // On non-macOS, keychain always returns None.
        assert!(result.is_none());
    }
}
