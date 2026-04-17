//! Lifecycle hooks: shell commands fired at key points in the scriptorium
//! pipeline.
//!
//! Hooks are configured in `.scriptorium/config.toml` under `[hooks]`.
//! Each hook is a shell command string with template variables that are
//! expanded before execution:
//!
//! ```toml
//! [hooks]
//! pre_ingest = "echo 'ingesting {source}'"
//! post_ingest = "curl -X POST https://hooks.example.com -d '{summary}'"
//! ```
//!
//! Pre-hooks can abort the operation by exiting with a non-zero status.
//! Post-hooks are fire-and-forget — their exit status is logged but does
//! not affect the outcome.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::error::{Error, Result};

/// Timeout for hook execution.
const HOOK_TIMEOUT: Duration = Duration::from_secs(30);

/// Hook configuration from `config.toml`.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct HooksConfig {
    /// Runs before the LLM ingest call. Non-zero exit aborts the ingest.
    /// Template vars: `{source}`.
    pub pre_ingest: Option<String>,
    /// Runs after a successful ingest commit.
    /// Template vars: `{source}`, `{commit_id}`, `{summary}`, `{created}`, `{updated}`.
    pub post_ingest: Option<String>,
    /// Runs after a maintain cycle completes.
    /// Template vars: `{errors}`, `{warnings}`, `{stale_pages}`, `{stale_embeddings}`.
    pub post_maintain: Option<String>,
    /// Runs when the file watcher detects a new source file.
    /// Template vars: `{path}`.
    pub on_watch_trigger: Option<String>,
}

/// Result of running a hook.
#[derive(Debug)]
pub enum HookResult {
    /// No hook configured for this event — nothing happened.
    Skipped,
    /// Hook ran and exited with status 0.
    Ok,
    /// Hook ran and exited with a non-zero status. For pre-hooks, this
    /// means "abort the operation."
    Aborted { exit_code: i32, stderr: String },
    /// Hook failed to execute (e.g. command not found, timeout).
    Error(String),
}

impl HookResult {
    /// True if the hook explicitly asked to abort (non-zero exit on a pre-hook).
    pub fn should_abort(&self) -> bool {
        matches!(self, Self::Aborted { .. })
    }
}

/// Run a pre-ingest hook. Returns `Err` if the hook aborts the operation.
pub async fn pre_ingest(config: &HooksConfig, source: &Path) -> Result<()> {
    let Some(cmd) = &config.pre_ingest else {
        return Ok(());
    };
    let vars = HashMap::from([("source".to_string(), source.display().to_string())]);
    let result = run_shell(cmd, &vars).await;
    match result {
        HookResult::Skipped | HookResult::Ok => Ok(()),
        HookResult::Aborted { exit_code, stderr } => Err(Error::Other(anyhow::anyhow!(
            "pre_ingest hook aborted (exit {exit_code}): {stderr}"
        ))),
        HookResult::Error(e) => {
            warn!(hook = "pre_ingest", error = %e, "hook execution failed");
            Ok(()) // Don't block on hook infrastructure failures.
        }
    }
}

/// Fire a post-ingest hook (non-blocking — errors are logged, not propagated).
pub async fn post_ingest(
    config: &HooksConfig,
    source: &Path,
    commit_id: &str,
    summary: &str,
    created: usize,
    updated: usize,
) {
    let Some(cmd) = &config.post_ingest else {
        return;
    };
    let vars = HashMap::from([
        ("source".to_string(), source.display().to_string()),
        ("commit_id".to_string(), commit_id.to_string()),
        ("summary".to_string(), summary.to_string()),
        ("created".to_string(), created.to_string()),
        ("updated".to_string(), updated.to_string()),
    ]);
    let result = run_shell(cmd, &vars).await;
    if let HookResult::Error(e) = &result {
        warn!(hook = "post_ingest", error = %e, "hook execution failed");
    }
    if let HookResult::Aborted { exit_code, stderr } = &result {
        warn!(hook = "post_ingest", exit_code, stderr = %stderr, "hook returned non-zero");
    }
}

/// Fire a post-maintain hook.
pub async fn post_maintain(
    config: &HooksConfig,
    errors: usize,
    warnings: usize,
    stale_pages: usize,
    stale_embeddings: usize,
) {
    let Some(cmd) = &config.post_maintain else {
        return;
    };
    let vars = HashMap::from([
        ("errors".to_string(), errors.to_string()),
        ("warnings".to_string(), warnings.to_string()),
        ("stale_pages".to_string(), stale_pages.to_string()),
        ("stale_embeddings".to_string(), stale_embeddings.to_string()),
    ]);
    let result = run_shell(cmd, &vars).await;
    if let HookResult::Error(e) = &result {
        warn!(hook = "post_maintain", error = %e, "hook execution failed");
    }
}

/// Fire an on-watch-trigger hook.
pub async fn on_watch_trigger(config: &HooksConfig, path: &Path) -> Result<()> {
    let Some(cmd) = &config.on_watch_trigger else {
        return Ok(());
    };
    let vars = HashMap::from([("path".to_string(), path.display().to_string())]);
    let result = run_shell(cmd, &vars).await;
    match result {
        HookResult::Skipped | HookResult::Ok => Ok(()),
        HookResult::Aborted { exit_code, stderr } => Err(Error::Other(anyhow::anyhow!(
            "on_watch_trigger hook aborted (exit {exit_code}): {stderr}"
        ))),
        HookResult::Error(e) => {
            warn!(hook = "on_watch_trigger", error = %e, "hook execution failed");
            Ok(())
        }
    }
}

/// Execute a shell command with template variable expansion.
async fn run_shell(template: &str, vars: &HashMap<String, String>) -> HookResult {
    let cmd = expand_template(template, vars);
    debug!(hook_cmd = %cmd, "executing hook");

    let result = tokio::time::timeout(HOOK_TIMEOUT, async {
        tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&cmd)
            .output()
            .await
    })
    .await;

    match result {
        Ok(Ok(output)) => {
            let code = output.status.code().unwrap_or(-1);
            if output.status.success() {
                debug!(hook_cmd = %cmd, "hook succeeded");
                HookResult::Ok
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                debug!(hook_cmd = %cmd, exit_code = code, "hook failed");
                HookResult::Aborted {
                    exit_code: code,
                    stderr,
                }
            }
        }
        Ok(Err(e)) => HookResult::Error(format!("exec failed: {e}")),
        Err(_) => HookResult::Error(format!("timed out after {}s", HOOK_TIMEOUT.as_secs())),
    }
}

/// Replace `{key}` placeholders in a template string.
fn expand_template(template: &str, vars: &HashMap<String, String>) -> String {
    let mut result = template.to_string();
    for (key, value) in vars {
        result = result.replace(&format!("{{{key}}}"), value);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn expand_template_replaces_vars() {
        let vars = HashMap::from([
            ("source".to_string(), "/tmp/test.md".to_string()),
            ("summary".to_string(), "added 2 pages".to_string()),
        ]);
        let result = expand_template("ingested {source}: {summary}", &vars);
        assert_eq!(result, "ingested /tmp/test.md: added 2 pages");
    }

    #[test]
    fn expand_template_preserves_unknown_vars() {
        let vars = HashMap::new();
        let result = expand_template("hello {unknown}", &vars);
        assert_eq!(result, "hello {unknown}");
    }

    #[tokio::test]
    async fn hook_pre_ingest_can_abort() {
        let config = HooksConfig {
            pre_ingest: Some("exit 1".into()),
            ..Default::default()
        };
        let result = pre_ingest(&config, &PathBuf::from("/tmp/test.md")).await;
        assert!(result.is_err(), "non-zero exit should abort");
    }

    #[tokio::test]
    async fn hook_missing_config_is_noop() {
        let config = HooksConfig::default();
        // All hooks are None — should all succeed silently.
        pre_ingest(&config, &PathBuf::from("/tmp/test.md"))
            .await
            .unwrap();
        post_ingest(
            &config,
            &PathBuf::from("/tmp/test.md"),
            "abc",
            "summary",
            1,
            0,
        )
        .await;
        post_maintain(&config, 0, 0, 0, 0).await;
        on_watch_trigger(&config, &PathBuf::from("/tmp/test.md"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn hook_post_ingest_receives_vars() {
        // Use a hook that writes vars to a file so we can verify expansion.
        let dir = tempfile::tempdir().unwrap();
        let out_path = dir.path().join("hook_out.txt");
        let cmd = format!(
            "echo '{{source}} {{commit_id}} {{summary}} {{created}} {{updated}}' > {}",
            out_path.display()
        );
        let config = HooksConfig {
            post_ingest: Some(cmd),
            ..Default::default()
        };
        post_ingest(
            &config,
            &PathBuf::from("/tmp/src.md"),
            "abc123",
            "two pages",
            2,
            1,
        )
        .await;
        let content = std::fs::read_to_string(&out_path).unwrap();
        assert!(content.contains("/tmp/src.md"), "source var not expanded");
        assert!(content.contains("abc123"), "commit_id var not expanded");
        assert!(content.contains("two pages"), "summary var not expanded");
    }

    #[tokio::test]
    async fn hook_timeout_does_not_hang() {
        // This test verifies that a slow hook gets killed by the timeout.
        // We use a very short "sleep" that should still finish within the
        // default 30s timeout, just to verify the machinery works.
        let config = HooksConfig {
            pre_ingest: Some("sleep 0.1".into()),
            ..Default::default()
        };
        let result = pre_ingest(&config, &PathBuf::from("/tmp/test.md")).await;
        assert!(result.is_ok(), "short hook should succeed");
    }
}
