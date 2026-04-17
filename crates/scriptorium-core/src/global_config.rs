//! Global configuration: multi-vault registry.
//!
//! Stored at `~/.config/scriptorium/config.toml` (or the platform-appropriate
//! config directory via the `directories` crate). Separate from per-vault
//! config at `.scriptorium/config.toml`.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

const APP_NAME: &str = "scriptorium";
const CONFIG_FILE: &str = "config.toml";

/// A registered vault entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VaultEntry {
    pub name: String,
    pub path: PathBuf,
}

/// Top-level global config: vault registry + default selection.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GlobalConfig {
    /// Name of the default vault (must match a name in `vaults`).
    pub default: Option<String>,
    /// Registered vaults.
    #[serde(default)]
    pub vaults: Vec<VaultEntry>,
}

impl GlobalConfig {
    /// Platform-appropriate config directory.
    pub fn config_dir() -> Option<PathBuf> {
        directories::ProjectDirs::from("", "", APP_NAME).map(|dirs| dirs.config_dir().to_path_buf())
    }

    /// Full path to the global config file.
    pub fn config_path() -> Option<PathBuf> {
        Self::config_dir().map(|d| d.join(CONFIG_FILE))
    }

    /// Load from the platform default path. Returns `Default` if the file
    /// does not exist.
    pub fn load() -> Result<Self> {
        let path = Self::config_path()
            .ok_or_else(|| Error::Other(anyhow::anyhow!("cannot determine config directory")))?;
        Self::load_from(&path)
    }

    /// Load from an explicit path (for testing).
    pub fn load_from(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => toml::from_str(&text)
                .map_err(|e| Error::Other(anyhow::anyhow!("parse global config: {e}"))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(Error::io(path, e)),
        }
    }

    /// Save to the platform default path, creating parent dirs.
    pub fn save(&self) -> Result<()> {
        let path = Self::config_path()
            .ok_or_else(|| Error::Other(anyhow::anyhow!("cannot determine config directory")))?;
        self.save_to(&path)
    }

    /// Save to an explicit path (for testing).
    pub fn save_to(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| Error::io(parent, e))?;
        }
        let text = toml::to_string_pretty(self)
            .map_err(|e| Error::Other(anyhow::anyhow!("serialize global config: {e}")))?;
        std::fs::write(path, text).map_err(|e| Error::io(path, e))
    }

    /// Look up a vault by name.
    pub fn find(&self, name: &str) -> Option<&VaultEntry> {
        self.vaults.iter().find(|v| v.name == name)
    }

    /// Get the default vault entry.
    pub fn default_vault(&self) -> Option<&VaultEntry> {
        self.default.as_ref().and_then(|name| self.find(name))
    }

    /// Add or update a vault entry. Returns `true` if it was an update.
    pub fn register(&mut self, name: String, path: PathBuf) -> bool {
        if let Some(existing) = self.vaults.iter_mut().find(|v| v.name == name) {
            existing.path = path;
            true
        } else {
            self.vaults.push(VaultEntry { name, path });
            false
        }
    }

    /// Remove a vault by name. Returns the removed entry, if any.
    pub fn unregister(&mut self, name: &str) -> Option<VaultEntry> {
        let pos = self.vaults.iter().position(|v| v.name == name)?;
        let removed = self.vaults.remove(pos);
        // Clear default if it pointed to the removed vault.
        if self.default.as_deref() == Some(name) {
            self.default = None;
        }
        Some(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_returns_default_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.toml");
        let cfg = GlobalConfig::load_from(&path).unwrap();
        assert!(cfg.default.is_none());
        assert!(cfg.vaults.is_empty());
    }

    #[test]
    fn save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");

        let mut cfg = GlobalConfig::default();
        cfg.register("main".into(), PathBuf::from("/home/user/vault"));
        cfg.register("work".into(), PathBuf::from("/home/user/work-vault"));
        cfg.default = Some("main".into());

        cfg.save_to(&path).unwrap();
        let loaded = GlobalConfig::load_from(&path).unwrap();

        assert_eq!(loaded.default.as_deref(), Some("main"));
        assert_eq!(loaded.vaults.len(), 2);
        assert_eq!(
            loaded.find("main").unwrap().path,
            PathBuf::from("/home/user/vault")
        );
        assert_eq!(
            loaded.find("work").unwrap().path,
            PathBuf::from("/home/user/work-vault")
        );
    }

    #[test]
    fn register_adds_new() {
        let mut cfg = GlobalConfig::default();
        let updated = cfg.register("test".into(), PathBuf::from("/tmp/test"));
        assert!(!updated);
        assert_eq!(cfg.vaults.len(), 1);
        assert_eq!(cfg.find("test").unwrap().path, PathBuf::from("/tmp/test"));
    }

    #[test]
    fn register_updates_existing() {
        let mut cfg = GlobalConfig::default();
        cfg.register("test".into(), PathBuf::from("/old/path"));
        let updated = cfg.register("test".into(), PathBuf::from("/new/path"));
        assert!(updated);
        assert_eq!(cfg.vaults.len(), 1);
        assert_eq!(cfg.find("test").unwrap().path, PathBuf::from("/new/path"));
    }

    #[test]
    fn unregister_removes() {
        let mut cfg = GlobalConfig::default();
        cfg.register("a".into(), PathBuf::from("/a"));
        cfg.register("b".into(), PathBuf::from("/b"));
        let removed = cfg.unregister("a");
        assert!(removed.is_some());
        assert_eq!(cfg.vaults.len(), 1);
        assert!(cfg.find("a").is_none());
    }

    #[test]
    fn unregister_clears_default_if_matched() {
        let mut cfg = GlobalConfig::default();
        cfg.register("main".into(), PathBuf::from("/main"));
        cfg.default = Some("main".into());
        cfg.unregister("main");
        assert!(cfg.default.is_none());
    }

    #[test]
    fn unregister_nonexistent_is_noop() {
        let mut cfg = GlobalConfig::default();
        assert!(cfg.unregister("ghost").is_none());
    }

    #[test]
    fn default_vault_returns_correct_entry() {
        let mut cfg = GlobalConfig::default();
        cfg.register("alpha".into(), PathBuf::from("/alpha"));
        cfg.register("beta".into(), PathBuf::from("/beta"));
        cfg.default = Some("beta".into());
        let entry = cfg.default_vault().unwrap();
        assert_eq!(entry.name, "beta");
        assert_eq!(entry.path, PathBuf::from("/beta"));
    }

    #[test]
    fn default_vault_none_when_unset() {
        let cfg = GlobalConfig::default();
        assert!(cfg.default_vault().is_none());
    }

    #[test]
    fn config_dir_returns_some() {
        // On macOS/Linux with a HOME set, this should always succeed.
        assert!(GlobalConfig::config_dir().is_some());
    }
}
