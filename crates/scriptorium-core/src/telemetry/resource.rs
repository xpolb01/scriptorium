#![allow(clippy::doc_markdown)]

//! OTel-style resource descriptor — stable identity for a process instance.
//!
//! A [`Resource`] collects process-specific attributes (service name/version, host,
//! PID, OS, runtime, vault path) into a canonical JSON form, then hashes it with SHA-256.
//! The hash is deterministic — identical attributes always produce identical hashes —
//! which makes it suitable for stable resource deduplication in the telemetry store.

use crate::telemetry::envelope::Source;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;

/// OTel-style resource descriptor — stable identity for a process instance.
/// Serialized as canonical JSON (sorted keys) for a deterministic sha256 hash.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Resource {
    /// OTel semantic-convention attributes. Sorted via BTreeMap for canonical JSON.
    pub attributes: BTreeMap<String, String>,
    /// sha256(canonical_json(attributes)) — 64 lowercase hex chars. Stable identity.
    pub attributes_hash: String,
}

impl Resource {
    /// Detect process-specific attributes and produce a stable Resource.
    ///
    /// Attributes set:
    /// - `service.name`: `"scriptorium-cli"` | `"scriptorium-mcp"` | `"scriptorium-hook"` | `"scriptorium-core"`
    /// - `service.version`: env!("CARGO_PKG_VERSION")
    /// - `host.name`: hostname::get() (fallback "unknown")
    /// - `process.pid`: std::process::id() as decimal string
    /// - `process.runtime.name`: `"rust"`
    /// - `process.runtime.version`: env!("RUSTC_VERSION") if available, else fallback "1.94"
    /// - `os.type`: std::env::consts::OS
    /// - `scriptorium.vault`: vault path canonicalized (if provided)
    pub fn detect(source: Source, vault_path: Option<&Path>) -> Resource {
        let mut attributes = BTreeMap::new();

        // service.name per Source
        let service_name = match source {
            Source::Cli => "scriptorium-cli",
            Source::Mcp => "scriptorium-mcp",
            Source::Hook => "scriptorium-hook",
            Source::Core => "scriptorium-core",
        };
        attributes.insert("service.name".to_string(), service_name.to_string());

        // service.version from Cargo.toml
        attributes.insert(
            "service.version".to_string(),
            env!("CARGO_PKG_VERSION").to_string(),
        );

        // host.name via hostname crate
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "unknown".to_string());
        attributes.insert("host.name".to_string(), hostname);

        // process.pid as decimal string
        attributes.insert("process.pid".to_string(), std::process::id().to_string());

        // process.runtime.name
        attributes.insert("process.runtime.name".to_string(), "rust".to_string());

        // process.runtime.version — try env!("RUSTC_VERSION"), fallback to "1.94"
        let rustc_version = option_env!("RUSTC_VERSION").unwrap_or("1.94").to_string();
        attributes.insert("process.runtime.version".to_string(), rustc_version);

        // os.type (e.g., "linux", "macos", "windows")
        attributes.insert("os.type".to_string(), std::env::consts::OS.to_string());

        // scriptorium.vault — canonicalized path if provided
        if let Some(vault) = vault_path {
            match vault.canonicalize() {
                Ok(canonical) => {
                    attributes.insert(
                        "scriptorium.vault".to_string(),
                        canonical.to_string_lossy().into_owned(),
                    );
                }
                Err(_) => {
                    // Canonicalization failed (vault doesn't exist yet). Fall back to literal path.
                    tracing::debug!(
                        "vault path canonicalization failed, using literal path: {:?}",
                        vault
                    );
                    attributes.insert(
                        "scriptorium.vault".to_string(),
                        vault.to_string_lossy().into_owned(),
                    );
                }
            }
        }

        Self::from_attributes(attributes)
    }

    /// Build a resource from pre-supplied attributes (used mostly for tests).
    pub fn from_attributes(attributes: BTreeMap<String, String>) -> Resource {
        let attributes_hash = Self::compute_hash(&attributes);
        Resource {
            attributes,
            attributes_hash,
        }
    }

    /// Canonical JSON serialization (sorted keys, no whitespace).
    /// Used for hashing to ensure deterministic output.
    pub fn canonical_json(&self) -> String {
        // BTreeMap serializes with sorted keys. serde_json produces no whitespace by default.
        serde_json::to_string(&self.attributes).unwrap_or_else(|_| "{}".to_string())
    }

    /// Compute sha256 hash of canonical_json. Returns 64 lowercase hex chars.
    fn compute_hash(attributes: &BTreeMap<String, String>) -> String {
        let canonical = serde_json::to_string(attributes).unwrap_or_else(|_| "{}".to_string());
        let mut hasher = Sha256::new();
        hasher.update(canonical.as_bytes());
        hex_encode(&hasher.finalize())
    }
}

/// Encode bytes to lowercase hex string (0-9a-f).
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_hash() {
        // Same inputs → same hash
        let r1 = Resource::detect(Source::Cli, None);
        let r2 = Resource::detect(Source::Cli, None);
        assert_eq!(
            r1.attributes_hash, r2.attributes_hash,
            "identical detect() calls should produce identical hashes"
        );
    }

    #[test]
    fn vault_differentiates() {
        // Different vaults → different hashes
        let r1 = Resource::detect(Source::Cli, Some(Path::new("/tmp")));
        let r2 = Resource::detect(Source::Cli, Some(Path::new("/var")));
        assert_ne!(
            r1.attributes_hash, r2.attributes_hash,
            "different vault paths should produce different hashes"
        );
    }

    #[test]
    fn different_source_different_hash() {
        let r_cli = Resource::detect(Source::Cli, None);
        let r_mcp = Resource::detect(Source::Mcp, None);
        assert_ne!(
            r_cli.attributes_hash, r_mcp.attributes_hash,
            "different sources should produce different hashes"
        );
    }

    #[test]
    fn canonical_json_sorted_keys() {
        let mut attrs = BTreeMap::new();
        attrs.insert("zeta".to_string(), "1".to_string());
        attrs.insert("alpha".to_string(), "2".to_string());
        attrs.insert("mu".to_string(), "3".to_string());

        let resource = Resource::from_attributes(attrs);
        let canonical = resource.canonical_json();

        // Keys should be in alphabetical order: alpha, mu, zeta
        assert_eq!(
            canonical, r#"{"alpha":"2","mu":"3","zeta":"1"}"#,
            "canonical JSON should have sorted keys"
        );
    }

    #[test]
    fn hash_is_64_hex() {
        let resource = Resource::detect(Source::Cli, None);
        assert_eq!(
            resource.attributes_hash.len(),
            64,
            "SHA256 hash should be exactly 64 hex chars"
        );
        assert!(
            resource
                .attributes_hash
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "hash should be lowercase hex only: {}",
            resource.attributes_hash
        );
    }

    #[test]
    fn service_name_mapping() {
        let sources = vec![
            (Source::Cli, "scriptorium-cli"),
            (Source::Mcp, "scriptorium-mcp"),
            (Source::Hook, "scriptorium-hook"),
            (Source::Core, "scriptorium-core"),
        ];

        for (source, expected_name) in sources {
            let resource = Resource::detect(source, None);
            assert_eq!(
                resource.attributes.get("service.name"),
                Some(&expected_name.to_string()),
                "Source::{:?} should map to {}",
                source,
                expected_name
            );
        }
    }

    #[test]
    fn vault_canonicalization_missing_path() {
        // Non-existent path should not panic, should fall back gracefully
        let non_existent = Path::new("/this/path/definitely/does/not/exist/12345");
        let resource = Resource::detect(Source::Cli, Some(non_existent));

        // Should have set the vault attribute to the literal path
        assert!(resource.attributes.contains_key("scriptorium.vault"));
        let vault_attr = resource.attributes.get("scriptorium.vault").unwrap();
        assert!(vault_attr.contains("does/not/exist"));
    }

    #[test]
    fn service_version_matches_pkg() {
        let resource = Resource::detect(Source::Cli, None);
        assert_eq!(
            resource.attributes.get("service.version"),
            Some(&env!("CARGO_PKG_VERSION").to_string()),
            "service.version should match CARGO_PKG_VERSION"
        );
    }

    #[test]
    fn from_attributes_roundtrip() {
        let mut attrs = BTreeMap::new();
        attrs.insert("key1".to_string(), "value1".to_string());
        attrs.insert("key2".to_string(), "value2".to_string());

        let resource = Resource::from_attributes(attrs.clone());
        assert_eq!(resource.attributes, attrs);
        assert!(!resource.attributes_hash.is_empty());
    }

    #[test]
    fn canonical_json_idempotent() {
        let resource = Resource::detect(Source::Cli, None);
        let json1 = resource.canonical_json();
        let json2 = resource.canonical_json();
        assert_eq!(json1, json2, "canonical_json should be idempotent");
    }

    #[test]
    fn hash_stability_across_instances() {
        // Create two resources with identical attributes and verify hash matches
        let mut attrs = BTreeMap::new();
        attrs.insert("service.name".to_string(), "test-service".to_string());
        attrs.insert("service.version".to_string(), "1.0.0".to_string());

        let r1 = Resource::from_attributes(attrs.clone());
        let r2 = Resource::from_attributes(attrs);

        assert_eq!(r1.attributes_hash, r2.attributes_hash);
    }
}
