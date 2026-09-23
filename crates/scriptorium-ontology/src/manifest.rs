//! `subgraph.toml` and `ontology.toml`: the declarative half of a
//! sub-ontology (the schema is the other half).

use serde::{Deserialize, Serialize};

use crate::layout::DEFAULT_DATA_DIR;

/// Default address `scriptorium-ontology serve` binds to.
pub const DEFAULT_SERVE_ADDR: &str = "127.0.0.1:4200";

/// Optional `ontology/ontology.toml`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OntologyManifest {
    #[serde(default)]
    pub serve: ServeSettings,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServeSettings {
    /// Socket address `scriptorium-ontology serve` binds to (default `127.0.0.1:4200`).
    pub addr: Option<String>,
    /// Base URL the gateway uses to reach local subgraphs, when it differs
    /// from `http://<addr>` (e.g. behind a proxy or in a container).
    pub base_url: Option<String>,
}

impl ServeSettings {
    pub fn addr(&self) -> &str {
        self.addr.as_deref().unwrap_or(DEFAULT_SERVE_ADDR)
    }

    pub fn base_url(&self) -> String {
        self.base_url
            .clone()
            .unwrap_or_else(|| format!("http://{}", self.addr()))
            .trim_end_matches('/')
            .to_string()
    }
}

/// `ontology/<name>/subgraph.toml`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubgraphManifest {
    /// Defaults to the directory name.
    pub name: Option<String>,
    pub description: Option<String>,
    /// Owning people or teams (free-form; mirror them in CODEOWNERS).
    #[serde(default)]
    pub owners: Vec<String>,
    /// Where a local sub-ontology keeps its instances.
    pub storage: Option<StorageConfig>,
    /// A sub-ontology served by someone else.
    pub remote: Option<RemoteConfig>,
}

/// Storage backend for a local sub-ontology's instances.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub enum StorageConfig {
    /// One YAML file per entity, committed to git:
    /// `<sub-ontology>/<data_dir>/<Type>/<key>.yaml`.
    Git {
        #[serde(default = "default_data_dir")]
        data_dir: String,
    },
}

impl StorageConfig {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Git { .. } => "git",
        }
    }
}

fn default_data_dir() -> String {
    DEFAULT_DATA_DIR.to_string()
}

/// A Federation v2 subgraph served elsewhere. Its `schema.graphql` in git is
/// a pinned snapshot of the remote SDL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteConfig {
    pub url: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_git_storage_with_default_data_dir() {
        let m: SubgraphManifest = toml::from_str(
            r#"
            description = "Papers"
            owners = ["@me"]
            [storage]
            kind = "git"
            "#,
        )
        .unwrap();
        assert_eq!(
            m.storage,
            Some(StorageConfig::Git {
                data_dir: "data".into()
            })
        );
        assert!(m.remote.is_none());
    }

    #[test]
    fn parses_remote() {
        let m: SubgraphManifest = toml::from_str(
            r#"
            [remote]
            url = "https://example.com/graphql"
            "#,
        )
        .unwrap();
        assert_eq!(m.remote.unwrap().url, "https://example.com/graphql");
    }

    #[test]
    fn rejects_unknown_keys_and_kinds() {
        assert!(toml::from_str::<SubgraphManifest>("descripton = \"typo\"").is_err());
        let err = toml::from_str::<SubgraphManifest>("[storage]\nkind = \"cassandra\"")
            .unwrap_err()
            .to_string();
        assert!(err.contains("cassandra"), "{err}");
        // A misspelled storage setting would silently fall back to the default.
        let err =
            toml::from_str::<SubgraphManifest>("[storage]\nkind = \"git\"\ndata_dri = \"entries\"")
                .unwrap_err()
                .to_string();
        assert!(err.contains("data_dri"), "{err}");
    }

    #[test]
    fn serve_defaults() {
        let s = ServeSettings::default();
        assert_eq!(s.addr(), "127.0.0.1:4200");
        assert_eq!(s.base_url(), "http://127.0.0.1:4200");
        let s = ServeSettings {
            addr: None,
            base_url: Some("https://ontology.internal/".into()),
        };
        assert_eq!(s.base_url(), "https://ontology.internal");
    }
}
