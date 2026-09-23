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
    /// Markdown notes with YAML frontmatter, one entity per file, read from
    /// the same git snapshot as the schema.
    Markdown(MarkdownConfig),
    /// A Postgres database (build feature `postgres`).
    Postgres(PostgresConfig),
    /// A `DynamoDB` table (build feature `dynamodb`).
    Dynamodb(DynamoConfig),
}

impl StorageConfig {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Git { .. } => "git",
            Self::Markdown(_) => "markdown",
            Self::Postgres(_) => "postgres",
            Self::Dynamodb(_) => "dynamodb",
        }
    }
}

/// `kind = "markdown"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarkdownConfig {
    /// Repository-relative directory of `.md` files, searched recursively.
    pub dir: String,
    /// The entity type of every file. Exactly one of `type` and
    /// `type_field` is required.
    #[serde(rename = "type")]
    pub type_name: Option<String>,
    /// The frontmatter field naming each file's entity type.
    pub type_field: Option<String>,
    /// Where each entity's key comes from: `stem` (the file name without
    /// `.md`) or `frontmatter:<field>` (e.g. `frontmatter:id`).
    #[serde(default = "default_markdown_key")]
    pub key: String,
    /// A field that receives the markdown body.
    pub body_field: Option<String>,
    /// A field that receives the keys of the pages the body `[[wikilinks]]` to.
    pub links_field: Option<String>,
}

fn default_markdown_key() -> String {
    "stem".to_string()
}

/// `kind = "postgres"`: one table per entity type, one column per field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    /// Environment variable holding the connection string. The connection
    /// string itself never goes in git.
    pub url_env: String,
    /// Schema the tables live in.
    #[serde(default = "default_pg_schema")]
    pub schema: String,
    /// Entity type → table name; the default is the type name in `snake_case`.
    #[serde(default)]
    pub tables: std::collections::BTreeMap<String, String>,
}

fn default_pg_schema() -> String {
    "public".to_string()
}

/// `kind = "dynamodb"`: one table, items keyed by `pk = "<Type>#<key>"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamoConfig {
    pub table: String,
    /// AWS region; the default comes from the standard AWS configuration.
    pub region: Option<String>,
    /// Environment variable holding an endpoint URL override (`DynamoDB` Local).
    pub endpoint_url_env: Option<String>,
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
    fn parses_every_storage_kind() {
        let kinds: Vec<&str> = [
            "[storage]\nkind = \"git\"\n",
            "[storage]\nkind = \"markdown\"\ndir = \"notes\"\ntype = \"Note\"\n",
            "[storage]\nkind = \"postgres\"\nurl_env = \"DB_URL\"\n[storage.tables]\nPaper = \"papers\"\n",
            "[storage]\nkind = \"dynamodb\"\ntable = \"t\"\n",
        ]
        .iter()
        .map(|toml_text| {
            let m: SubgraphManifest = toml::from_str(toml_text).unwrap();
            m.storage.unwrap().kind()
        })
        .collect();
        assert_eq!(kinds, ["git", "markdown", "postgres", "dynamodb"]);
        let md: SubgraphManifest =
            toml::from_str("[storage]\nkind = \"markdown\"\ndir = \"n\"\ntype = \"N\"\n").unwrap();
        let Some(StorageConfig::Markdown(md)) = md.storage else {
            panic!("markdown")
        };
        assert_eq!(md.key, "stem");
        let pg: SubgraphManifest =
            toml::from_str("[storage]\nkind = \"postgres\"\nurl_env = \"X\"\n").unwrap();
        let Some(StorageConfig::Postgres(pg)) = pg.storage else {
            panic!("postgres")
        };
        assert_eq!(pg.schema, "public");
        assert!(
            toml::from_str::<SubgraphManifest>(
                "[storage]\nkind = \"postgres\"\nurl = \"postgres://secret\"\n"
            )
            .is_err(),
            "a literal connection string is rejected"
        );
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
