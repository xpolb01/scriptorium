//! Discover the sub-ontologies present in a snapshot.
//!
//! A directory directly under the ontology directory is a sub-ontology when
//! it contains `subgraph.toml`. Loading never fails on a single bad
//! sub-ontology: problems are collected as [`RegistryIssue`]s and the rest
//! of the registry still loads, so `check` can report everything at once.

use std::collections::BTreeSet;

use crate::error::Result;
use crate::layout::{
    join, validate_sub_ontology_name, ONTOLOGY_MANIFEST, SCHEMA_FILE, SUBGRAPH_MANIFEST,
};
use crate::manifest::{OntologyManifest, RemoteConfig, StorageConfig, SubgraphManifest};
use crate::snapshot::Snapshot;

/// Every sub-ontology found at one snapshot.
#[derive(Debug, Clone)]
pub struct Registry {
    /// Repository-relative ontology directory, e.g. `ontology`.
    pub ontology_dir: String,
    pub manifest: OntologyManifest,
    /// Sorted by name.
    pub sub_ontologies: Vec<SubOntology>,
    /// Where the registry was read from, e.g. `HEAD@1a2b3c4`.
    pub source: String,
    /// The commit read, when the snapshot is a git revision.
    pub commit: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SubOntology {
    pub name: String,
    /// Repository-relative directory, e.g. `ontology/research`.
    pub dir: String,
    pub description: Option<String>,
    pub owners: Vec<String>,
    /// Federation v2 subgraph SDL (`schema.graphql`).
    pub sdl: String,
    pub source: SubOntologySource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubOntologySource {
    /// Served by scriptorium over the configured storage.
    Local(StorageConfig),
    /// Served elsewhere; `sdl` is a pinned copy of its schema.
    Remote(RemoteConfig),
}

impl SubOntology {
    pub fn is_local(&self) -> bool {
        matches!(self.source, SubOntologySource::Local(_))
    }

    /// Short description of where instances live: `git`, `remote`, …
    pub fn storage_kind(&self) -> &'static str {
        match &self.source {
            SubOntologySource::Local(storage) => storage.kind(),
            SubOntologySource::Remote(_) => "remote",
        }
    }

    pub fn schema_path(&self) -> String {
        join(&self.dir, SCHEMA_FILE)
    }

    /// URL the gateway uses to reach this sub-ontology's subgraph.
    pub fn subgraph_url(&self, local_base_url: &str) -> String {
        match &self.source {
            SubOntologySource::Local(_) => {
                format!(
                    "{}/{}/graphql",
                    local_base_url.trim_end_matches('/'),
                    self.name
                )
            }
            SubOntologySource::Remote(remote) => remote.url.clone(),
        }
    }
}

/// A problem that kept a directory from loading as a sub-ontology (or a
/// registry-wide problem such as a duplicate name).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryIssue {
    /// Repository-relative path the issue refers to.
    pub path: String,
    pub message: String,
}

impl Registry {
    /// Load every sub-ontology under `ontology_dir` in `snapshot`.
    pub fn load(snapshot: &dyn Snapshot, ontology_dir: &str) -> Result<(Self, Vec<RegistryIssue>)> {
        let ontology_dir = ontology_dir.trim_matches('/').to_string();
        let mut issues = Vec::new();

        let manifest_path = join(&ontology_dir, ONTOLOGY_MANIFEST);
        let manifest = match snapshot.read_string(&manifest_path)? {
            None => OntologyManifest::default(),
            Some(text) => toml::from_str(&text).unwrap_or_else(|e| {
                issues.push(RegistryIssue {
                    path: manifest_path.clone(),
                    message: format!("invalid {ONTOLOGY_MANIFEST}: {e}"),
                });
                OntologyManifest::default()
            }),
        };

        let mut sub_ontologies = Vec::new();
        let mut seen = BTreeSet::new();
        for entry in snapshot.list(&ontology_dir)? {
            // Git reads do not follow symlinks: a symlinked sub-ontology
            // would vanish from composition without a word.
            if entry.is_symlink {
                issues.push(RegistryIssue {
                    path: join(&ontology_dir, &entry.name),
                    message: "is a symlink; symlinks under the ontology are not read, so a sub-ontology must be a directory"
                        .to_string(),
                });
                continue;
            }
            if !entry.is_dir {
                continue;
            }
            let dir = join(&ontology_dir, &entry.name);
            match load_one(snapshot, &dir, &entry.name)? {
                Loaded::NotASubOntology => {}
                Loaded::Invalid(issue) => issues.push(issue),
                Loaded::Ok(sub) => {
                    if seen.insert(sub.name.clone()) {
                        sub_ontologies.push(*sub);
                    } else {
                        issues.push(RegistryIssue {
                            path: join(&dir, SUBGRAPH_MANIFEST),
                            message: format!(
                                "duplicate sub-ontology name `{}` (names must be unique)",
                                sub.name
                            ),
                        });
                    }
                }
            }
        }
        sub_ontologies.sort_by(|a, b| a.name.cmp(&b.name));

        Ok((
            Self {
                ontology_dir,
                manifest,
                sub_ontologies,
                source: snapshot.label(),
                commit: snapshot.commit(),
            },
            issues,
        ))
    }

    pub fn get(&self, name: &str) -> Option<&SubOntology> {
        self.sub_ontologies.iter().find(|s| s.name == name)
    }

    pub fn local(&self) -> impl Iterator<Item = &SubOntology> {
        self.sub_ontologies.iter().filter(|s| s.is_local())
    }
}

enum Loaded {
    NotASubOntology,
    Invalid(RegistryIssue),
    Ok(Box<SubOntology>),
}

fn load_one(snapshot: &dyn Snapshot, dir: &str, dir_name: &str) -> Result<Loaded> {
    let manifest_path = join(dir, SUBGRAPH_MANIFEST);
    let schema_path = join(dir, SCHEMA_FILE);
    let invalid = |path: &str, message: String| {
        Ok(Loaded::Invalid(RegistryIssue {
            path: path.to_string(),
            message,
        }))
    };

    let Some(manifest_text) = snapshot.read_string(&manifest_path)? else {
        if snapshot.read(&schema_path)?.is_some() {
            return invalid(
                &manifest_path,
                format!("{SCHEMA_FILE} exists but {SUBGRAPH_MANIFEST} is missing"),
            );
        }
        return Ok(Loaded::NotASubOntology);
    };
    let manifest: SubgraphManifest = match toml::from_str(&manifest_text) {
        Ok(m) => m,
        Err(e) => return invalid(&manifest_path, format!("invalid {SUBGRAPH_MANIFEST}: {e}")),
    };

    let name = manifest
        .name
        .clone()
        .unwrap_or_else(|| dir_name.to_string());
    if let Err(message) = validate_sub_ontology_name(&name) {
        return invalid(&manifest_path, message);
    }

    let source = match (manifest.storage, manifest.remote) {
        (Some(storage), None) => SubOntologySource::Local(storage),
        (None, Some(remote)) => {
            if let Err(message) = crate::remote::validate_url(&remote.url) {
                return invalid(&manifest_path, message);
            }
            SubOntologySource::Remote(remote)
        }
        (Some(_), Some(_)) => {
            return invalid(
                &manifest_path,
                "declare either [storage] (local) or [remote], not both".into(),
            )
        }
        (None, None) => {
            return invalid(
                &manifest_path,
                "missing [storage] (local sub-ontology) or [remote] (served elsewhere)".into(),
            )
        }
    };

    // Git trees are read by exact path: `./data` must read as `data`.
    let source = match source {
        SubOntologySource::Local(StorageConfig::Git { data_dir }) => {
            match normalize_data_dir(&data_dir) {
                Ok(data_dir) => SubOntologySource::Local(StorageConfig::Git { data_dir }),
                Err(message) => return invalid(&manifest_path, message),
            }
        }
        other => other,
    };

    let Some(sdl) = snapshot.read_string(&schema_path)? else {
        return invalid(
            &schema_path,
            format!("sub-ontology `{name}` has no {SCHEMA_FILE}"),
        );
    };

    Ok(Loaded::Ok(Box::new(SubOntology {
        name,
        dir: dir.to_string(),
        description: manifest.description,
        owners: manifest.owners,
        sdl,
        source,
    })))
}

/// `data_dir` as the path records are read at: relative, inside the
/// sub-ontology, without `.` or empty segments.
fn normalize_data_dir(data_dir: &str) -> std::result::Result<String, String> {
    if data_dir.starts_with('/')
        || data_dir.contains('\\')
        || data_dir.split('/').any(|p| p == "..")
    {
        return Err(format!(
            "`data_dir = \"{data_dir}\"` must be a relative path inside the sub-ontology"
        ));
    }
    let normalized = crate::layout::normalize_dir(data_dir);
    if normalized.is_empty() {
        return Err(format!(
            "`data_dir = \"{data_dir}\"` must name a directory inside the sub-ontology"
        ));
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{test_support::commit_files, GitSnapshot};

    const SDL: &str = "type Query { ok: Boolean }";
    const GIT: &str = "[storage]\nkind = \"git\"\n";

    #[test]
    fn remote_urls_carrying_credentials_are_issues() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[
                (
                    "ontology/partner/subgraph.toml",
                    "[remote]\nurl = \"https://p.example/graphql?api_key=s3cret\"\n",
                ),
                ("ontology/partner/schema.graphql", SDL),
            ],
            "init",
        );
        let snap = GitSnapshot::open(dir.path(), "HEAD").unwrap();
        let (reg, issues) = Registry::load(&snap, "ontology").unwrap();
        assert!(reg.get("partner").is_none());
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(
            issues[0].message.contains("must not have a query"),
            "{issues:?}"
        );
        assert!(!issues[0].message.contains("s3cret"), "{issues:?}");
    }

    #[test]
    fn loads_local_and_remote_sub_ontologies_sorted() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[
                ("ontology/research/subgraph.toml", GIT),
                ("ontology/research/schema.graphql", SDL),
                (
                    "ontology/core/subgraph.toml",
                    "owners = [\"@me\"]\n[storage]\nkind = \"git\"\n",
                ),
                ("ontology/core/schema.graphql", SDL),
                (
                    "ontology/partner/subgraph.toml",
                    "[remote]\nurl = \"https://p.example/graphql\"\n",
                ),
                ("ontology/partner/schema.graphql", SDL),
                ("ontology/docs/README.md", "not a sub-ontology"),
                (
                    "ontology/ontology.toml",
                    "[serve]\naddr = \"127.0.0.1:5000\"\n",
                ),
            ],
            "init",
        );
        let snap = GitSnapshot::open(dir.path(), "HEAD").unwrap();
        let (reg, issues) = Registry::load(&snap, "ontology").unwrap();
        assert!(issues.is_empty(), "{issues:?}");
        let names: Vec<_> = reg.sub_ontologies.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["core", "partner", "research"]);
        assert_eq!(reg.get("core").unwrap().owners, ["@me"]);
        assert_eq!(reg.local().count(), 2);
        assert_eq!(reg.manifest.serve.addr(), "127.0.0.1:5000");
        let base = reg.manifest.serve.base_url();
        assert_eq!(
            reg.get("research").unwrap().subgraph_url(&base),
            "http://127.0.0.1:5000/research/graphql"
        );
        assert_eq!(
            reg.get("partner").unwrap().subgraph_url(&base),
            "https://p.example/graphql"
        );
        assert!(reg.commit.is_some());
    }

    #[test]
    fn data_dirs_are_read_as_written_or_refused() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = |d: &str| format!("[storage]\nkind = \"git\"\ndata_dir = \"{d}\"\n");
        let (dotted, parent, absolute) = (data_dir("./data/"), data_dir("../x"), data_dir("/x"));
        commit_files(
            dir.path(),
            &[
                ("ontology/a/subgraph.toml", &dotted),
                ("ontology/a/schema.graphql", SDL),
                ("ontology/b/subgraph.toml", &parent),
                ("ontology/b/schema.graphql", SDL),
                ("ontology/c/subgraph.toml", &absolute),
                ("ontology/c/schema.graphql", SDL),
            ],
            "init",
        );
        let snap = GitSnapshot::open(dir.path(), "HEAD").unwrap();
        let (reg, issues) = Registry::load(&snap, "ontology").unwrap();
        let loaded: Vec<(&str, &SubOntologySource)> = reg
            .sub_ontologies
            .iter()
            .map(|s| (s.name.as_str(), &s.source))
            .collect();
        assert_eq!(
            loaded,
            [(
                "a",
                &SubOntologySource::Local(StorageConfig::Git {
                    data_dir: "data".into()
                })
            )]
        );
        let refused: Vec<&str> = issues.iter().map(|i| i.path.as_str()).collect();
        assert_eq!(
            refused,
            ["ontology/b/subgraph.toml", "ontology/c/subgraph.toml"],
            "{issues:?}"
        );
    }

    #[test]
    fn symlinked_sub_ontologies_are_issues_not_gaps() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[
                ("ontology/core/subgraph.toml", GIT),
                ("ontology/core/schema.graphql", SDL),
            ],
            "init",
        );
        // A symlink where a sub-ontology's directory was.
        let repo = git2::Repository::open(dir.path()).unwrap();
        let mut index = repo.index().unwrap();
        index
            .add_frombuffer(
                &git2::IndexEntry {
                    ctime: git2::IndexTime::new(0, 0),
                    mtime: git2::IndexTime::new(0, 0),
                    dev: 0,
                    ino: 0,
                    mode: 0o120_000,
                    uid: 0,
                    gid: 0,
                    file_size: 0,
                    id: git2::Oid::ZERO_SHA1,
                    flags: 0,
                    flags_extended: 0,
                    path: b"ontology/research".to_vec(),
                },
                b"../elsewhere/research",
            )
            .unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let sig = git2::Signature::now("t", "t@example.com").unwrap();
        let parent = repo.head().unwrap().peel_to_commit().unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "link", &tree, &[&parent])
            .unwrap();

        let snap = GitSnapshot::open(dir.path(), "HEAD").unwrap();
        let (reg, issues) = Registry::load(&snap, "ontology").unwrap();
        assert_eq!(reg.sub_ontologies.len(), 1);
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].path, "ontology/research");
        assert!(
            issues[0].message.contains("symlink"),
            "{}",
            issues[0].message
        );
    }

    #[test]
    fn collects_issues_without_failing() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[
                // both storage and remote
                (
                    "ontology/a/subgraph.toml",
                    "[storage]\nkind = \"git\"\n[remote]\nurl = \"x\"\n",
                ),
                ("ontology/a/schema.graphql", SDL),
                // neither
                ("ontology/b/subgraph.toml", "description = \"x\"\n"),
                ("ontology/b/schema.graphql", SDL),
                // no schema
                ("ontology/c/subgraph.toml", GIT),
                // schema without manifest
                ("ontology/d/schema.graphql", SDL),
                // bad name
                (
                    "ontology/e/subgraph.toml",
                    "name = \"Bad-Name\"\n[storage]\nkind = \"git\"\n",
                ),
                ("ontology/e/schema.graphql", SDL),
                // duplicate of f
                ("ontology/f/subgraph.toml", GIT),
                ("ontology/f/schema.graphql", SDL),
                (
                    "ontology/g/subgraph.toml",
                    "name = \"f\"\n[storage]\nkind = \"git\"\n",
                ),
                ("ontology/g/schema.graphql", SDL),
            ],
            "init",
        );
        let snap = GitSnapshot::open(dir.path(), "HEAD").unwrap();
        let (reg, issues) = Registry::load(&snap, "ontology").unwrap();
        let names: Vec<_> = reg.sub_ontologies.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["f"]);
        let paths: Vec<_> = issues.iter().map(|i| i.path.as_str()).collect();
        assert_eq!(
            paths,
            [
                "ontology/a/subgraph.toml",
                "ontology/b/subgraph.toml",
                "ontology/c/schema.graphql",
                "ontology/d/subgraph.toml",
                "ontology/e/subgraph.toml",
                "ontology/g/subgraph.toml",
            ]
        );
    }

    #[test]
    fn missing_ontology_dir_is_an_empty_registry() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(dir.path(), &[("README.md", "x")], "init");
        let snap = GitSnapshot::open(dir.path(), "HEAD").unwrap();
        let (reg, issues) = Registry::load(&snap, "ontology").unwrap();
        assert!(reg.sub_ontologies.is_empty());
        assert_eq!(issues, [] as [RegistryIssue; 0]);
    }
}
