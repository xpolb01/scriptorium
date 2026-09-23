//! `ontology init`: scaffold the example ontology into a repository.
//!
//! The example is three sub-ontologies that extend each other: `core`
//! (people), `research` (papers; adds `Person.papers`), and `software`
//! (repositories; adds `Paper.implementations`). It exercises entity
//! extension, inverse relations, enums, and git-backed records.

use std::path::{Component, Path, PathBuf};

use crate::error::{Error, Result};

/// `(path relative to the ontology directory, contents)`.
pub const EXAMPLE: &[(&str, &str)] = &[
    (
        "core/data/Person/chang.yaml",
        include_str!("../../../templates/ontology/core/data/Person/chang.yaml"),
    ),
    (
        "core/data/Person/devlin.yaml",
        include_str!("../../../templates/ontology/core/data/Person/devlin.yaml"),
    ),
    (
        "core/data/Person/gomez.yaml",
        include_str!("../../../templates/ontology/core/data/Person/gomez.yaml"),
    ),
    (
        "core/data/Person/jones.yaml",
        include_str!("../../../templates/ontology/core/data/Person/jones.yaml"),
    ),
    (
        "core/data/Person/kaiser.yaml",
        include_str!("../../../templates/ontology/core/data/Person/kaiser.yaml"),
    ),
    (
        "core/data/Person/lee.yaml",
        include_str!("../../../templates/ontology/core/data/Person/lee.yaml"),
    ),
    (
        "core/data/Person/parmar.yaml",
        include_str!("../../../templates/ontology/core/data/Person/parmar.yaml"),
    ),
    (
        "core/data/Person/polosukhin.yaml",
        include_str!("../../../templates/ontology/core/data/Person/polosukhin.yaml"),
    ),
    (
        "core/data/Person/shazeer.yaml",
        include_str!("../../../templates/ontology/core/data/Person/shazeer.yaml"),
    ),
    (
        "core/data/Person/toutanova.yaml",
        include_str!("../../../templates/ontology/core/data/Person/toutanova.yaml"),
    ),
    (
        "core/data/Person/uszkoreit.yaml",
        include_str!("../../../templates/ontology/core/data/Person/uszkoreit.yaml"),
    ),
    (
        "core/data/Person/vaswani.yaml",
        include_str!("../../../templates/ontology/core/data/Person/vaswani.yaml"),
    ),
    (
        "core/schema.graphql",
        include_str!("../../../templates/ontology/core/schema.graphql"),
    ),
    (
        "core/subgraph.toml",
        include_str!("../../../templates/ontology/core/subgraph.toml"),
    ),
    (
        "ontology.toml",
        include_str!("../../../templates/ontology/ontology.toml"),
    ),
    (
        "research/data/Paper/attention-is-all-you-need.yaml",
        include_str!(
            "../../../templates/ontology/research/data/Paper/attention-is-all-you-need.yaml"
        ),
    ),
    (
        "research/data/Paper/bert.yaml",
        include_str!("../../../templates/ontology/research/data/Paper/bert.yaml"),
    ),
    (
        "research/schema.graphql",
        include_str!("../../../templates/ontology/research/schema.graphql"),
    ),
    (
        "research/subgraph.toml",
        include_str!("../../../templates/ontology/research/subgraph.toml"),
    ),
    (
        "software/data/Repository/google-research-bert.yaml",
        include_str!(
            "../../../templates/ontology/software/data/Repository/google-research-bert.yaml"
        ),
    ),
    (
        "software/data/Repository/tensor2tensor.yaml",
        include_str!("../../../templates/ontology/software/data/Repository/tensor2tensor.yaml"),
    ),
    (
        "software/schema.graphql",
        include_str!("../../../templates/ontology/software/schema.graphql"),
    ),
    (
        "software/subgraph.toml",
        include_str!("../../../templates/ontology/software/subgraph.toml"),
    ),
];

/// Write [`EXAMPLE`] under `<repo_root>/<ontology_dir>/`. Refuses to touch
/// an ontology directory that already has content. Returns the written
/// paths, relative to `repo_root`.
pub fn scaffold_example(repo_root: &Path, ontology_dir: &str) -> Result<Vec<PathBuf>> {
    confine(repo_root, ontology_dir)?;
    // Written paths are staged as-is, and git rejects `./`-prefixed ones.
    let ontology_dir = &crate::layout::normalize_dir(ontology_dir);
    let target = repo_root.join(ontology_dir);
    let occupied = std::fs::read_dir(&target).is_ok_and(|mut entries| entries.next().is_some());
    if occupied {
        return Err(Error::Invalid {
            what: "ontology directory",
            path: target.display().to_string(),
            message: "already exists and is not empty".into(),
        });
    }
    let mut written = Vec::with_capacity(EXAMPLE.len());
    for (rel, contents) in EXAMPLE {
        let rel_path = Path::new(ontology_dir).join(rel);
        let full = repo_root.join(&rel_path);
        if let Some(parent) = full.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::io(parent.display().to_string(), e))?;
        }
        std::fs::write(&full, contents).map_err(|e| Error::io(full.display().to_string(), e))?;
        written.push(rel_path);
    }
    Ok(written)
}

/// `ontology_dir` must name a place inside `repo_root`: relative, no `..`,
/// and no existing component that is a symlink (which could point anywhere).
fn confine(repo_root: &Path, ontology_dir: &str) -> Result<()> {
    let invalid = |message: &str| Error::Invalid {
        what: "ontology directory",
        path: ontology_dir.to_string(),
        message: message.to_string(),
    };
    let rel = Path::new(ontology_dir);
    let normal = rel
        .components()
        .all(|c| matches!(c, Component::Normal(_) | Component::CurDir));
    if !normal || rel.components().all(|c| matches!(c, Component::CurDir)) {
        return Err(invalid(
            "must be a relative path inside the repository (no `..`, not absolute, not the root)",
        ));
    }
    let mut current = repo_root.to_path_buf();
    for component in rel.components() {
        current.push(component);
        match std::fs::symlink_metadata(&current) {
            Ok(meta) if meta.file_type().is_symlink() => {
                return Err(invalid(&format!(
                    "{} is a symlink; refusing to write through it",
                    current.display()
                )));
            }
            Ok(_) => {}
            // Nothing exists below this point, so nothing can redirect the write.
            Err(_) => break,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::check::check;
    use crate::snapshot::WorktreeSnapshot;

    #[test]
    fn example_ontology_checks_clean() {
        let dir = tempfile::tempdir().unwrap();
        let written = scaffold_example(dir.path(), "ontology").unwrap();
        assert_eq!(written.len(), EXAMPLE.len());
        let report = check(&WorktreeSnapshot::new(dir.path()), "ontology").unwrap();
        assert!(report.issues.is_empty(), "{:#?}", report.issues);
        assert_eq!(report.sub_ontologies, 3);
        assert!(report.records > 10);
        assert!(
            scaffold_example(dir.path(), "ontology").is_err(),
            "refuses to overwrite"
        );
    }

    #[test]
    fn refuses_directories_outside_the_repository() {
        let repo = tempfile::tempdir().unwrap();
        for dir in ["../escape", "/tmp/escape", "", ".", "a/../../b"] {
            let err = scaffold_example(repo.path(), dir).unwrap_err().to_string();
            assert!(
                err.contains("relative path inside the repository"),
                "{dir}: {err}"
            );
        }
        assert!(!repo.path().join("../escape").exists());
    }

    #[cfg(unix)]
    #[test]
    fn refuses_to_write_through_symlinks() {
        let repo = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), repo.path().join("ontology")).unwrap();
        let err = scaffold_example(repo.path(), "ontology")
            .unwrap_err()
            .to_string();
        assert!(err.contains("is a symlink"), "{err}");
        assert!(
            std::fs::read_dir(outside.path()).unwrap().next().is_none(),
            "nothing written"
        );
    }
}
