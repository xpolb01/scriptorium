//! Skill framework: named markdown instruction sets for AI agents.
//!
//! Skills teach agents (Claude Code, `OpenClaw`, Hermes, etc.) how to perform
//! specific scriptorium workflows. They are NOT code — they are prompts
//! stored as `SKILL.md` files under `<vault>/skills/`. A `manifest.json`
//! registry lists all available skills.
//!
//! The CLI serves skills via `scriptorium skill list/show`, and the MCP
//! server exposes them via `scriptorium_skill_list/read` so agents can
//! discover and read them programmatically.
//!
//! Reference: `GBrain`'s `skills/manifest.json` and `skills/*/SKILL.md`.

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::vault::Vault;

/// One entry in the skill manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillEntry {
    pub name: String,
    pub path: String,
    pub description: String,
}

/// The skill manifest (`skills/manifest.json`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillManifest {
    pub name: String,
    pub version: String,
    pub skills: Vec<SkillEntry>,
}

/// A loaded skill with its full content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Skill {
    pub name: String,
    pub path: Utf8PathBuf,
    pub description: String,
    pub content: String,
}

/// Skills directory name inside the vault.
const SKILLS_DIR: &str = "skills";
const MANIFEST_FILE: &str = "manifest.json";

/// Load the skill manifest from `<vault>/skills/manifest.json`.
/// Returns an empty manifest if the file doesn't exist.
pub fn load_manifest(vault: &Vault) -> Result<SkillManifest> {
    let path = vault.root().join(SKILLS_DIR).join(MANIFEST_FILE);
    match std::fs::read_to_string(path.as_std_path()) {
        Ok(text) => {
            let manifest: SkillManifest = serde_json::from_str(&text).map_err(|e| {
                Error::Other(anyhow::anyhow!("invalid skills manifest: {e}"))
            })?;
            Ok(manifest)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(SkillManifest {
            name: "scriptorium".into(),
            version: "0.0.0".into(),
            skills: Vec::new(),
        }),
        Err(e) => Err(Error::io(path.into_std_path_buf(), e)),
    }
}

/// List all skills from the manifest.
pub fn list_skills(vault: &Vault) -> Result<Vec<SkillEntry>> {
    let manifest = load_manifest(vault)?;
    Ok(manifest.skills)
}

/// Load a single skill by name, reading its `SKILL.md` content.
pub fn load_skill(vault: &Vault, name: &str) -> Result<Skill> {
    let manifest = load_manifest(vault)?;
    let entry = manifest
        .skills
        .iter()
        .find(|s| s.name == name)
        .ok_or_else(|| {
            Error::Other(anyhow::anyhow!("skill not found: {name}"))
        })?;

    let skill_path = Utf8PathBuf::from(SKILLS_DIR).join(&entry.path);
    let abs_path = vault.root().join(&skill_path);
    let content = std::fs::read_to_string(abs_path.as_std_path())
        .map_err(|e| Error::io(abs_path.into_std_path_buf(), e))?;

    Ok(Skill {
        name: entry.name.clone(),
        path: skill_path,
        description: entry.description.clone(),
        content,
    })
}

/// Scaffold the default skills directory from bundled templates.
/// Does not overwrite existing files.
pub fn init_skills(vault: &Vault) -> Result<usize> {
    let skills_dir = vault.root().join(SKILLS_DIR);
    std::fs::create_dir_all(skills_dir.as_std_path())
        .map_err(|e| Error::io(skills_dir.clone().into_std_path_buf(), e))?;

    let templates: &[(&str, &str)] = &[
        ("manifest.json", include_str!("../../../templates/skills/manifest.json")),
        ("ingest/SKILL.md", include_str!("../../../templates/skills/ingest/SKILL.md")),
        ("query/SKILL.md", include_str!("../../../templates/skills/query/SKILL.md")),
        ("maintain/SKILL.md", include_str!("../../../templates/skills/maintain/SKILL.md")),
        ("review/SKILL.md", include_str!("../../../templates/skills/review/SKILL.md")),
        ("learn/SKILL.md", include_str!("../../../templates/skills/learn/SKILL.md")),
    ];

    let mut written = 0;
    for (rel, content) in templates {
        let path = skills_dir.join(rel);
        if path.as_std_path().exists() {
            continue;
        }
        if let Some(parent) = path.as_std_path().parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::io(parent.to_path_buf(), e))?;
        }
        std::fs::write(path.as_std_path(), content)
            .map_err(|e| Error::io(path.into_std_path_buf(), e))?;
        written += 1;
    }
    Ok(written)
}

/// Resolve a vault-relative skill path for display.
pub fn skills_dir(vault: &Vault) -> Utf8PathBuf {
    vault.root().join(SKILLS_DIR)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vault::Vault;

    #[test]
    fn skill_manifest_round_trip() {
        let manifest = SkillManifest {
            name: "test".into(),
            version: "1.0.0".into(),
            skills: vec![SkillEntry {
                name: "ingest".into(),
                path: "ingest/SKILL.md".into(),
                description: "Ingest sources".into(),
            }],
        };
        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: SkillManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.skills.len(), 1);
        assert_eq!(parsed.skills[0].name, "ingest");
    }

    #[test]
    fn load_skill_reads_content() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("wiki")).unwrap();
        std::fs::create_dir_all(dir.path().join("skills/test")).unwrap();
        std::fs::write(
            dir.path().join("skills/manifest.json"),
            r#"{"name":"t","version":"1","skills":[{"name":"test","path":"test/SKILL.md","description":"desc"}]}"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("skills/test/SKILL.md"),
            "# Skill: Test\n\nContent here.\n",
        )
        .unwrap();

        let vault = Vault::open(dir.path()).unwrap();
        let skill = load_skill(&vault, "test").unwrap();
        assert_eq!(skill.name, "test");
        assert!(skill.content.contains("Content here"));
    }

    #[test]
    fn init_skills_scaffolds_defaults() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("wiki")).unwrap();
        let vault = Vault::open(dir.path()).unwrap();

        let written = init_skills(&vault).unwrap();
        assert_eq!(written, 6, "manifest + 5 skill files");

        // Verify manifest is valid.
        let manifest = load_manifest(&vault).unwrap();
        assert_eq!(manifest.skills.len(), 5);

        // Verify each skill loads.
        for entry in &manifest.skills {
            let skill = load_skill(&vault, &entry.name).unwrap();
            assert!(!skill.content.is_empty());
        }

        // Second init is a no-op (doesn't overwrite).
        let written2 = init_skills(&vault).unwrap();
        assert_eq!(written2, 0, "should not overwrite existing files");
    }

    #[test]
    fn list_skills_returns_all() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("wiki")).unwrap();
        let vault = Vault::open(dir.path()).unwrap();
        init_skills(&vault).unwrap();

        let skills = list_skills(&vault).unwrap();
        assert_eq!(skills.len(), 5);
        let names: Vec<&str> = skills.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"ingest"));
        assert!(names.contains(&"query"));
        assert!(names.contains(&"maintain"));
        assert!(names.contains(&"review"));
        assert!(names.contains(&"learn"));
    }
}
