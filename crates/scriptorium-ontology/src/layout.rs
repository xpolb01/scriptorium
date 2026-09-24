//! On-disk (in-git) layout conventions and name validation.

/// Default ontology directory, relative to the repository root.
pub const ONTOLOGY_DIR: &str = "ontology";
/// Optional top-level settings file inside the ontology directory.
pub const ONTOLOGY_MANIFEST: &str = "ontology.toml";
/// Marks a directory as a sub-ontology.
pub const SUBGRAPH_MANIFEST: &str = "subgraph.toml";
/// The sub-ontology's Federation v2 subgraph schema.
pub const SCHEMA_FILE: &str = "schema.graphql";
/// Default directory for git-backed records, relative to the sub-ontology.
pub const DEFAULT_DATA_DIR: &str = "data";
/// Extension of git-backed record files.
pub const RECORD_EXT: &str = "yaml";

/// Join repository-relative path segments with `/`, skipping empty ones.
pub fn join(base: &str, rest: &str) -> String {
    match (base.trim_matches('/'), rest.trim_matches('/')) {
        ("", r) => r.to_string(),
        (b, "") => b.to_string(),
        (b, r) => format!("{b}/{r}"),
    }
}

/// Canonical form of a repository-relative directory: `./ontology/` and
/// `ontology` both become `ontology`. Other components are kept as given
/// (callers that write validate them separately).
pub fn normalize_dir(dir: &str) -> String {
    dir.split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .collect::<Vec<_>>()
        .join("/")
}

/// Resolve `rel` against the repository-relative directory `base`,
/// applying `.` and `..`. `None` when the result would leave the repository.
pub fn resolve_within(base: &str, rel: &str) -> Option<String> {
    if rel.starts_with('/') {
        return None;
    }
    let base = normalize_dir(base);
    let mut parts: Vec<&str> = base.split('/').filter(|p| !p.is_empty()).collect();
    for part in rel.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop()?;
            }
            other => parts.push(other),
        }
    }
    Some(parts.join("/"))
}

/// Sub-ontology names become `join__Graph` enum values in the supergraph
/// and path segments in subgraph URLs, so keep them to `[a-z][a-z0-9_]*`.
pub fn validate_sub_ontology_name(name: &str) -> std::result::Result<(), String> {
    let mut chars = name.chars();
    let valid = matches!(chars.next(), Some('a'..='z'))
        && chars.all(|c| matches!(c, 'a'..='z' | '0'..='9' | '_'))
        && name.len() <= 63;
    if valid {
        Ok(())
    } else {
        Err(format!(
            "sub-ontology name `{name}` must match [a-z][a-z0-9_]* (max 63 chars)"
        ))
    }
}

/// Record keys are file names, so they must be safe path segments; a
/// negative integer (`-42`, canonical) is one too.
pub fn validate_record_key(key: &str) -> std::result::Result<(), String> {
    let mut chars = key.chars();
    let negative = key.strip_prefix('-').is_some_and(|digits| {
        !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) && !digits.starts_with('0')
    });
    let valid = (negative
        || matches!(chars.next(), Some(c) if c.is_ascii_alphanumeric())
            && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')))
        && key.len() <= 128;
    if valid {
        Ok(())
    } else {
        Err(format!(
            "record key `{key}` must be 1-128 chars of [A-Za-z0-9._-] starting with a letter or digit, or a negative integer"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_skips_empty_segments() {
        assert_eq!(join("", "a"), "a");
        assert_eq!(join("a/", ""), "a");
        assert_eq!(
            join("ontology", "core/schema.graphql"),
            "ontology/core/schema.graphql"
        );
    }

    #[test]
    fn normalize_dir_drops_dot_and_empty_segments() {
        assert_eq!(normalize_dir("./ontology/"), "ontology");
        assert_eq!(normalize_dir("a/./b//c"), "a/b/c");
        assert_eq!(normalize_dir("ontology"), "ontology");
        assert_eq!(
            normalize_dir("../x"),
            "../x",
            "left for validation to reject"
        );
    }

    #[test]
    fn resolve_within_stays_inside_the_repository() {
        assert_eq!(
            resolve_within("ontology/notes", "pages").as_deref(),
            Some("ontology/notes/pages")
        );
        assert_eq!(
            resolve_within("ontology/wiki", "../../wiki").as_deref(),
            Some("wiki")
        );
        assert_eq!(
            resolve_within("ontology/x", "./a/./b/").as_deref(),
            Some("ontology/x/a/b")
        );
        assert_eq!(resolve_within("ontology/x", "../../.."), None);
        assert_eq!(resolve_within("ontology/x", "/etc"), None);
    }

    #[test]
    fn sub_ontology_names() {
        assert!(validate_sub_ontology_name("core").is_ok());
        assert!(validate_sub_ontology_name("research_v2").is_ok());
        assert!(validate_sub_ontology_name("Research").is_err());
        assert!(validate_sub_ontology_name("re-search").is_err());
        assert!(validate_sub_ontology_name("2core").is_err());
        assert!(validate_sub_ontology_name("").is_err());
    }

    #[test]
    fn record_keys() {
        assert!(validate_record_key("vaswani").is_ok());
        assert!(validate_record_key("01J8Z3NDEKTSV4RRFFQ69G5FAV").is_ok());
        assert!(validate_record_key("rfc-9110.v2").is_ok());
        assert!(validate_record_key(".hidden").is_err());
        assert!(validate_record_key("a/b").is_err());
        assert!(validate_record_key("").is_err());
        assert!(validate_record_key("-42").is_ok());
        for key in ["-", "-0", "-042", "-a", "--1", "-4x"] {
            assert!(validate_record_key(key).is_err(), "{key}");
        }
    }
}
