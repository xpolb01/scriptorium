//! Stem normalization for duplicate detection.
//!
//! Two wiki pages whose filenames differ only in case or directory placement
//! will collide on case-insensitive file systems (macOS APFS, Windows NTFS)
//! and produce ambiguous wikilink targets. [`normalize_stem`] reduces a
//! vault-relative path to a canonical lowercase stem so the lint layer can
//! detect these collisions before they cause data loss.

use camino::Utf8Path;

/// Normalize a vault-relative path to its canonical stem for duplicate detection.
///
/// Rules:
/// - Normalize path separators to forward slashes (camino already does this)
/// - Extract the file stem (filename without `.md` extension)
/// - Lowercase it (macOS APFS case-insensitivity)
/// - Handle `index.md` special case: `foo/index.md` → stem `foo` (parent
///   directory name). No `index.md` pages exist in the vault today, but we
///   handle this defensively for future compatibility.
/// - Non-`.md` files: return the full filename lowercased
pub fn normalize_stem(path: &Utf8Path) -> String {
    let file_name = path.file_name().unwrap_or("");

    if path.extension() != Some("md") {
        return file_name.to_lowercase();
    }

    let stem = path.file_stem().unwrap_or("");

    if stem.eq_ignore_ascii_case("index") {
        if let Some(parent) = path.parent() {
            if let Some(parent_name) = parent.file_name() {
                return parent_name.to_lowercase();
            }
        }
        return stem.to_lowercase();
    }

    stem.to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regular_md_file() {
        assert_eq!(normalize_stem(Utf8Path::new("wiki/concepts/foo.md")), "foo");
    }

    #[test]
    fn case_insensitive() {
        assert_eq!(normalize_stem(Utf8Path::new("wiki/concepts/FOO.md")), "foo");
        assert_eq!(normalize_stem(Utf8Path::new("wiki/entities/Foo.md")), "foo");
    }

    #[test]
    fn index_md_uses_parent_dir() {
        assert_eq!(
            normalize_stem(Utf8Path::new("wiki/concepts/foo/index.md")),
            "foo"
        );
    }

    #[test]
    fn index_md_case_insensitive() {
        assert_eq!(
            normalize_stem(Utf8Path::new("wiki/topics/bar/INDEX.md")),
            "bar"
        );
    }

    #[test]
    fn bare_index_md() {
        assert_eq!(normalize_stem(Utf8Path::new("index.md")), "index");
    }

    #[test]
    fn non_md_file() {
        assert_eq!(
            normalize_stem(Utf8Path::new("wiki/concepts/notes.txt")),
            "notes.txt"
        );
    }

    #[test]
    fn nested_path() {
        assert_eq!(
            normalize_stem(Utf8Path::new("wiki/topics/nested/deep.md")),
            "deep"
        );
    }

    #[test]
    fn empty_path() {
        // Edge case: empty string produces empty stem
        assert_eq!(normalize_stem(Utf8Path::new("")), "");
    }

    #[test]
    fn dotfile_no_extension() {
        // A dotfile like `.hidden` has no stem in the usual sense
        assert_eq!(normalize_stem(Utf8Path::new("wiki/.hidden")), ".hidden");
    }

    #[test]
    fn double_extension() {
        // `foo.bar.md` — file_stem is `foo.bar`
        assert_eq!(
            normalize_stem(Utf8Path::new("wiki/concepts/foo.bar.md")),
            "foo.bar"
        );
    }

    #[test]
    fn deeply_nested_index() {
        // index.md several levels deep should use immediate parent
        assert_eq!(
            normalize_stem(Utf8Path::new("wiki/topics/a/b/c/index.md")),
            "c"
        );
    }

    #[test]
    fn mixed_case_extension() {
        // `.MD` is not `.md` — camino is case-sensitive on extension
        // so this should be treated as a non-md file
        assert_eq!(
            normalize_stem(Utf8Path::new("wiki/concepts/Foo.MD")),
            "foo.md"
        );
    }
}
