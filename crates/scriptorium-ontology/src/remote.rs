//! Remote sub-ontologies: pin a live subgraph's schema in git.
//!
//! A remote sub-ontology's `schema.graphql` is a copy of what its server
//! advertises as `_service { sdl }`. `scriptorium-ontology add-remote`
//! takes the first copy and `pull` refreshes it; both refuse a schema that
//! does not compose with the rest of the ontology, and the commit that
//! records it is the version every later `compose` and `check` reads.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};

/// Largest response accepted from a remote subgraph, in bytes.
pub const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Timeout for one schema fetch, connection included.
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Check that `url` is an `http(s)` URL a schema can be fetched from, and
/// that it carries no credentials: it is committed, and it is copied into
/// the supergraph.
pub fn validate_url(url: &str) -> std::result::Result<(), String> {
    let parsed = reqwest::Url::parse(url)
        .map_err(|e| format!("`{}` is not a URL: {e}", display_url(url)))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(format!(
            "`{}` must be an http:// or https:// URL",
            display_url(url)
        ));
    }
    if parsed.host_str().is_none() {
        return Err(format!("`{}` has no host", display_url(url)));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(format!(
            "`{}`: a remote URL must not carry a user or password (it is committed); pass credentials with --header",
            display_url(url)
        ));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(format!(
            "`{}`: a remote URL must not have a query or fragment (it is committed, and queries often carry tokens); pass credentials with --header",
            display_url(url)
        ));
    }
    Ok(())
}

/// Headers to send to remote sub-ontologies, local to one clone: kept in
/// the git directory (`<git-dir>/scriptorium-ontology/remotes.toml`), never
/// committed. Each binding holds environment variable *names*, and is tied
/// to the URL it was made for, so a commit that repoints a remote cannot
/// redirect the operator's credentials.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    /// Sub-ontology directory → URL → header → environment variable. Keyed
    /// by directory, not name, so two ontologies in one repository keep
    /// their own; by URL too, so linked worktrees pinning one directory at
    /// different URLs each keep theirs.
    #[serde(default)]
    pub remote: BTreeMap<String, BTreeMap<String, BTreeMap<String, String>>>,
}

impl Credentials {
    /// Where the bindings of the repository at `repo_root` live.
    pub fn path(repo_root: &Path) -> Result<PathBuf> {
        let repo = git2::Repository::open(repo_root)?;
        Ok(repo
            .commondir()
            .join("scriptorium-ontology")
            .join("remotes.toml"))
    }

    pub fn load(repo_root: &Path) -> Result<Self> {
        let path = Self::path(repo_root)?;
        match std::fs::read_to_string(&path) {
            Ok(text) => toml::from_str(&text).map_err(|e| Error::Invalid {
                what: "remote credentials",
                path: path.display().to_string(),
                message: e.to_string(),
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(Error::io(path.display().to_string(), e)),
        }
    }

    pub fn save(&self, repo_root: &Path) -> Result<()> {
        let path = Self::path(repo_root)?;
        let io = |e| Error::io(path.display().to_string(), e);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(io)?;
        }
        let text = format!(
            "# Headers scriptorium-ontology sends to remote sub-ontologies: environment\n# variable names, each bound to one URL. Local to this clone; never committed.\n{}",
            toml::to_string(self).map_err(|e| Error::Invalid {
                what: "remote credentials",
                path: path.display().to_string(),
                message: e.to_string(),
            })?
        );
        // Replaced whole, never truncated in place: a failed write leaves
        // every existing binding as it was.
        let temp = path.with_extension(format!("toml.{}.tmp", std::process::id()));
        let written = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .and_then(|mut file| {
                use std::io::Write as _;
                file.write_all(text.as_bytes())?;
                file.sync_all()
            })
            .and_then(|()| std::fs::rename(&temp, &path));
        if written.is_err() {
            let _ = std::fs::remove_file(&temp);
        }
        written.map_err(io)
    }

    /// Bind `headers` to the remote at `dir` served from `url`.
    pub fn bind(&mut self, dir: &str, url: &str, headers: BTreeMap<String, String>) {
        self.remote
            .entry(dir.to_string())
            .or_default()
            .insert(url.to_string(), headers);
    }

    /// The headers to send to `url` for the remote `name` at `dir`: those
    /// bound for this URL; none when `dir` has no binding. A binding for
    /// another URL only is an error: the manifest was repointed.
    pub fn headers_for(
        &self,
        dir: &str,
        name: &str,
        url: &str,
    ) -> std::result::Result<BTreeMap<String, String>, String> {
        let Some(urls) = self.remote.get(dir).filter(|urls| !urls.is_empty()) else {
            return Ok(BTreeMap::new());
        };
        if let Some(headers) = urls.get(url) {
            return Ok(headers.clone());
        }
        Err(format!(
            "its credentials are bound to {}, but the manifest now points to {}; review that change, then bind them again with `scriptorium-ontology pull {name} --header …`",
            urls.keys()
                .map(|bound| display_url(bound))
                .collect::<Vec<_>>()
                .join(", "),
            display_url(url)
        ))
    }
}

/// Parse `--header NAME=ENV`: send header `NAME` with the value of the
/// environment variable `ENV`.
pub fn parse_header_arg(arg: &str) -> std::result::Result<(String, String), String> {
    let (name, var) = arg.split_once('=').ok_or_else(|| {
        format!("`{arg}`: expected HEADER=ENV_VAR (the variable holds the value)")
    })?;
    reqwest::header::HeaderName::from_bytes(name.as_bytes())
        .map_err(|_| format!("`{name}` is not a valid header name"))?;
    let mut chars = var.chars();
    let valid_var = matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !valid_var {
        return Err(format!(
            "`{var}` is not an environment variable name (letters, digits, `_`)"
        ));
    }
    Ok((name.to_string(), var.to_string()))
}

/// Fetch `_service { sdl }` from the subgraph at `url`.
///
/// `headers_env` maps a header name to the environment variable holding
/// its value. Headers are sent over plain `http` only to a loopback host,
/// and then never through a proxy; redirects are not followed. Any of
/// those could hand them to someone else.
pub async fn fetch_sdl(url: &str, headers_env: &BTreeMap<String, String>) -> Result<String> {
    let fail = |message: String| Error::Remote {
        url: display_url(url),
        message,
    };
    validate_url(url).map_err(&fail)?;
    let parsed = reqwest::Url::parse(url).map_err(|e| fail(e.to_string()))?;
    if !headers_env.is_empty() && parsed.scheme() == "http" && !is_loopback(&parsed) {
        return Err(fail(
            "refusing to send headers over plain http to a non-local host; use https".into(),
        ));
    }
    let mut headers = reqwest::header::HeaderMap::new();
    for (name, var) in headers_env {
        let header = reqwest::header::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| fail(format!("`{name}` is not a valid header name")))?;
        if headers.contains_key(&header) {
            // Names ignore case; sending one of two bound values silently
            // would hide which credential the remote saw.
            return Err(fail(format!(
                "header `{name}` is bound twice (header names ignore case)"
            )));
        }
        let value = std::env::var(var)
            .map_err(|_| fail(format!("header `{name}` reads ${var}, which is not set")))?;
        let mut value = reqwest::header::HeaderValue::from_str(&value)
            .map_err(|_| fail(format!("${var} is not a valid value for header `{name}`")))?;
        value.set_sensitive(true);
        headers.insert(header, value);
    }
    let mut client = reqwest::Client::builder()
        .timeout(FETCH_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none());
    if parsed.scheme() == "http" && !headers.is_empty() {
        // A proxy would see plain-http headers; go to the loopback host directly.
        client = client.no_proxy();
    }
    let client = client.build().map_err(|e| fail(e.to_string()))?;
    let mut response = client
        .post(parsed)
        .headers(headers)
        .json(&serde_json::json!({ "query": "query ScriptoriumPin { _service { sdl } }" }))
        .send()
        .await
        .map_err(|e| fail(e.without_url().to_string()))?;
    let status = response.status();
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| fail(e.without_url().to_string()))?
    {
        if body.len() + chunk.len() > MAX_RESPONSE_BYTES {
            return Err(fail(format!(
                "the response is larger than {} MiB",
                MAX_RESPONSE_BYTES >> 20
            )));
        }
        body.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        return Err(fail(format!("HTTP {status}")));
    }
    parse_service_response(&body).map_err(fail)
}

/// The SDL in a `_service { sdl }` response body.
fn parse_service_response(body: &[u8]) -> std::result::Result<String, String> {
    let json: Value = serde_json::from_slice(body)
        .map_err(|e| format!("the response is not GraphQL JSON: {e}"))?;
    let errors: Vec<&str> = json
        .get("errors")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|e| e.get("message").and_then(Value::as_str))
        .take(5)
        .collect();
    if !errors.is_empty() {
        return Err(format!(
            "the subgraph answered with errors: {}",
            errors.join("; ")
        ));
    }
    json.pointer("/data/_service/sdl")
        .and_then(Value::as_str)
        .filter(|sdl| !sdl.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            "the response has no `_service { sdl }`; is this a federation subgraph?".to_string()
        })
}

fn is_loopback(url: &reqwest::Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// `url` without credentials, query, or fragment, for messages.
pub fn display_url(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(mut parsed) => {
            let _ = parsed.set_username("");
            let _ = parsed.set_password(None);
            parsed.set_query(None);
            parsed.set_fragment(None);
            parsed.to_string()
        }
        Err(_) => "(invalid URL)".to_string(),
    }
}

/// Lines added and removed between two schema versions.
pub fn line_changes(old: &str, new: &str) -> (usize, usize) {
    similar::TextDiff::from_lines(old, new)
        .iter_all_changes()
        .fold((0, 0), |(added, removed), change| match change.tag() {
            similar::ChangeTag::Insert => (added + 1, removed),
            similar::ChangeTag::Delete => (added, removed + 1),
            similar::ChangeTag::Equal => (added, removed),
        })
}

/// A unified diff of `path` from `old` to `new`.
pub fn unified_diff(old: &str, new: &str, path: &str) -> String {
    similar::TextDiff::from_lines(old, new)
        .unified_diff()
        .context_radius(3)
        .header(&format!("a/{path}"), &format!("b/{path}"))
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_args_name_a_header_and_a_variable() {
        assert_eq!(
            parse_header_arg("Authorization=PEOPLE_TOKEN").unwrap(),
            ("Authorization".into(), "PEOPLE_TOKEN".into())
        );
        for bad in ["Authorization", "Bad Header=X", "X-Key=1TOKEN", "X-Key=A-B"] {
            assert!(parse_header_arg(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn urls_must_be_http_with_a_host_and_no_credentials() {
        assert!(validate_url("https://people.example/graphql").is_ok());
        assert!(validate_url("http://127.0.0.1:4000/graphql").is_ok());
        for bad in [
            "ftp://people.example/graphql",
            "people.example",
            "file:///etc/passwd",
            "https://user:secret@people.example/graphql",
            "https://people.example/graphql?token=secret",
            "https://people.example/graphql#x",
        ] {
            let err = validate_url(bad).unwrap_err();
            assert!(!err.contains("secret"), "{bad}: {err}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_failed_save_keeps_the_bindings() {
        use std::os::unix::fs::PermissionsExt;
        let repo = tempfile::tempdir().unwrap();
        git2::Repository::init(repo.path()).unwrap();
        let binding = |env: &str| {
            let mut credentials = Credentials::default();
            credentials.bind(
                "ontology/people",
                "https://people.example/graphql",
                BTreeMap::from([("Authorization".into(), env.into())]),
            );
            credentials
        };
        binding("FIRST_TOKEN").save(repo.path()).unwrap();
        let path = Credentials::path(repo.path()).unwrap();
        let dir = path.parent().unwrap().to_path_buf();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        let probe = dir.join(".probe");
        let binds = std::fs::write(&probe, "").is_err();
        let _ = std::fs::remove_file(&probe);
        let saved = binding("SECOND_TOKEN").save(repo.path());
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        if !binds {
            // Permissions do not bind here (root): nothing can fail.
            return;
        }
        assert!(saved.is_err(), "the replacement cannot be written");
        assert_eq!(
            Credentials::load(repo.path()).unwrap(),
            binding("FIRST_TOKEN")
        );
    }

    #[test]
    fn bindings_only_follow_the_url_they_were_made_for() {
        let token = |env: &str| BTreeMap::from([("Authorization".to_string(), env.to_string())]);
        let mut credentials = Credentials::default();
        credentials.bind(
            "ontology/people",
            "https://people.example/graphql",
            token("PEOPLE_TOKEN"),
        );
        let headers = |dir: &str, url: &str| credentials.headers_for(dir, "people", url);
        assert_eq!(
            headers("ontology/people", "https://people.example/graphql").unwrap(),
            token("PEOPLE_TOKEN")
        );
        let err = headers("ontology/people", "https://attacker.example/graphql").unwrap_err();
        assert!(
            err.contains("bound to https://people.example/graphql"),
            "{err}"
        );
        assert!(
            headers("ontology/other", "https://attacker.example/graphql")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn bindings_are_kept_per_directory_and_url() {
        let token = |env: &str| BTreeMap::from([("Authorization".to_string(), env.to_string())]);
        let mut credentials = Credentials::default();
        // Two ontologies in one repository, each with a remote `people`…
        credentials.bind(
            "ontology/people",
            "https://a.example/graphql",
            token("A_TOKEN"),
        );
        credentials.bind(
            "other/people",
            "https://b.example/graphql",
            token("B_TOKEN"),
        );
        // …and a linked worktree pinning the first at another URL.
        credentials.bind(
            "ontology/people",
            "https://c.example/graphql",
            token("C_TOKEN"),
        );
        let headers = |dir: &str, url: &str| credentials.headers_for(dir, "people", url).unwrap();
        assert_eq!(
            headers("ontology/people", "https://a.example/graphql"),
            token("A_TOKEN")
        );
        assert_eq!(
            headers("other/people", "https://b.example/graphql"),
            token("B_TOKEN")
        );
        assert_eq!(
            headers("ontology/people", "https://c.example/graphql"),
            token("C_TOKEN")
        );
        // Round-trips through the file.
        let text = toml::to_string(&credentials).unwrap();
        assert_eq!(toml::from_str::<Credentials>(&text).unwrap(), credentials);
    }

    #[test]
    fn messages_never_show_url_credentials() {
        assert_eq!(
            display_url("https://user:secret@people.example/graphql?token=abc#x"),
            "https://people.example/graphql"
        );
    }

    #[tokio::test]
    async fn headers_are_not_sent_over_plain_http_to_other_hosts() {
        let headers = BTreeMap::from([("Authorization".to_string(), "PATH".to_string())]);
        let err = fetch_sdl("http://people.example/graphql", &headers)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("use https"), "{err}");
    }

    #[tokio::test]
    async fn headers_differing_only_in_case_are_refused() {
        let headers = BTreeMap::from([
            ("Authorization".to_string(), "PATH".to_string()),
            ("authorization".to_string(), "HOME".to_string()),
        ]);
        let err = fetch_sdl("http://127.0.0.1:9/graphql", &headers)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("bound twice"), "{err}");
    }

    #[tokio::test]
    async fn unset_header_variables_are_named_not_guessed() {
        let headers = BTreeMap::from([(
            "Authorization".to_string(),
            "SCRIPTORIUM_ONTOLOGY_TEST_UNSET_TOKEN".to_string(),
        )]);
        let err = fetch_sdl("http://127.0.0.1:9/graphql", &headers)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("$SCRIPTORIUM_ONTOLOGY_TEST_UNSET_TOKEN, which is not set"),
            "{err}"
        );
    }

    #[test]
    fn service_responses_yield_the_sdl_or_say_why_not() {
        assert_eq!(
            parse_service_response(br#"{"data":{"_service":{"sdl":"type Query { a: Int }"}}}"#)
                .unwrap(),
            "type Query { a: Int }"
        );
        let err = parse_service_response(br#"{"errors":[{"message":"no _service"}]}"#).unwrap_err();
        assert!(err.contains("no _service"), "{err}");
        let err = parse_service_response(br#"{"data":{"other":1}}"#).unwrap_err();
        assert!(err.contains("federation subgraph"), "{err}");
        assert!(parse_service_response(b"<html>").is_err());
    }

    #[test]
    fn diffs_count_changed_lines() {
        let old = "type A {\n  a: Int\n}\n";
        let new = "type A {\n  a: Int\n  b: Int\n}\n";
        assert_eq!(line_changes(old, new), (1, 0));
        let diff = unified_diff(old, new, "ontology/x/schema.graphql");
        assert!(diff.contains("+++ b/ontology/x/schema.graphql"), "{diff}");
        assert!(diff.contains("+  b: Int"), "{diff}");
    }
}
