//! Self-installing launchd scheduler for `scriptorium ingest-drain`.
//!
//! `install()` resolves the binary path, fetches API credentials from the
//! existing keychain mechanism, and writes a plist with those credentials
//! baked into `EnvironmentVariables`. This avoids needing launchd to
//! reach into the user's login keychain at runtime — launchd-spawned
//! jobs run without a Security Server session and `security
//! find-generic-password` returns empty.

use std::fs;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{anyhow, Context, Result};
use serde::Serialize;

const PLIST_LABEL: &str = "com.bogdan.scriptorium-drain";

const PLIST_TEMPLATE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>{LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>{BINARY}</string>
    <string>-C</string>
    <string>{VAULT}</string>
    <string>ingest-drain</string>
  </array>
  <key>StartInterval</key><integer>60</integer>
  <key>RunAtLoad</key><false/>
  <key>SessionCreate</key><true/>
  <key>StandardOutPath</key><string>{LOG_PATH}</string>
  <key>StandardErrorPath</key><string>{LOG_PATH}</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key><string>/Users/bogdan/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
{ENV_VARS_XML}  </dict>
</dict>
</plist>
"#;

const KEYCHAIN_SPECS: &[(&str, &str)] = &[
    ("SCRIPTORIUM_ANTHROPIC_API_KEY", "scriptorium-anthropic"),
    ("SCRIPTORIUM_GOOGLE_API_KEY", "scriptorium-google"),
    ("OPENAI_API_KEY", "scriptorium-openai"),
];

pub struct InstallOpts {
    pub vault: Option<PathBuf>,
    pub reinstall: bool,
}

#[derive(Serialize)]
pub struct InstallReport {
    pub label: String,
    pub plist_path: PathBuf,
    pub binary: PathBuf,
    pub vault: PathBuf,
    pub log_path: PathBuf,
    pub injected_env_keys: Vec<String>,
    pub bootout_status: String,
    pub bootstrap_status: String,
}

#[derive(Serialize)]
pub struct UninstallReport {
    pub label: String,
    pub plist_path: PathBuf,
    pub plist_existed: bool,
    pub bootout_status: String,
}

#[derive(Serialize)]
pub struct StatusReport {
    pub label: String,
    pub loaded: bool,
    pub plist_path: PathBuf,
    pub plist_exists: bool,
    pub launchctl_print: String,
    pub recent_log: String,
    pub queue_stats: Option<scriptorium_core::ingest_queue::QueueStats>,
}

fn home() -> Result<PathBuf> {
    Ok(PathBuf::from(
        std::env::var("HOME").context("HOME env var not set")?,
    ))
}

fn get_uid() -> Result<u32> {
    let out = Command::new("id").arg("-u").output()?;
    if !out.status.success() {
        return Err(anyhow!("id -u failed"));
    }
    let s = String::from_utf8(out.stdout).context("id -u output not utf8")?;
    s.trim().parse().context("uid parse")
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn render_plist(binary: &str, vault: &str, log_path: &str, env_vars_xml: &str) -> String {
    PLIST_TEMPLATE
        .replace("{LABEL}", PLIST_LABEL)
        .replace("{BINARY}", binary)
        .replace("{VAULT}", vault)
        .replace("{LOG_PATH}", log_path)
        .replace("{ENV_VARS_XML}", env_vars_xml)
}

fn build_env_vars_xml(injected: &mut Vec<String>) -> String {
    let mut xml = String::new();
    for (env_var, service) in KEYCHAIN_SPECS {
        if let Some(value) = scriptorium_core::keychain::resolve_key(env_var, service) {
            xml.push_str(&format!(
                "    <key>{}</key><string>{}</string>\n",
                env_var,
                xml_escape(&value)
            ));
            injected.push((*env_var).to_string());
        }
    }
    xml
}

fn capture(out: std::io::Result<std::process::Output>) -> String {
    match out {
        Ok(o) => format!(
            "exit={} stdout={} stderr={}",
            o.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&o.stdout).trim(),
            String::from_utf8_lossy(&o.stderr).trim()
        ),
        Err(e) => format!("spawn failed: {e}"),
    }
}

pub fn install(opts: InstallOpts) -> Result<InstallReport> {
    let binary = std::env::current_exe()?
        .canonicalize()
        .context("canonicalize current_exe")?;
    let home = home()?;
    let vault = opts.vault.unwrap_or_else(|| home.join("scriptorium-vault"));
    let log_path = home.join(".claude/artifacts/scriptorium-drain.log");
    let plist_path = home
        .join("Library/LaunchAgents")
        .join(format!("{PLIST_LABEL}.plist"));

    if let Some(parent) = plist_path.parent() {
        fs::create_dir_all(parent).context("create LaunchAgents dir")?;
    }
    if let Some(parent) = log_path.parent() {
        let _ = fs::create_dir_all(parent);
    }

    let mut injected_env_keys: Vec<String> = Vec::new();
    let env_vars_xml = build_env_vars_xml(&mut injected_env_keys);
    let plist_content = render_plist(
        &binary.display().to_string(),
        &vault.display().to_string(),
        &log_path.display().to_string(),
        &env_vars_xml,
    );

    let uid = get_uid()?;
    let target = format!("gui/{uid}");
    let plist_str = plist_path.display().to_string();

    let bootout_status = if plist_path.exists() || opts.reinstall {
        capture(
            Command::new("launchctl")
                .args(["bootout", &target, &plist_str])
                .output(),
        )
    } else {
        "(skipped: not loaded)".to_string()
    };

    let mut f = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&plist_path)
        .context("open plist for write")?;
    f.write_all(plist_content.as_bytes())
        .context("write plist")?;
    f.sync_all().ok();
    drop(f);

    let out = Command::new("launchctl")
        .args(["bootstrap", &target, &plist_str])
        .output()
        .context("launchctl bootstrap spawn")?;
    let bootstrap_status = capture(Ok(out.clone()));
    if !out.status.success() {
        return Err(anyhow!(
            "launchctl bootstrap failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }

    Ok(InstallReport {
        label: PLIST_LABEL.to_string(),
        plist_path,
        binary,
        vault,
        log_path,
        injected_env_keys,
        bootout_status,
        bootstrap_status,
    })
}

pub fn uninstall() -> Result<UninstallReport> {
    let home = home()?;
    let plist_path = home
        .join("Library/LaunchAgents")
        .join(format!("{PLIST_LABEL}.plist"));
    let plist_existed = plist_path.exists();

    let uid = get_uid()?;
    let target = format!("gui/{uid}");
    let plist_str = plist_path.display().to_string();

    let bootout_status = capture(
        Command::new("launchctl")
            .args(["bootout", &target, &plist_str])
            .output(),
    );

    if plist_existed {
        let _ = fs::remove_file(&plist_path);
    }

    Ok(UninstallReport {
        label: PLIST_LABEL.to_string(),
        plist_path,
        plist_existed,
        bootout_status,
    })
}

pub fn status(vault: Option<PathBuf>) -> Result<StatusReport> {
    let home = home()?;
    let plist_path = home
        .join("Library/LaunchAgents")
        .join(format!("{PLIST_LABEL}.plist"));
    let log_path = home.join(".claude/artifacts/scriptorium-drain.log");

    let uid = get_uid()?;
    let target_label = format!("gui/{uid}/{PLIST_LABEL}");

    let print_out = Command::new("launchctl")
        .args(["print", &target_label])
        .output();
    let (loaded, launchctl_print) = match &print_out {
        Ok(o) => (
            o.status.success(),
            format!(
                "{}{}",
                String::from_utf8_lossy(&o.stdout),
                String::from_utf8_lossy(&o.stderr)
            ),
        ),
        Err(e) => (false, format!("launchctl print failed: {e}")),
    };

    let recent_log = if log_path.exists() {
        match Command::new("tail")
            .args(["-n", "20", log_path.to_str().unwrap_or("")])
            .output()
        {
            Ok(o) => String::from_utf8_lossy(&o.stdout).into_owned(),
            Err(e) => format!("tail failed: {e}"),
        }
    } else {
        "(no log file yet)".to_string()
    };

    let vault_path = vault.unwrap_or_else(|| home.join("scriptorium-vault"));
    let queue_stats = scriptorium_core::Vault::open(&vault_path)
        .ok()
        .and_then(|v| scriptorium_core::ingest_queue::queue_stats(&v).ok());

    Ok(StatusReport {
        label: PLIST_LABEL.to_string(),
        loaded,
        plist_path: plist_path.clone(),
        plist_exists: plist_path.exists(),
        launchctl_print,
        recent_log,
        queue_stats,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plist_template_substitutes_placeholders() {
        let out = render_plist("/bin/foo", "/v", "/log", "");
        assert!(out.contains("<string>/bin/foo</string>"));
        assert!(out.contains("<string>/v</string>"));
        assert!(out.contains("<string>/log</string>"));
        assert!(out.contains("<string>com.bogdan.scriptorium-drain</string>"));
        assert!(!out.contains("{LABEL}"));
        assert!(!out.contains("{BINARY}"));
        assert!(!out.contains("{VAULT}"));
        assert!(!out.contains("{LOG_PATH}"));
        assert!(!out.contains("{ENV_VARS_XML}"));
    }

    #[test]
    fn plist_template_escapes_xml_entities() {
        let escaped = xml_escape("<bad>&amp\"'");
        assert_eq!(escaped, "&lt;bad&gt;&amp;amp&quot;&apos;");
        let env_block = format!(
            "    <key>K</key><string>{}</string>\n",
            xml_escape("a&b<c>")
        );
        let out = render_plist("/b", "/v", "/l", &env_block);
        assert!(out.contains("a&amp;b&lt;c&gt;"));
        assert!(!out.contains("a&b<c>"));
    }

    #[test]
    fn plist_includes_session_create() {
        let out = render_plist("/b", "/v", "/l", "");
        assert!(out.contains("<key>SessionCreate</key><true/>"));
    }

    #[test]
    fn plist_template_renders_env_vars_block() {
        let env_block = "    <key>FOO_KEY</key><string>val</string>\n";
        let out = render_plist("/b", "/v", "/l", env_block);
        assert!(out.contains("<key>FOO_KEY</key><string>val</string>"));
        assert!(out.contains("<key>EnvironmentVariables</key>"));
    }
}
