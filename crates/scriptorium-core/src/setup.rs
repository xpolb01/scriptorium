//! Setup wizard: interactive configuration of providers, API keys, and
//! vault settings.
//!
//! Stores API keys in the macOS keychain (via [`crate::keychain`]) and
//! writes provider/model choices to `.scriptorium/config.toml`.

use std::io::{BufRead, Write};

use crate::config::{ChunkStrategy, Config, EmbeddingsConfig, GitConfig, LlmConfig, PathsConfig};
use crate::error::{Error, Result};
use crate::hooks::HooksConfig;
use crate::keychain;
use crate::vault::Vault;

/// Provider choice for the wizard.
#[derive(Debug, Clone, Copy)]
pub enum Provider {
    Claude,
    OpenAi,
    Gemini,
    Ollama,
}

impl Provider {
    fn config_name(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::OpenAi => "openai",
            Self::Gemini => "gemini",
            Self::Ollama => "ollama",
        }
    }

    fn needs_api_key(self) -> bool {
        !matches!(self, Self::Ollama)
    }

    fn key_env_var(self) -> &'static str {
        match self {
            Self::Claude => "SCRIPTORIUM_ANTHROPIC_API_KEY",
            Self::OpenAi => "OPENAI_API_KEY",
            Self::Gemini => "SCRIPTORIUM_GOOGLE_API_KEY",
            Self::Ollama => "",
        }
    }

    fn keychain_service(self) -> &'static str {
        match self {
            Self::Claude => keychain::services::ANTHROPIC,
            Self::OpenAi => keychain::services::OPENAI,
            Self::Gemini => keychain::services::GOOGLE,
            Self::Ollama => "",
        }
    }

    fn default_model(self) -> &'static str {
        match self {
            Self::Claude => "claude-opus-4-6",
            Self::OpenAi => "gpt-4o-mini",
            Self::Gemini => "gemini-2.5-pro",
            Self::Ollama => "llama3.1",
        }
    }

    fn default_embed_model(self) -> &'static str {
        match self {
            Self::Claude => "", // Claude has no embeddings
            Self::OpenAi => "text-embedding-3-small",
            Self::Gemini => "gemini-embedding-2-preview",
            Self::Ollama => "nomic-embed-text",
        }
    }

    fn supports_embeddings(self) -> bool {
        !matches!(self, Self::Claude)
    }
}

/// Result of running the setup wizard.
#[derive(Debug)]
pub struct SetupResult {
    pub llm_provider: String,
    pub embed_provider: String,
    pub keys_stored: usize,
    pub config_written: bool,
}

/// Run the interactive setup wizard.
///
/// Reads from `input` and writes prompts to `output` so it can be tested.
pub fn run_setup(
    vault: &Vault,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
) -> Result<SetupResult> {
    writeln!(output, "Scriptorium Setup Wizard").ok();
    writeln!(output, "========================\n").ok();

    // Step 1: Choose LLM provider.
    writeln!(output, "Step 1: Choose your LLM provider for chat/ingest:").ok();
    writeln!(output, "  1) Claude (Anthropic) — recommended").ok();
    writeln!(output, "  2) OpenAI (GPT-4o)").ok();
    writeln!(output, "  3) Gemini (Google)").ok();
    writeln!(output, "  4) Ollama (local, no API key)").ok();
    write!(output, "Choice [1]: ").ok();
    output.flush().ok();
    let llm_provider = read_provider_choice(input, Provider::Claude);

    // Step 2: API key for LLM provider.
    let mut keys_stored = 0;
    if llm_provider.needs_api_key() {
        keys_stored += prompt_and_store_key(input, output, llm_provider, "LLM")?;
    }

    // Step 3: Choose embeddings provider.
    writeln!(output).ok();
    writeln!(output, "Step 2: Choose your embeddings provider:").ok();
    if llm_provider.supports_embeddings() {
        writeln!(
            output,
            "  (press Enter to use same as LLM: {})",
            llm_provider.config_name()
        )
        .ok();
    } else {
        writeln!(
            output,
            "  (Claude doesn't support embeddings — pick another)"
        )
        .ok();
    }
    writeln!(output, "  1) OpenAI (text-embedding-3-small)").ok();
    writeln!(
        output,
        "  2) Gemini (gemini-embedding-2-preview) — recommended"
    )
    .ok();
    writeln!(output, "  3) Ollama (nomic-embed-text, local)").ok();
    let default_embed = if llm_provider.supports_embeddings() {
        llm_provider
    } else {
        Provider::Gemini
    };
    let default_label = match default_embed {
        Provider::OpenAi => "1",
        Provider::Gemini | Provider::Claude => "2",
        Provider::Ollama => "3",
    };
    write!(output, "Choice [{default_label}]: ").ok();
    output.flush().ok();
    let embed_provider = read_embed_choice(input, default_embed);

    // Step 4: API key for embeddings provider (if different from LLM).
    if embed_provider.needs_api_key()
        && embed_provider.keychain_service() != llm_provider.keychain_service()
    {
        keys_stored += prompt_and_store_key(input, output, embed_provider, "embeddings")?;
    }

    // Step 5: Write config.toml.
    writeln!(output).ok();
    let config = Config {
        llm: LlmConfig {
            provider: llm_provider.config_name().into(),
            model: llm_provider.default_model().into(),
            timeout_secs: 120,
        },
        embeddings: EmbeddingsConfig {
            provider: embed_provider.config_name().into(),
            model: embed_provider.default_embed_model().into(),
            chunk_strategy: ChunkStrategy::default(),
        },
        git: GitConfig::default(),
        paths: PathsConfig::default(),
        hooks: HooksConfig::default(),
    };

    let config_path = vault.meta_dir().join("config.toml");
    std::fs::create_dir_all(vault.meta_dir().as_std_path())
        .map_err(|e| Error::io(vault.meta_dir().into_std_path_buf(), e))?;
    let toml_str = toml::to_string_pretty(&config)
        .map_err(|e| Error::Other(anyhow::anyhow!("serialize config: {e}")))?;
    std::fs::write(config_path.as_std_path(), &toml_str)
        .map_err(|e| Error::io(config_path.clone().into_std_path_buf(), e))?;

    writeln!(output, "Config written to {config_path}").ok();
    writeln!(output, "\nSetup complete! Try: scriptorium doctor").ok();

    Ok(SetupResult {
        llm_provider: llm_provider.config_name().into(),
        embed_provider: embed_provider.config_name().into(),
        keys_stored,
        config_written: true,
    })
}

fn read_provider_choice(input: &mut dyn BufRead, default: Provider) -> Provider {
    let mut line = String::new();
    if input.read_line(&mut line).is_err() {
        return default;
    }
    match line.trim() {
        "1" | "" => Provider::Claude,
        "2" => Provider::OpenAi,
        "3" => Provider::Gemini,
        "4" => Provider::Ollama,
        _ => default,
    }
}

fn read_embed_choice(input: &mut dyn BufRead, default: Provider) -> Provider {
    let mut line = String::new();
    if input.read_line(&mut line).is_err() {
        return default;
    }
    match line.trim() {
        "1" => Provider::OpenAi,
        "2" => Provider::Gemini,
        "3" => Provider::Ollama,
        _ => default,
    }
}

fn prompt_and_store_key(
    input: &mut dyn BufRead,
    output: &mut dyn Write,
    provider: Provider,
    role: &str,
) -> Result<usize> {
    // Check if key already exists in keychain.
    if let Some(_existing) = keychain::get_key(provider.keychain_service()) {
        write!(
            output,
            "  {} API key found in keychain. Replace? [y/N]: ",
            provider.config_name()
        )
        .ok();
        output.flush().ok();
        let mut line = String::new();
        let _ = input.read_line(&mut line);
        if !line.trim().eq_ignore_ascii_case("y") {
            writeln!(output, "  Keeping existing key.").ok();
            return Ok(0);
        }
    }

    write!(
        output,
        "  Enter your {} API key for {}: ",
        provider.config_name(),
        role,
    )
    .ok();
    output.flush().ok();
    let mut key = String::new();
    input
        .read_line(&mut key)
        .map_err(|e| Error::Other(anyhow::anyhow!("read key: {e}")))?;
    let key = key.trim();
    if key.is_empty() {
        writeln!(
            output,
            "  Skipped (no key entered). Set {} later.",
            provider.key_env_var()
        )
        .ok();
        return Ok(0);
    }

    if keychain::set_key(provider.keychain_service(), key) {
        writeln!(
            output,
            "  Stored in keychain as '{}'.",
            provider.keychain_service()
        )
        .ok();
        Ok(1)
    } else {
        writeln!(
            output,
            "  Warning: could not store in keychain. Set {} in your shell profile.",
            provider.key_env_var()
        )
        .ok();
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn provider_properties_are_consistent() {
        // Claude: chat only, no embeddings.
        assert!(!Provider::Claude.supports_embeddings());
        assert!(Provider::Claude.needs_api_key());

        // Gemini: both chat and embeddings.
        assert!(Provider::Gemini.supports_embeddings());
        assert!(Provider::Gemini.needs_api_key());

        // Ollama: no API key.
        assert!(!Provider::Ollama.needs_api_key());
        assert!(Provider::Ollama.supports_embeddings());
    }

    #[test]
    fn setup_with_defaults_writes_config() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("wiki")).unwrap();
        let vault = Vault::open(dir.path()).unwrap();

        // Simulate: press Enter for every prompt (accept all defaults).
        let mut input = Cursor::new("1\n\n2\n\n");
        let mut output = Vec::new();

        let result = run_setup(&vault, &mut input, &mut output).unwrap();
        assert!(result.config_written);
        assert_eq!(result.llm_provider, "claude");

        // Verify config was written.
        let config_path = vault.meta_dir().join("config.toml");
        assert!(config_path.as_std_path().exists());
    }
}
