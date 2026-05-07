//! Facebook data export → scriptorium markdown source files.
//!
//! Accepts one or more Facebook export directories (the platform splits large
//! exports across multiple ZIPs). Scans for JSON data files, fixes Facebook's
//! broken UTF-8 encoding, and writes one markdown file per logical unit
//! (conversation, year of posts, etc.) into the vault's `sources/data/facebook/`
//! directory.
//!
//! ## Encoding
//!
//! Facebook JSON exports store non-ASCII text as mojibake: the original UTF-8
//! bytes are reinterpreted as latin-1 code points, then JSON-escaped. Every
//! string field must be decoded via [`fix_encoding`] before use.
//!
//! ## Usage
//!
//! ```text
//! scriptorium social facebook <EXPORT_DIRS>... [--output-dir <path>] [--dry-run]
//! ```

use std::collections::BTreeMap;
use std::fmt::Write as FmtWrite;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::{TimeZone, Utc};
use serde::Deserialize;
use tracing::{info, warn};

use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Which categories to import.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    Messages,
    Posts,
    Comments,
    Friends,
    Search,
    Events,
    Groups,
}

impl Category {
    pub fn all() -> Vec<Self> {
        vec![
            Self::Messages,
            Self::Posts,
            Self::Comments,
            Self::Friends,
            Self::Search,
            Self::Events,
            Self::Groups,
        ]
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "messages" => Some(Self::Messages),
            "posts" => Some(Self::Posts),
            "comments" => Some(Self::Comments),
            "friends" => Some(Self::Friends),
            "search" => Some(Self::Search),
            "events" => Some(Self::Events),
            "groups" => Some(Self::Groups),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Messages => "messages",
            Self::Posts => "posts",
            Self::Comments => "comments",
            Self::Friends => "friends",
            Self::Search => "search",
            Self::Events => "events",
            Self::Groups => "groups",
        }
    }
}

/// Options for [`import`].
pub struct FacebookImportOptions {
    /// One or more Facebook export directories.
    pub export_dirs: Vec<PathBuf>,
    /// Where to write markdown source files.
    pub output_dir: PathBuf,
    /// Which categories to process (empty = all).
    pub categories: Vec<Category>,
    /// If true, report what would be generated without writing.
    pub dry_run: bool,
}

/// Per-category stats.
#[derive(Debug, Clone)]
pub struct CategoryReport {
    pub category: String,
    pub files_written: usize,
    pub items_processed: usize,
}

/// An error that didn't stop the whole import.
#[derive(Debug, Clone)]
pub struct ImportError {
    pub path: String,
    pub error: String,
}

/// Report returned by [`import`].
#[derive(Debug, Clone)]
pub struct FacebookImportReport {
    pub categories: Vec<CategoryReport>,
    pub total_files_written: usize,
    pub total_bytes: u64,
    pub elapsed: Duration,
    pub errors: Vec<ImportError>,
}

/// Run the Facebook import pipeline.
///
/// `progress` is called with `(current_step, total_steps, description)`.
pub fn import(
    options: &FacebookImportOptions,
    progress: impl Fn(usize, usize, &str),
) -> Result<FacebookImportReport> {
    let start = Instant::now();

    // Find the JSON root — the export dir that contains structured data.
    let json_root = find_json_root(&options.export_dirs)?;
    info!("JSON root: {}", json_root.display());

    let categories = if options.categories.is_empty() {
        Category::all()
    } else {
        options.categories.clone()
    };

    let total = categories.len();
    let mut reports = Vec::new();
    let mut errors = Vec::new();
    let mut total_files = 0usize;
    let mut total_bytes = 0u64;

    for (i, cat) in categories.iter().enumerate() {
        progress(i + 1, total, cat.name());

        let out_dir = options.output_dir.join(cat.name());

        match process_category(*cat, &json_root, &out_dir, options.dry_run) {
            Ok(report) => {
                total_files += report.files_written;
                total_bytes += report.items_processed as u64; // approximate
                reports.push(report);
            }
            Err(e) => {
                let err = ImportError {
                    path: cat.name().to_string(),
                    error: e.to_string(),
                };
                warn!("category {} failed: {}", cat.name(), err.error);
                errors.push(err);
            }
        }
    }

    Ok(FacebookImportReport {
        categories: reports,
        total_files_written: total_files,
        total_bytes,
        elapsed: start.elapsed(),
        errors,
    })
}

// ---------------------------------------------------------------------------
// Category dispatch
// ---------------------------------------------------------------------------

fn process_category(
    cat: Category,
    json_root: &Path,
    out_dir: &Path,
    dry_run: bool,
) -> Result<CategoryReport> {
    match cat {
        Category::Messages => process_messages(json_root, out_dir, dry_run),
        Category::Posts => process_posts(json_root, out_dir, dry_run),
        Category::Comments => process_comments(json_root, out_dir, dry_run),
        Category::Friends => process_friends(json_root, out_dir, dry_run),
        Category::Search => process_search(json_root, out_dir, dry_run),
        Category::Events => process_events(json_root, out_dir, dry_run),
        Category::Groups => process_groups(json_root, out_dir, dry_run),
    }
}

// ---------------------------------------------------------------------------
// Encoding fix
// ---------------------------------------------------------------------------

/// Fix Facebook's broken UTF-8 encoding.
///
/// Facebook JSON exports store non-ASCII as mojibake: the original UTF-8 byte
/// sequence is treated as latin-1 code points. Each `char` in the input holds
/// a single byte of the original UTF-8. We reassemble those bytes and decode.
fn fix_encoding(s: &str) -> String {
    let bytes: Vec<u8> = s.chars().map(|c| c as u8).collect();
    String::from_utf8(bytes).unwrap_or_else(|_| s.to_string())
}

/// Format a unix timestamp (seconds) as `YYYY-MM-DD`.
fn ts_to_date(ts: i64) -> String {
    Utc.timestamp_opt(ts, 0)
        .single()
        .map_or_else(|| "unknown".into(), |dt| dt.format("%Y-%m-%d").to_string())
}

/// Format a unix timestamp (milliseconds) as `YYYY-MM-DD HH:MM`.
fn ts_ms_to_datetime(ts_ms: i64) -> String {
    Utc.timestamp_millis_opt(ts_ms).single().map_or_else(
        || "unknown".into(),
        |dt| dt.format("%Y-%m-%d %H:%M").to_string(),
    )
}

/// Format a unix timestamp (milliseconds) as year (i32).
fn ts_ms_to_year(ts_ms: i64) -> i32 {
    Utc.timestamp_millis_opt(ts_ms).single().map_or(0, |dt| {
        dt.format("%Y").to_string().parse::<i32>().unwrap_or(0)
    })
}

/// Format a unix timestamp (seconds) as year (i32).
fn ts_to_year(ts: i64) -> i32 {
    Utc.timestamp_opt(ts, 0).single().map_or(0, |dt| {
        dt.format("%Y").to_string().parse::<i32>().unwrap_or(0)
    })
}

/// Create a safe filename slug from a string.
fn slugify(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>()
        .to_lowercase()
        .replace("--", "-")
        .trim_matches('-')
        .to_string()
}

/// Write a markdown file to disk (or just count it for dry-run).
/// Returns the number of bytes written.
fn write_source(path: &Path, content: &str, dry_run: bool) -> Result<u64> {
    if dry_run {
        return Ok(content.len() as u64);
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| Error::io(parent, e))?;
    }
    fs::write(path, content).map_err(|e| Error::io(path, e))?;
    Ok(content.len() as u64)
}

/// Find the export directory that contains JSON data (has a
/// `your_facebook_activity/messages/` dir with JSON files, or top-level
/// structured dirs like `connections/`, `logged_information/`).
fn find_json_root(dirs: &[PathBuf]) -> Result<PathBuf> {
    for dir in dirs {
        // Check for structured data dirs that only exist in the JSON export
        let markers = [
            "connections",
            "logged_information",
            "security_and_login_information",
            "personal_information",
        ];
        let has_markers = markers.iter().any(|m| dir.join(m).is_dir());
        if has_markers {
            return Ok(dir.clone());
        }
    }
    // Fallback: pick the first dir that has any .json files
    for dir in dirs {
        if has_json_files(dir) {
            return Ok(dir.clone());
        }
    }
    Err(Error::Other(anyhow::anyhow!(
        "none of the provided directories contain Facebook JSON data"
    )))
}

fn has_json_files(dir: &Path) -> bool {
    fs::read_dir(dir).ok().is_some_and(|entries| {
        entries.filter_map(std::result::Result::ok).any(|e| {
            e.path().extension().is_some_and(|ext| ext == "json")
                || e.file_type().is_ok_and(|ft| ft.is_dir())
        })
    })
}

// ---------------------------------------------------------------------------
// Serde models
// ---------------------------------------------------------------------------

// --- Messages ---

#[allow(dead_code)] // fields used for deserialization
#[derive(Debug, Deserialize)]
struct MessageFile {
    #[serde(default)]
    participants: Vec<Participant>,
    #[serde(default)]
    messages: Vec<RawMessage>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    thread_path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Participant {
    name: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct RawMessage {
    #[serde(default)]
    sender_name: String,
    #[serde(default)]
    timestamp_ms: i64,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    photos: Option<Vec<MediaRef>>,
    #[serde(default)]
    videos: Option<Vec<MediaRef>>,
    #[serde(default)]
    files: Option<Vec<MediaRef>>,
    #[serde(default)]
    audio_files: Option<Vec<MediaRef>>,
    #[serde(default)]
    gifs: Option<Vec<MediaRef>>,
    #[serde(default)]
    sticker: Option<StickerRef>,
    #[serde(default)]
    reactions: Option<Vec<Reaction>>,
    #[serde(default)]
    share: Option<ShareRef>,
    #[serde(default)]
    is_unsent_image_by_messenger_kid_parent: bool,
    #[serde(default, rename = "type")]
    msg_type: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct MediaRef {
    #[serde(default)]
    uri: Option<String>,
    #[serde(default)]
    creation_timestamp: Option<i64>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct StickerRef {
    #[serde(default)]
    uri: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Reaction {
    #[serde(default)]
    reaction: String,
    #[serde(default)]
    actor: String,
}

#[derive(Debug, Deserialize)]
struct ShareRef {
    #[serde(default)]
    link: Option<String>,
    #[serde(default)]
    share_text: Option<String>,
}

// --- Posts ---

#[derive(Debug, Deserialize)]
struct RawPost {
    #[serde(default)]
    timestamp: i64,
    #[serde(default)]
    data: Option<Vec<PostData>>,
    #[serde(default)]
    attachments: Option<Vec<PostAttachment>>,
    #[serde(default)]
    title: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct PostData {
    #[serde(default)]
    post: Option<String>,
    #[serde(default)]
    update_timestamp: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct PostAttachment {
    #[serde(default)]
    data: Option<Vec<AttachmentData>>,
}

#[derive(Debug, Deserialize)]
struct AttachmentData {
    #[serde(default)]
    external_context: Option<ExternalContext>,
    #[serde(default)]
    media: Option<AttachmentMedia>,
}

#[derive(Debug, Deserialize)]
struct ExternalContext {
    #[serde(default)]
    url: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct AttachmentMedia {
    #[serde(default)]
    uri: Option<String>,
    #[serde(default)]
    description: Option<String>,
}

// --- Comments ---

#[derive(Debug, Deserialize)]
struct CommentsFile {
    #[serde(default)]
    comments_v2: Vec<RawComment>,
}

#[derive(Debug, Deserialize)]
struct RawComment {
    #[serde(default)]
    timestamp: i64,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    data: Option<Vec<CommentData>>,
}

#[derive(Debug, Deserialize)]
struct CommentData {
    #[serde(default)]
    comment: Option<CommentBody>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct CommentBody {
    #[serde(default)]
    timestamp: i64,
    #[serde(default)]
    comment: Option<String>,
    #[serde(default)]
    author: Option<String>,
}

// --- Reactions (likes_and_reactions files) ---
// These are a flat array at root level, not wrapped in an object.

#[derive(Debug, Deserialize)]
struct RawReaction {
    #[serde(default)]
    timestamp: i64,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    label_values: Option<Vec<LabelValue>>,
}

#[derive(Debug, Deserialize)]
struct LabelValue {
    #[serde(default)]
    label: String,
    #[serde(default)]
    value: String,
}

// --- Friends ---

#[derive(Debug, Deserialize)]
struct FriendsFile {
    #[serde(default)]
    friends_v2: Vec<FriendEntry>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct FollowersFile {
    // followers have similar format
    #[serde(default, alias = "followers_v2", alias = "following_v2")]
    entries: Vec<FriendEntry>,
}

#[derive(Debug, Deserialize)]
struct FriendEntry {
    #[serde(default)]
    name: String,
    #[serde(default)]
    timestamp: i64,
}

// --- Search history ---

#[derive(Debug, Deserialize)]
struct SearchFile {
    #[serde(default)]
    searches_v2: Vec<SearchEntry>,
}

#[derive(Debug, Deserialize)]
struct SearchEntry {
    #[serde(default)]
    timestamp: i64,
    #[serde(default)]
    data: Option<Vec<SearchData>>,
    #[serde(default)]
    title: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SearchData {
    #[serde(default)]
    text: Option<String>,
}

// --- Events ---

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct EventsFile {
    #[serde(
        default,
        alias = "events_invited_v2",
        alias = "events_joined",
        alias = "your_events_v2",
        alias = "event_responses_v2"
    )]
    events: Vec<EventEntry>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct EventEntry {
    #[serde(default)]
    name: String,
    #[serde(default)]
    start_timestamp: i64,
    #[serde(default)]
    end_timestamp: Option<i64>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    place: Option<EventPlace>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct EventPlace {
    #[serde(default)]
    name: Option<String>,
}

// --- Groups ---

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct GroupsFile {
    #[serde(default, alias = "groups_joined_v2", alias = "groups_admined_v2")]
    groups: Vec<GroupEntry>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct GroupEntry {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    timestamp: i64,
}

// ---------------------------------------------------------------------------
// Category processors
// ---------------------------------------------------------------------------

/// Maximum source file size before splitting (~4 MB to stay under the 5 MB
/// bulk-ingest limit with headroom for frontmatter).
const MAX_FILE_BYTES: usize = 4 * 1024 * 1024;

#[allow(clippy::too_many_lines)]
fn process_messages(json_root: &Path, out_dir: &Path, dry_run: bool) -> Result<CategoryReport> {
    let msg_root = json_root.join("your_facebook_activity").join("messages");
    if !msg_root.is_dir() {
        return Ok(CategoryReport {
            category: "messages".into(),
            files_written: 0,
            items_processed: 0,
        });
    }

    let subdirs = ["inbox", "e2ee_cutover", "archived_threads"];
    let mut files_written = 0usize;
    let mut items_processed = 0usize;

    for subdir in &subdirs {
        let base = msg_root.join(subdir);
        if !base.is_dir() {
            continue;
        }
        let conversations = read_dir_sorted(&base)?;
        for conv_dir in &conversations {
            if !conv_dir.is_dir() {
                continue;
            }
            let conv_name = conv_dir
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();

            // Read all message_N.json files in this conversation
            let mut all_messages = Vec::new();
            let mut participants = Vec::new();
            let mut title = None;

            let json_files = list_json_files(conv_dir);
            for jf in &json_files {
                match fs::read_to_string(jf) {
                    Ok(raw) => match serde_json::from_str::<MessageFile>(&raw) {
                        Ok(mf) => {
                            if participants.is_empty() {
                                participants = mf
                                    .participants
                                    .iter()
                                    .map(|p| fix_encoding(&p.name))
                                    .collect();
                            }
                            if title.is_none() {
                                title = mf.title.as_deref().map(fix_encoding);
                            }
                            all_messages.extend(mf.messages);
                        }
                        Err(e) => {
                            warn!("parse {}: {e}", jf.display());
                        }
                    },
                    Err(e) => {
                        warn!("read {}: {e}", jf.display());
                    }
                }
            }

            if all_messages.is_empty() {
                continue;
            }

            // Sort by timestamp ascending
            all_messages.sort_by_key(|m| m.timestamp_ms);
            items_processed += all_messages.len();

            let first_ts = all_messages.first().map_or(0, |m| m.timestamp_ms);
            let last_ts = all_messages.last().map_or(0, |m| m.timestamp_ms);
            let date_range_str = format!(
                "{} to {}",
                ts_ms_to_datetime(first_ts).split(' ').next().unwrap_or("?"),
                ts_ms_to_datetime(last_ts).split(' ').next().unwrap_or("?"),
            );

            let display_name = title
                .as_deref()
                .unwrap_or_else(|| participants.first().map_or(&conv_name, String::as_str));

            // Build markdown body
            let body = format_conversation(
                &conv_name,
                display_name,
                &participants,
                &all_messages,
                &date_range_str,
            );

            // Split if too large
            let slug = slugify(&conv_name);
            if body.len() > MAX_FILE_BYTES {
                let chunks = split_conversation_by_year(&all_messages);
                for (year, year_msgs) in &chunks {
                    let yr_first = year_msgs.first().map_or(0, |m| m.timestamp_ms);
                    let yr_last = year_msgs.last().map_or(0, |m| m.timestamp_ms);
                    let yr_range = format!(
                        "{} to {}",
                        ts_ms_to_datetime(yr_first).split(' ').next().unwrap_or("?"),
                        ts_ms_to_datetime(yr_last).split(' ').next().unwrap_or("?"),
                    );
                    let yr_body = format_conversation_refs(
                        &conv_name,
                        display_name,
                        &participants,
                        year_msgs,
                        &yr_range,
                    );
                    let path = out_dir.join(format!("{slug}-{year}.md"));
                    write_source(&path, &yr_body, dry_run)?;
                    files_written += 1;
                }
            } else {
                let path = out_dir.join(format!("{slug}.md"));
                write_source(&path, &body, dry_run)?;
                files_written += 1;
            }
        }
    }

    Ok(CategoryReport {
        category: "messages".into(),
        files_written,
        items_processed,
    })
}

#[allow(clippy::too_many_lines)]
fn format_conversation(
    conv_id: &str,
    display_name: &str,
    participants: &[String],
    messages: &[RawMessage],
    date_range: &str,
) -> String {
    let mut out = String::with_capacity(messages.len() * 100);

    // Frontmatter
    let _ = writeln!(out, "---");
    let _ = writeln!(out, "source: facebook-export");
    let _ = writeln!(out, "category: messages");
    let _ = writeln!(out, "conversation_id: {conv_id}");
    let _ = writeln!(out, "participants:");
    for p in participants {
        let _ = writeln!(out, "  - \"{p}\"");
    }
    let _ = writeln!(out, "message_count: {}", messages.len());
    let _ = writeln!(out, "date_range: \"{date_range}\"");
    let _ = writeln!(out, "---");
    let _ = writeln!(out);

    // Header
    let _ = writeln!(out, "# Conversation: {display_name}");
    let _ = writeln!(out);
    let _ = writeln!(out, "**Participants:** {}", participants.join(", "));
    let _ = writeln!(
        out,
        "**Messages:** {} | **Period:** {date_range}",
        messages.len()
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "---");
    let _ = writeln!(out);

    // Messages
    for msg in messages {
        let sender = fix_encoding(&msg.sender_name);
        let dt = ts_ms_to_datetime(msg.timestamp_ms);
        let _ = write!(out, "**[{dt}]** **{sender}:** ");

        let mut has_content = false;

        if let Some(content) = &msg.content {
            let text = fix_encoding(content);
            if !text.is_empty() {
                let _ = write!(out, "{text}");
                has_content = true;
            }
        }

        // Media indicators
        if let Some(photos) = &msg.photos {
            if !photos.is_empty() {
                if has_content {
                    let _ = write!(out, " ");
                }
                let _ = write!(out, "*[{} photo(s)]*", photos.len());
                has_content = true;
            }
        }
        if let Some(videos) = &msg.videos {
            if !videos.is_empty() {
                if has_content {
                    let _ = write!(out, " ");
                }
                let _ = write!(out, "*[{} video(s)]*", videos.len());
                has_content = true;
            }
        }
        if let Some(files) = &msg.files {
            if !files.is_empty() {
                if has_content {
                    let _ = write!(out, " ");
                }
                let _ = write!(out, "*[{} file(s)]*", files.len());
                has_content = true;
            }
        }
        if let Some(audio) = &msg.audio_files {
            if !audio.is_empty() {
                if has_content {
                    let _ = write!(out, " ");
                }
                let _ = write!(out, "*[audio]*");
                has_content = true;
            }
        }
        if let Some(gifs) = &msg.gifs {
            if !gifs.is_empty() {
                if has_content {
                    let _ = write!(out, " ");
                }
                let _ = write!(out, "*[GIF]*");
                has_content = true;
            }
        }
        if msg.sticker.is_some() {
            if has_content {
                let _ = write!(out, " ");
            }
            let _ = write!(out, "*[sticker]*");
            has_content = true;
        }
        if let Some(share) = &msg.share {
            if has_content {
                let _ = write!(out, " ");
            }
            if let Some(link) = &share.link {
                let _ = write!(out, "*[shared: {link}]*");
            } else if let Some(text) = &share.share_text {
                let _ = write!(out, "*[shared: {}]*", fix_encoding(text));
            }
            has_content = true;
        }

        if !has_content {
            let _ = write!(out, "*[empty message]*");
        }

        // Reactions
        if let Some(reactions) = &msg.reactions {
            if !reactions.is_empty() {
                let rx: Vec<String> = reactions
                    .iter()
                    .map(|r| {
                        let emoji = fix_encoding(&r.reaction);
                        let actor = fix_encoding(&r.actor);
                        format!("{emoji} {actor}")
                    })
                    .collect();
                let _ = write!(out, " _{}_", rx.join(", "));
            }
        }

        let _ = writeln!(out);
        let _ = writeln!(out);
    }

    out
}

fn split_conversation_by_year(messages: &[RawMessage]) -> BTreeMap<i32, Vec<&RawMessage>> {
    let mut by_year: BTreeMap<i32, Vec<&RawMessage>> = BTreeMap::new();
    for msg in messages {
        let year = ts_ms_to_year(msg.timestamp_ms);
        by_year.entry(year).or_default().push(msg);
    }
    by_year
}

fn format_conversation_refs(
    conv_id: &str,
    display_name: &str,
    participants: &[String],
    messages: &[&RawMessage],
    date_range: &str,
) -> String {
    // Reuse format_conversation by converting refs to owned slice
    // This is a workaround — we create a thin wrapper
    let mut out = String::with_capacity(messages.len() * 100);

    let _ = writeln!(out, "---");
    let _ = writeln!(out, "source: facebook-export");
    let _ = writeln!(out, "category: messages");
    let _ = writeln!(out, "conversation_id: {conv_id}");
    let _ = writeln!(out, "participants:");
    for p in participants {
        let _ = writeln!(out, "  - \"{p}\"");
    }
    let _ = writeln!(out, "message_count: {}", messages.len());
    let _ = writeln!(out, "date_range: \"{date_range}\"");
    let _ = writeln!(out, "---");
    let _ = writeln!(out);
    let _ = writeln!(out, "# Conversation: {display_name}");
    let _ = writeln!(out);
    let _ = writeln!(out, "**Participants:** {}", participants.join(", "));
    let _ = writeln!(
        out,
        "**Messages:** {} | **Period:** {date_range}",
        messages.len()
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "---");
    let _ = writeln!(out);

    for msg in messages {
        let sender = fix_encoding(&msg.sender_name);
        let dt = ts_ms_to_datetime(msg.timestamp_ms);
        let _ = write!(out, "**[{dt}]** **{sender}:** ");

        if let Some(content) = &msg.content {
            let text = fix_encoding(content);
            if !text.is_empty() {
                let _ = write!(out, "{text}");
            }
        }

        if let Some(photos) = &msg.photos {
            if !photos.is_empty() {
                let _ = write!(out, " *[{} photo(s)]*", photos.len());
            }
        }
        if msg.sticker.is_some() {
            let _ = write!(out, " *[sticker]*");
        }
        if let Some(share) = &msg.share {
            if let Some(link) = &share.link {
                let _ = write!(out, " *[shared: {link}]*");
            }
        }

        let _ = writeln!(out);
        let _ = writeln!(out);
    }

    out
}

#[allow(clippy::too_many_lines)]
fn process_posts(json_root: &Path, out_dir: &Path, dry_run: bool) -> Result<CategoryReport> {
    let posts_dir = json_root.join("your_facebook_activity").join("posts");
    if !posts_dir.is_dir() {
        return Ok(CategoryReport {
            category: "posts".into(),
            files_written: 0,
            items_processed: 0,
        });
    }

    let mut all_posts: Vec<RawPost> = Vec::new();

    // Read all JSON files that contain posts (top-level arrays or objects)
    for entry in list_json_files(&posts_dir) {
        let fname = entry
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        // Skip album files and other non-post files
        if fname.starts_with("your_posts")
            || fname.starts_with("posts_on_other")
            || fname == "archive.json"
        {
            match fs::read_to_string(&entry) {
                Ok(raw) => {
                    // Posts files are top-level arrays
                    if let Ok(posts) = serde_json::from_str::<Vec<RawPost>>(&raw) {
                        all_posts.extend(posts);
                    }
                }
                Err(e) => warn!("read {}: {e}", entry.display()),
            }
        }
    }

    if all_posts.is_empty() {
        return Ok(CategoryReport {
            category: "posts".into(),
            files_written: 0,
            items_processed: 0,
        });
    }

    all_posts.sort_by_key(|p| p.timestamp);
    let items = all_posts.len();

    // Group by year
    let mut by_year: BTreeMap<i32, Vec<&RawPost>> = BTreeMap::new();
    for post in &all_posts {
        let year = ts_to_year(post.timestamp);
        by_year.entry(year).or_default().push(post);
    }

    let mut files_written = 0;
    for (year, posts) in &by_year {
        let mut out = String::new();

        let _ = writeln!(out, "---");
        let _ = writeln!(out, "source: facebook-export");
        let _ = writeln!(out, "category: posts");
        let _ = writeln!(out, "year: {year}");
        let _ = writeln!(out, "post_count: {}", posts.len());
        let _ = writeln!(out, "---");
        let _ = writeln!(out);
        let _ = writeln!(out, "# Facebook Posts — {year}");
        let _ = writeln!(out);

        for post in posts {
            let date = ts_to_date(post.timestamp);
            let title = post.title.as_deref().map(fix_encoding);

            let _ = writeln!(out, "## [{date}] {}", title.as_deref().unwrap_or("Post"));
            let _ = writeln!(out);

            // Post text
            if let Some(data) = &post.data {
                for d in data {
                    if let Some(text) = &d.post {
                        let fixed = fix_encoding(text);
                        if !fixed.is_empty() {
                            let _ = writeln!(out, "{fixed}");
                            let _ = writeln!(out);
                        }
                    }
                }
            }

            // Attachments
            if let Some(attachments) = &post.attachments {
                for att in attachments {
                    if let Some(data) = &att.data {
                        for d in data {
                            if let Some(ctx) = &d.external_context {
                                if let Some(url) = &ctx.url {
                                    let _ = writeln!(out, "Link: {url}");
                                }
                            }
                            if let Some(media) = &d.media {
                                if let Some(desc) = &media.description {
                                    let _ = writeln!(out, "{}", fix_encoding(desc));
                                }
                            }
                        }
                    }
                }
            }

            let _ = writeln!(out, "---");
            let _ = writeln!(out);
        }

        let path = out_dir.join(format!("{year}-posts.md"));
        write_source(&path, &out, dry_run)?;
        files_written += 1;
    }

    Ok(CategoryReport {
        category: "posts".into(),
        files_written,
        items_processed: items,
    })
}

#[allow(clippy::too_many_lines)]
fn process_comments(json_root: &Path, out_dir: &Path, dry_run: bool) -> Result<CategoryReport> {
    let cr_dir = json_root
        .join("your_facebook_activity")
        .join("comments_and_reactions");
    if !cr_dir.is_dir() {
        return Ok(CategoryReport {
            category: "comments".into(),
            files_written: 0,
            items_processed: 0,
        });
    }

    let mut all_comments: Vec<RawComment> = Vec::new();
    let mut all_reactions: Vec<RawReaction> = Vec::new();

    for entry in list_json_files(&cr_dir) {
        let fname = entry
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        match fs::read_to_string(&entry) {
            Ok(raw) => {
                if fname.starts_with("comments") {
                    if let Ok(cf) = serde_json::from_str::<CommentsFile>(&raw) {
                        all_comments.extend(cf.comments_v2);
                    }
                } else if fname.starts_with("likes_and_reactions") {
                    if let Ok(reactions) = serde_json::from_str::<Vec<RawReaction>>(&raw) {
                        all_reactions.extend(reactions);
                    }
                }
            }
            Err(e) => warn!("read {}: {e}", entry.display()),
        }
    }

    let items = all_comments.len() + all_reactions.len();
    let mut files_written = 0;

    // Comments by year
    if !all_comments.is_empty() {
        all_comments.sort_by_key(|c| c.timestamp);
        let mut by_year: BTreeMap<i32, Vec<&RawComment>> = BTreeMap::new();
        for c in &all_comments {
            by_year.entry(ts_to_year(c.timestamp)).or_default().push(c);
        }

        for (year, comments) in &by_year {
            let mut out = String::new();
            let _ = writeln!(out, "---");
            let _ = writeln!(out, "source: facebook-export");
            let _ = writeln!(out, "category: comments");
            let _ = writeln!(out, "year: {year}");
            let _ = writeln!(out, "comment_count: {}", comments.len());
            let _ = writeln!(out, "---");
            let _ = writeln!(out);
            let _ = writeln!(out, "# Facebook Comments — {year}");
            let _ = writeln!(out);

            for c in comments {
                let date = ts_to_date(c.timestamp);
                let title = c.title.as_deref().map(fix_encoding);
                let _ = writeln!(
                    out,
                    "**[{date}]** {}",
                    title.as_deref().unwrap_or("Comment")
                );

                if let Some(data) = &c.data {
                    for d in data {
                        if let Some(body) = &d.comment {
                            if let Some(text) = &body.comment {
                                let _ = writeln!(out, "> {}", fix_encoding(text));
                            }
                        }
                    }
                }
                let _ = writeln!(out);
            }

            let path = out_dir.join(format!("{year}-comments.md"));
            write_source(&path, &out, dry_run)?;
            files_written += 1;
        }
    }

    // Reactions by year
    if !all_reactions.is_empty() {
        all_reactions.sort_by_key(|r| r.timestamp);
        let mut by_year: BTreeMap<i32, Vec<&RawReaction>> = BTreeMap::new();
        for r in &all_reactions {
            by_year.entry(ts_to_year(r.timestamp)).or_default().push(r);
        }

        for (year, reactions) in &by_year {
            let mut out = String::new();
            let _ = writeln!(out, "---");
            let _ = writeln!(out, "source: facebook-export");
            let _ = writeln!(out, "category: reactions");
            let _ = writeln!(out, "year: {year}");
            let _ = writeln!(out, "reaction_count: {}", reactions.len());
            let _ = writeln!(out, "---");
            let _ = writeln!(out);
            let _ = writeln!(out, "# Facebook Reactions — {year}");
            let _ = writeln!(out);

            for r in reactions {
                let date = ts_to_date(r.timestamp);
                let title = r.title.as_deref().map(fix_encoding);

                let reaction_type = r
                    .label_values
                    .as_ref()
                    .and_then(|lvs| {
                        lvs.iter()
                            .find(|lv| lv.label == "Reaction")
                            .map(|lv| lv.value.as_str())
                    })
                    .unwrap_or("Like");

                let _ = writeln!(
                    out,
                    "**[{date}]** {reaction_type} — {}",
                    title.as_deref().unwrap_or("(reaction)")
                );
            }
            let _ = writeln!(out);

            let path = out_dir.join(format!("{year}-reactions.md"));
            write_source(&path, &out, dry_run)?;
            files_written += 1;
        }
    }

    Ok(CategoryReport {
        category: "comments".into(),
        files_written,
        items_processed: items,
    })
}

#[allow(clippy::too_many_lines)]
fn process_friends(json_root: &Path, out_dir: &Path, dry_run: bool) -> Result<CategoryReport> {
    let friends_dir = json_root.join("connections").join("friends");
    let followers_dir = json_root.join("connections").join("followers");
    let mut files_written = 0;
    let mut items = 0;

    // Friends
    let friends_path = friends_dir.join("your_friends.json");
    if friends_path.is_file() {
        if let Ok(raw) = fs::read_to_string(&friends_path) {
            if let Ok(ff) = serde_json::from_str::<FriendsFile>(&raw) {
                items += ff.friends_v2.len();
                let mut out = String::new();
                let _ = writeln!(out, "---");
                let _ = writeln!(out, "source: facebook-export");
                let _ = writeln!(out, "category: friends");
                let _ = writeln!(out, "friend_count: {}", ff.friends_v2.len());
                let _ = writeln!(out, "---");
                let _ = writeln!(out);
                let _ = writeln!(out, "# Facebook Friends");
                let _ = writeln!(out);

                for f in &ff.friends_v2 {
                    let name = fix_encoding(&f.name);
                    let date = ts_to_date(f.timestamp);
                    let _ = writeln!(out, "- **{name}** (added {date})");
                }

                let path = out_dir.join("friends.md");
                write_source(&path, &out, dry_run)?;
                files_written += 1;
            }
        }
    }

    // Removed friends
    let removed_path = friends_dir.join("removed_friends.json");
    if removed_path.is_file() {
        if let Ok(raw) = fs::read_to_string(&removed_path) {
            if let Ok(ff) = serde_json::from_str::<FriendsFile>(&raw) {
                if !ff.friends_v2.is_empty() {
                    items += ff.friends_v2.len();
                    let mut out = String::new();
                    let _ = writeln!(out, "---");
                    let _ = writeln!(out, "source: facebook-export");
                    let _ = writeln!(out, "category: friends");
                    let _ = writeln!(out, "subcategory: removed");
                    let _ = writeln!(out, "count: {}", ff.friends_v2.len());
                    let _ = writeln!(out, "---");
                    let _ = writeln!(out);
                    let _ = writeln!(out, "# Removed Friends");
                    let _ = writeln!(out);

                    for f in &ff.friends_v2 {
                        let name = fix_encoding(&f.name);
                        let date = ts_to_date(f.timestamp);
                        let _ = writeln!(out, "- **{name}** (removed {date})");
                    }

                    let path = out_dir.join("removed-friends.md");
                    write_source(&path, &out, dry_run)?;
                    files_written += 1;
                }
            }
        }
    }

    // Followers
    for fname in &["who_you've_followed.json", "people_who_followed_you.json"] {
        let fpath = followers_dir.join(fname);
        if fpath.is_file() {
            if let Ok(raw) = fs::read_to_string(&fpath) {
                // These files have varying root keys — try to parse flexibly
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw) {
                    if let Some(obj) = val.as_object() {
                        for (_key, arr) in obj {
                            if let Some(entries) = arr.as_array() {
                                if entries.is_empty() {
                                    continue;
                                }
                                items += entries.len();
                                let slug = slugify(&fname.replace(".json", ""));
                                let mut out = String::new();
                                let _ = writeln!(out, "---");
                                let _ = writeln!(out, "source: facebook-export");
                                let _ = writeln!(out, "category: followers");
                                let _ = writeln!(out, "count: {}", entries.len());
                                let _ = writeln!(out, "---");
                                let _ = writeln!(out);
                                let _ = writeln!(
                                    out,
                                    "# {}",
                                    fname.replace(".json", "").replace('_', " ")
                                );
                                let _ = writeln!(out);

                                for entry in entries {
                                    if let Some(name) = entry.get("name").and_then(|v| v.as_str()) {
                                        let ts = entry
                                            .get("timestamp")
                                            .and_then(serde_json::Value::as_i64)
                                            .unwrap_or(0);
                                        let _ = writeln!(
                                            out,
                                            "- **{}** ({})",
                                            fix_encoding(name),
                                            ts_to_date(ts)
                                        );
                                    }
                                }

                                let path = out_dir.join(format!("{slug}.md"));
                                write_source(&path, &out, dry_run)?;
                                files_written += 1;
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(CategoryReport {
        category: "friends".into(),
        files_written,
        items_processed: items,
    })
}

fn process_search(json_root: &Path, out_dir: &Path, dry_run: bool) -> Result<CategoryReport> {
    let search_path = json_root
        .join("logged_information")
        .join("search")
        .join("your_search_history.json");

    if !search_path.is_file() {
        return Ok(CategoryReport {
            category: "search".into(),
            files_written: 0,
            items_processed: 0,
        });
    }

    let raw = fs::read_to_string(&search_path).map_err(|e| Error::io(&search_path, e))?;
    let sf: SearchFile = serde_json::from_str(&raw)
        .map_err(|e| Error::Other(anyhow::anyhow!("parse search: {e}")))?;

    if sf.searches_v2.is_empty() {
        return Ok(CategoryReport {
            category: "search".into(),
            files_written: 0,
            items_processed: 0,
        });
    }

    let items = sf.searches_v2.len();
    let mut out = String::new();

    let _ = writeln!(out, "---");
    let _ = writeln!(out, "source: facebook-export");
    let _ = writeln!(out, "category: search-history");
    let _ = writeln!(out, "search_count: {items}");
    let _ = writeln!(out, "---");
    let _ = writeln!(out);
    let _ = writeln!(out, "# Facebook Search History");
    let _ = writeln!(out);

    for s in &sf.searches_v2 {
        let date = ts_to_date(s.timestamp);
        let query = s
            .data
            .as_ref()
            .and_then(|d: &Vec<SearchData>| d.first())
            .and_then(|d: &SearchData| d.text.as_deref())
            .map(fix_encoding)
            .unwrap_or_default();
        let title = s.title.as_deref().map(fix_encoding);
        if !query.is_empty() {
            let _ = writeln!(
                out,
                "- **[{date}]** \"{query}\" — {}",
                title.as_deref().unwrap_or("")
            );
        }
    }

    let path = out_dir.join("search-history.md");
    write_source(&path, &out, dry_run)?;

    Ok(CategoryReport {
        category: "search".into(),
        files_written: 1,
        items_processed: items,
    })
}

fn process_events(json_root: &Path, out_dir: &Path, dry_run: bool) -> Result<CategoryReport> {
    let events_dir = json_root.join("your_facebook_activity").join("events");
    if !events_dir.is_dir() {
        return Ok(CategoryReport {
            category: "events".into(),
            files_written: 0,
            items_processed: 0,
        });
    }

    let mut all_events: Vec<(String, String, i64)> = Vec::new(); // (name, source_file, timestamp)

    for entry in list_json_files(&events_dir) {
        let fname = entry
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        if let Ok(raw) = fs::read_to_string(&entry) {
            // Events files have varying root keys
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw) {
                if let Some(obj) = val.as_object() {
                    for (_key, arr) in obj {
                        if let Some(entries) = arr.as_array() {
                            for e in entries {
                                let name = e
                                    .get("name")
                                    .and_then(|v| v.as_str())
                                    .map(fix_encoding)
                                    .unwrap_or_default();
                                let ts = e
                                    .get("start_timestamp")
                                    .or_else(|| e.get("timestamp"))
                                    .and_then(serde_json::Value::as_i64)
                                    .unwrap_or(0);
                                if !name.is_empty() || ts > 0 {
                                    all_events.push((name, fname.clone(), ts));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if all_events.is_empty() {
        return Ok(CategoryReport {
            category: "events".into(),
            files_written: 0,
            items_processed: 0,
        });
    }

    all_events.sort_by_key(|e| e.2);
    let items = all_events.len();

    let mut out = String::new();
    let _ = writeln!(out, "---");
    let _ = writeln!(out, "source: facebook-export");
    let _ = writeln!(out, "category: events");
    let _ = writeln!(out, "event_count: {items}");
    let _ = writeln!(out, "---");
    let _ = writeln!(out);
    let _ = writeln!(out, "# Facebook Events");
    let _ = writeln!(out);

    for (name, _source, ts) in &all_events {
        let date = ts_to_date(*ts);
        let _ = writeln!(out, "- **[{date}]** {name}");
    }

    let path = out_dir.join("events.md");
    write_source(&path, &out, dry_run)?;

    Ok(CategoryReport {
        category: "events".into(),
        files_written: 1,
        items_processed: items,
    })
}

#[allow(clippy::too_many_lines)]
fn process_groups(json_root: &Path, out_dir: &Path, dry_run: bool) -> Result<CategoryReport> {
    let groups_dir = json_root.join("your_facebook_activity").join("groups");
    if !groups_dir.is_dir() {
        return Ok(CategoryReport {
            category: "groups".into(),
            files_written: 0,
            items_processed: 0,
        });
    }

    let mut items = 0;
    let mut files_written = 0;

    // Group membership
    let groups_path = groups_dir.join("your_groups.json");
    if groups_path.is_file() {
        if let Ok(raw) = fs::read_to_string(&groups_path) {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw) {
                // Try to find the array of groups
                let entries: Vec<&serde_json::Value> = if let Some(arr) = val.as_array() {
                    arr.iter().collect()
                } else if let Some(obj) = val.as_object() {
                    obj.values()
                        .filter_map(|v| v.as_array())
                        .flatten()
                        .collect()
                } else {
                    vec![]
                };

                if !entries.is_empty() {
                    items += entries.len();
                    let mut out = String::new();
                    let _ = writeln!(out, "---");
                    let _ = writeln!(out, "source: facebook-export");
                    let _ = writeln!(out, "category: groups");
                    let _ = writeln!(out, "group_count: {}", entries.len());
                    let _ = writeln!(out, "---");
                    let _ = writeln!(out);
                    let _ = writeln!(out, "# Facebook Groups");
                    let _ = writeln!(out);

                    for e in &entries {
                        let name = e
                            .get("name")
                            .and_then(|v| v.as_str())
                            .map(fix_encoding)
                            .unwrap_or_default();
                        let ts = e
                            .get("timestamp")
                            .and_then(serde_json::Value::as_i64)
                            .unwrap_or(0);
                        let date = ts_to_date(ts);
                        if !name.is_empty() {
                            let _ = writeln!(out, "- **{name}** (joined {date})");
                        }
                    }

                    let path = out_dir.join("groups.md");
                    write_source(&path, &out, dry_run)?;
                    files_written += 1;
                }
            }
        }
    }

    // Group posts and comments
    let gpc_path = groups_dir.join("group_posts_and_comments.json");
    if gpc_path.is_file() {
        if let Ok(raw) = fs::read_to_string(&gpc_path) {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw) {
                if let Some(obj) = val.as_object() {
                    for (_key, arr) in obj {
                        if let Some(entries) = arr.as_array() {
                            if !entries.is_empty() {
                                items += entries.len();
                                let mut out = String::new();
                                let _ = writeln!(out, "---");
                                let _ = writeln!(out, "source: facebook-export");
                                let _ = writeln!(out, "category: groups");
                                let _ = writeln!(out, "subcategory: posts-and-comments");
                                let _ = writeln!(out, "count: {}", entries.len());
                                let _ = writeln!(out, "---");
                                let _ = writeln!(out);
                                let _ = writeln!(out, "# Group Posts and Comments");
                                let _ = writeln!(out);

                                for e in entries {
                                    let ts = e
                                        .get("timestamp")
                                        .and_then(serde_json::Value::as_i64)
                                        .unwrap_or(0);
                                    let date = ts_to_date(ts);
                                    let title =
                                        e.get("title").and_then(|v| v.as_str()).map(fix_encoding);
                                    let _ = writeln!(
                                        out,
                                        "**[{date}]** {}",
                                        title.as_deref().unwrap_or("(group activity)")
                                    );

                                    // Try to get comment/post text
                                    if let Some(data) = e.get("data").and_then(|v| v.as_array()) {
                                        for d in data {
                                            if let Some(comment) =
                                                d.get("comment").and_then(|v| v.as_object())
                                            {
                                                if let Some(text) =
                                                    comment.get("comment").and_then(|v| v.as_str())
                                                {
                                                    let _ =
                                                        writeln!(out, "> {}", fix_encoding(text));
                                                }
                                            }
                                        }
                                    }
                                    let _ = writeln!(out);
                                }

                                let path = out_dir.join("group-posts-and-comments.md");
                                write_source(&path, &out, dry_run)?;
                                files_written += 1;
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(CategoryReport {
        category: "groups".into(),
        files_written,
        items_processed: items,
    })
}

// ---------------------------------------------------------------------------
// Filesystem helpers
// ---------------------------------------------------------------------------

fn read_dir_sorted(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut entries: Vec<PathBuf> = fs::read_dir(dir)
        .map_err(|e| Error::io(dir, e))?
        .filter_map(|e: std::result::Result<fs::DirEntry, _>| e.ok())
        .map(|e: fs::DirEntry| e.path())
        .filter(|p: &PathBuf| {
            !p.file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .starts_with('.')
        })
        .collect();
    entries.sort();
    Ok(entries)
}

fn list_json_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(std::result::Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "json"))
        .collect();
    files.sort();
    files
}
