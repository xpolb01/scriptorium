#![allow(clippy::doc_markdown)]

//! Payload capping and truncation with SHA-256 tracking.
//!
//! This module provides utilities to safely truncate string bodies and
//! attribute values at UTF-8 character boundaries, recording truncation
//! metadata (original size, preview size, SHA-256 hash) for telemetry purposes.
//!
//! Truncation boundaries are always UTF-8 safe — never splits a multi-byte
//! character — and capped values are decorated with `telemetry.truncated` and
//! `telemetry.truncated_fields` attributes for observability.

use crate::telemetry::envelope::{Attributes, PayloadCap};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Metadata recorded when a body or attribute value is truncated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TruncationMeta {
    /// Field name: `"body"` for body truncation, or the attribute key name.
    pub field: String,
    /// Byte length of the original input before truncation.
    pub original_len: usize,
    /// Byte length of the returned (truncated or unchanged) preview.
    pub preview_len: usize,
    /// SHA-256 hash of the original input as 64-character lowercase hex string.
    pub sha256_hex: String,
    /// `true` iff the input was non-UTF-8 bytes (binary), in which case
    /// the preview is base64-encoded.
    pub binary: bool,
}

/// Truncate a string body at a UTF-8 character boundary, respecting
/// `cap.max_body_bytes`.
///
/// # Returns
///
/// A tuple `(preview, truncation_meta)` where:
/// - If the body fits within the cap, returns `(body.to_string(), None)`.
/// - If the body exceeds the cap, returns `(preview, Some(meta))` where
///   `preview` is the safely truncated string at a UTF-8 boundary, and
///   `meta` records the original size and SHA-256 hash.
///
/// # Example
///
/// ```ignore
/// use scriptorium_core::telemetry::payload::cap_body;
/// use scriptorium_core::telemetry::DEFAULT_PAYLOAD_CAP;
///
/// let body = "hello world";
/// let (preview, meta) = cap_body(body, &DEFAULT_PAYLOAD_CAP);
/// assert_eq!(preview, "hello world");
/// assert!(meta.is_none());
///
/// let long_body = "x".repeat(10_000);
/// let (preview, meta) = cap_body(&long_body, &DEFAULT_PAYLOAD_CAP);
/// assert!(preview.len() <= 8192);
/// assert!(meta.is_some());
/// ```
pub fn cap_body(body: &str, cap: &PayloadCap) -> (String, Option<TruncationMeta>) {
    let body_bytes = body.as_bytes();
    if body_bytes.len() <= cap.max_body_bytes {
        return (body.to_string(), None);
    }

    // Find the largest byte offset ≤ cap.max_body_bytes that is a valid
    // UTF-8 character boundary.
    let safe_len = safe_utf8_truncate(body, cap.max_body_bytes);

    let preview = body[..safe_len].to_string();
    let sha256_hex = compute_sha256_hex(body_bytes);

    let meta = TruncationMeta {
        field: "body".into(),
        original_len: body_bytes.len(),
        preview_len: safe_len,
        sha256_hex,
        binary: false,
    };

    (preview, Some(meta))
}

/// Truncate each attribute value at a UTF-8 character boundary, respecting
/// `cap.max_attr_value_bytes`.
///
/// For each attribute, the value is serialized to JSON (if not already a
/// primitive string), then truncated using the same UTF-8 boundary logic
/// as [`cap_body`]. If truncation occurs, the preview is stored as a JSON
/// string (to avoid breaking JSON syntax).
///
/// # Returns
///
/// A tuple `(mutated_attrs, metas)` where:
/// - `mutated_attrs` is the input attributes with truncated values replaced.
/// - `metas` is a vector of [`TruncationMeta`] (one per truncated attribute),
///   suitable for decoration via [`add_truncation_attrs`].
///
/// # Example
///
/// ```ignore
/// use scriptorium_core::telemetry::payload::cap_attributes;
/// use scriptorium_core::telemetry::DEFAULT_PAYLOAD_CAP;
/// use std::collections::BTreeMap;
///
/// let mut attrs = BTreeMap::new();
/// attrs.insert("key1".into(), serde_json::json!("small"));
/// attrs.insert("key2".into(), serde_json::json!("x".repeat(5000)));
///
/// let (mutated, metas) = cap_attributes(attrs, &DEFAULT_PAYLOAD_CAP);
/// assert_eq!(metas.len(), 1);
/// assert_eq!(metas[0].field, "key2");
/// ```
pub fn cap_attributes(
    mut attrs: Attributes,
    cap: &PayloadCap,
) -> (Attributes, Vec<TruncationMeta>) {
    let mut metas = Vec::new();

    for (key, value) in attrs.iter_mut() {
        // Serialize the value to JSON, then truncate.
        let json_str = serde_json::to_string(&value)
            .unwrap_or_else(|_| format!("{{\"error\": \"serialization failed\"}}"));

        let json_bytes = json_str.as_bytes();
        if json_bytes.len() <= cap.max_attr_value_bytes {
            continue;
        }

        // Truncate at a UTF-8 boundary.
        let safe_len = safe_utf8_truncate(&json_str, cap.max_attr_value_bytes);
        let preview = json_str[..safe_len].to_string();

        let sha256_hex = compute_sha256_hex(json_bytes);

        let meta = TruncationMeta {
            field: key.clone(),
            original_len: json_bytes.len(),
            preview_len: safe_len,
            sha256_hex,
            binary: false,
        };

        // Replace the value with the truncated preview as a JSON string
        // to avoid breaking JSON structure.
        *value = serde_json::Value::String(preview);
        metas.push(meta);
    }

    (attrs, metas)
}

/// Truncate raw bytes, encoding the result as base64.
///
/// For binary payloads (non-UTF-8), this function encodes the input as
/// base64. If the input exceeds the cap, only the first `cap.max_body_bytes`
/// bytes are encoded.
///
/// # Returns
///
/// A tuple `(base64_preview, truncation_meta)` where:
/// - If the input fits within the cap, returns `(base64(all bytes), None)`.
/// - If the input exceeds the cap, returns `(base64(first cap bytes), Some(meta))`
///   with `meta.binary = true`.
///
/// # Example
///
/// ```ignore
/// use scriptorium_core::telemetry::payload::cap_bytes;
/// use scriptorium_core::telemetry::DEFAULT_PAYLOAD_CAP;
///
/// let data = vec![0u8; 100];
/// let (encoded, meta) = cap_bytes(&data, "binary_field", &DEFAULT_PAYLOAD_CAP);
/// assert!(meta.is_none());
///
/// let large_data = vec![0u8; 10_000];
/// let (encoded, meta) = cap_bytes(&large_data, "binary_field", &DEFAULT_PAYLOAD_CAP);
/// assert!(meta.is_some());
/// assert!(meta.unwrap().binary);
/// ```
pub fn cap_bytes(
    bytes: &[u8],
    field_name: &str,
    cap: &PayloadCap,
) -> (String, Option<TruncationMeta>) {
    if bytes.len() <= cap.max_body_bytes {
        let encoded = BASE64.encode(bytes);
        return (encoded, None);
    }

    let capped_bytes = &bytes[..cap.max_body_bytes];
    let encoded = BASE64.encode(capped_bytes);

    let sha256_hex = compute_sha256_hex(bytes);

    let meta = TruncationMeta {
        field: field_name.into(),
        original_len: bytes.len(),
        preview_len: cap.max_body_bytes,
        sha256_hex,
        binary: true,
    };

    (encoded, Some(meta))
}

/// Decorate an [`Attributes`] map with truncation markers.
///
/// If `metas` is non-empty, this function inserts:
/// - `"telemetry.truncated"`: `true` (boolean)
/// - `"telemetry.truncated_fields"`: JSON array of truncation metadata
///
/// If `metas` is empty, the function is a no-op.
///
/// # Example
///
/// ```ignore
/// use scriptorium_core::telemetry::payload::{cap_body, add_truncation_attrs};
/// use scriptorium_core::telemetry::DEFAULT_PAYLOAD_CAP;
/// use std::collections::BTreeMap;
///
/// let long_body = "x".repeat(10_000);
/// let (preview, meta) = cap_body(&long_body, &DEFAULT_PAYLOAD_CAP);
///
/// let mut attrs = BTreeMap::new();
/// if let Some(m) = meta {
///     add_truncation_attrs(&mut attrs, &[m]);
/// }
///
/// assert_eq!(attrs.get("telemetry.truncated"), Some(&serde_json::json!(true)));
/// assert!(attrs.contains_key("telemetry.truncated_fields"));
/// ```
pub fn add_truncation_attrs(attrs: &mut Attributes, metas: &[TruncationMeta]) {
    if metas.is_empty() {
        return;
    }

    attrs.insert("telemetry.truncated".into(), serde_json::Value::Bool(true));

    if let Ok(fields_json) = serde_json::to_value(metas) {
        attrs.insert("telemetry.truncated_fields".into(), fields_json);
    }
}

// ── Helper functions ─────────────────────────────────────────────────────

/// Find the largest byte offset ≤ `max_bytes` that is a valid UTF-8
/// character boundary in the given string.
///
/// This ensures that truncation never splits a multi-byte UTF-8 codepoint.
fn safe_utf8_truncate(s: &str, max_bytes: usize) -> usize {
    if s.len() <= max_bytes {
        return s.len();
    }

    // Start from max_bytes and walk backward until we find a valid
    // character boundary.
    let mut i = max_bytes;
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

/// Compute the SHA-256 hash of a byte slice and return it as a 64-character
/// lowercase hexadecimal string.
fn compute_sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn default_cap() -> PayloadCap {
        PayloadCap {
            max_body_bytes: 8192,
            max_attr_value_bytes: 4096,
        }
    }

    #[test]
    fn cap_ascii_under_cap() {
        let body = "hello";
        let (preview, meta) = cap_body(body, &default_cap());
        assert_eq!(preview, "hello");
        assert!(meta.is_none());
    }

    #[test]
    fn cap_ascii_at_cap() {
        let body = "x".repeat(8192);
        let (preview, meta) = cap_body(&body, &default_cap());
        assert_eq!(preview, body);
        assert!(meta.is_none());
    }

    #[test]
    fn cap_ascii_over_cap() {
        let body = "x".repeat(10_000);
        let (preview, meta) = cap_body(&body, &default_cap());
        assert!(preview.len() <= 8192);
        assert_eq!(preview.as_bytes().len(), 8192);
        let m = meta.expect("should truncate");
        assert_eq!(m.field, "body");
        assert_eq!(m.original_len, 10_000);
        assert_eq!(m.preview_len, 8192);
        assert_eq!(m.sha256_hex.len(), 64);
        assert!(!m.binary);
    }

    #[test]
    fn cap_utf8_boundary() {
        // "é" is 2 bytes in UTF-8
        let body = "é".repeat(5000); // 10_000 bytes total
        let (preview, meta) = cap_body(&body, &default_cap());

        // Verify it's valid UTF-8
        assert!(std::str::from_utf8(preview.as_bytes()).is_ok());

        // Verify the length is a multiple of 2 (since each "é" is 2 bytes)
        assert_eq!(preview.as_bytes().len() % 2, 0);

        // Verify we got exactly 8192 bytes (max_body_bytes)
        assert_eq!(preview.as_bytes().len(), 8192);

        let m = meta.expect("should truncate");
        assert_eq!(m.original_len, 10_000);
        assert_eq!(m.preview_len, 8192);
    }

    #[test]
    fn cap_empty() {
        let (preview, meta) = cap_body("", &default_cap());
        assert_eq!(preview, "");
        assert!(meta.is_none());
    }

    #[test]
    fn cap_exactly_at_boundary_after_truncation() {
        // Ensure we don't loop infinitely if the cap lands inside a multi-byte char.
        // "héllo" is 6 bytes; 1367 repeats = 8202 bytes (just over 8192).
        let body = "héllo".repeat(1367);
        let (preview, meta) = cap_body(&body, &default_cap());

        assert!(std::str::from_utf8(preview.as_bytes()).is_ok());
        assert!(preview.as_bytes().len() <= 8192);

        // The original is > cap, so we should have truncation metadata
        assert!(meta.is_some());
    }

    #[test]
    fn cap_attributes_mixed() {
        let mut attrs = BTreeMap::new();
        attrs.insert("small_string".into(), serde_json::json!("hello"));
        attrs.insert("large_string".into(), serde_json::json!("x".repeat(5000)));
        attrs.insert("small_int".into(), serde_json::json!(42));

        let (mutated, metas) = cap_attributes(attrs, &default_cap());

        // Only large_string should be truncated
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].field, "large_string");
        assert_eq!(metas[0].original_len, 5002); // 5000 'x' + 2 quotes

        // Verify small values are unchanged
        assert_eq!(mutated.get("small_int"), Some(&serde_json::json!(42)));

        // Verify large string was truncated and is a string
        let truncated_val = mutated.get("large_string").unwrap();
        assert!(truncated_val.is_string());
        let truncated_str = truncated_val.as_str().unwrap();
        assert!(truncated_str.len() <= 4096);
    }

    #[test]
    fn cap_bytes_under() {
        let data = vec![0u8; 100];
        let (encoded, meta) = cap_bytes(&data, "test_field", &default_cap());
        assert!(meta.is_none());
        // Verify base64 decoding works
        let decoded = BASE64.decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn cap_bytes_over() {
        let data = vec![42u8; 10_000];
        let (encoded, meta) = cap_bytes(&data, "test_field", &default_cap());

        let m = meta.expect("should truncate");
        assert_eq!(m.field, "test_field");
        assert_eq!(m.original_len, 10_000);
        assert_eq!(m.preview_len, 8192);
        assert!(m.binary);

        // Verify base64 decoding works and we got exactly 8192 bytes
        let decoded = BASE64.decode(&encoded).unwrap();
        assert_eq!(decoded.len(), 8192);
        assert_eq!(&decoded[..], &vec![42u8; 8192][..]);
    }

    #[test]
    fn add_truncation_attrs_no_metas() {
        let mut attrs = BTreeMap::new();
        attrs.insert("existing".into(), serde_json::json!("value"));

        add_truncation_attrs(&mut attrs, &[]);

        assert_eq!(attrs.len(), 1);
        assert!(!attrs.contains_key("telemetry.truncated"));
    }

    #[test]
    fn add_truncation_attrs_sets_flags() {
        let mut attrs = BTreeMap::new();

        let meta = TruncationMeta {
            field: "test_field".into(),
            original_len: 1000,
            preview_len: 500,
            sha256_hex: "a".repeat(64),
            binary: false,
        };

        add_truncation_attrs(&mut attrs, &[meta.clone()]);

        assert_eq!(
            attrs.get("telemetry.truncated"),
            Some(&serde_json::json!(true))
        );
        assert!(attrs.contains_key("telemetry.truncated_fields"));

        let fields = attrs.get("telemetry.truncated_fields").unwrap();
        assert!(fields.is_array());
        let fields_arr = fields.as_array().unwrap();
        assert_eq!(fields_arr.len(), 1);

        let first_field = &fields_arr[0];
        assert_eq!(first_field["field"], "test_field");
        assert_eq!(first_field["original_len"], 1000);
        assert_eq!(first_field["sha256_hex"], "a".repeat(64));
    }

    #[test]
    fn sha256_produces_64_hex_chars() {
        let data = b"hello world";
        let hex = compute_sha256_hex(data);
        assert_eq!(hex.len(), 64);
        // Verify it's valid hex
        assert!(u128::from_str_radix(&hex[..32], 16).is_ok());
        assert!(u128::from_str_radix(&hex[32..], 16).is_ok());
    }
}
