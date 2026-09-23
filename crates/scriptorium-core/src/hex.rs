//! Lowercase hex encoding for digests.
//!
//! `sha2` 0.11 returns `hybrid_array::Array` digests, which (unlike the
//! `GenericArray` of `sha2` 0.10) do not implement `LowerHex`, so
//! `format!("{:x}", digest)` no longer compiles.

use std::fmt::Write as _;

/// Encode `bytes` as lowercase hex, two characters per byte.
pub(crate) fn lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // Writing to a `String` cannot fail.
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    #[test]
    fn matches_known_sha256_vectors() {
        assert_eq!(
            super::lower(&Sha256::digest(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            super::lower(&Sha256::digest(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn pads_each_byte_to_two_chars() {
        assert_eq!(super::lower(&[0x00, 0x0f, 0xa0, 0xff]), "000fa0ff");
        assert_eq!(super::lower(&[]), "");
    }
}
