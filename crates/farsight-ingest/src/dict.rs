//! Bundled Jetstream zstd dictionaries and frame decompression (see
//! `docs/design/firehose.md`).

use std::io::Read;

/// v1 `/subscribe?compress=true` dictionary.
pub const LEGACY_DICT: &[u8] = include_bytes!("../dictionaries/legacy_subscribe.zdict");
/// v2 `subscribeEvents` dictionary (dated 2026-08-11 upstream).
pub const V2_DICT: &[u8] = include_bytes!("../dictionaries/subscribe_events_20260811.zdict");

/// Largest decompressed frame accepted (guards against a hostile or
/// broken instance; real frames are a few KiB).
pub const MAX_FRAME_BYTES: u64 = 16 * 1024 * 1024;

/// The zstd dictionary ID in a structured dictionary's header (RFC 8878,
/// section 5: magic `0xEC30A437`, then a little-endian u32 ID).
pub fn dictionary_id(dict: &[u8]) -> Option<u32> {
    if dict.len() < 8 || dict[..4] != [0x37, 0xa4, 0x30, 0xec] {
        return None;
    }
    Some(u32::from_le_bytes([dict[4], dict[5], dict[6], dict[7]]))
}

/// Why a frame could not be decompressed.
#[derive(Debug, thiserror::Error)]
pub enum DecompressError {
    /// zstd rejected the frame.
    #[error("zstd: {0}")]
    Zstd(#[from] std::io::Error),
    /// The frame expands beyond [`MAX_FRAME_BYTES`].
    #[error("decompressed frame exceeds {MAX_FRAME_BYTES} bytes")]
    TooLarge,
}

/// Decompresses one zstd frame against `dict`.
pub fn decompress(frame: &[u8], dict: &[u8]) -> Result<Vec<u8>, DecompressError> {
    let decoder = zstd::stream::read::Decoder::with_dictionary(frame, dict)?;
    let mut out = Vec::with_capacity(frame.len() * 4);
    let n = decoder.take(MAX_FRAME_BYTES + 1).read_to_end(&mut out)?;
    if n as u64 > MAX_FRAME_BYTES {
        return Err(DecompressError::TooLarge);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_dictionaries_have_ids() {
        assert!(dictionary_id(LEGACY_DICT).is_some());
        assert!(dictionary_id(V2_DICT).is_some());
        assert_ne!(dictionary_id(LEGACY_DICT), dictionary_id(V2_DICT));
        assert_eq!(dictionary_id(b"not a dictionary"), None);
    }

    #[test]
    fn round_trip_with_dictionary() {
        let text = br#"{"did":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa","time_us":1,"kind":"commit"}"#;
        let mut c = zstd::bulk::Compressor::with_dictionary(3, LEGACY_DICT).unwrap();
        let frame = c.compress(text).unwrap();
        assert_eq!(decompress(&frame, LEGACY_DICT).unwrap(), text);
        assert!(decompress(b"garbage", LEGACY_DICT).is_err());
    }
}
