//! Bundled Jetstream zstd dictionaries and frame decompression (see
//! `docs/design/firehose.md`).

use std::io::Read;
use std::sync::LazyLock;

use zstd::dict::DecoderDictionary;

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

/// The largest window a frame may ask the decoder for, as a power of
/// two: [`MAX_FRAME_BYTES`]. A frame cannot expand beyond that, so it
/// needs no larger window, and one that asks for more is refused before
/// any memory is set aside for it.
pub const WINDOW_LOG_MAX: u32 = 24;

/// One of the bundled dictionaries, as a session uses it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dictionary {
    /// [`LEGACY_DICT`].
    Legacy,
    /// [`V2_DICT`].
    V2,
}

impl Dictionary {
    /// The dictionary's bytes.
    pub fn bytes(self) -> &'static [u8] {
        match self {
            Dictionary::Legacy => LEGACY_DICT,
            Dictionary::V2 => V2_DICT,
        }
    }

    /// The dictionary in the form the decoder works from. Building that
    /// form (its tables) costs far more than expanding one small frame,
    /// so it is built once per process and shared by every frame.
    fn prepared(self) -> &'static DecoderDictionary<'static> {
        static LEGACY: LazyLock<DecoderDictionary<'static>> =
            LazyLock::new(|| DecoderDictionary::copy(LEGACY_DICT));
        static V2: LazyLock<DecoderDictionary<'static>> =
            LazyLock::new(|| DecoderDictionary::copy(V2_DICT));
        match self {
            Dictionary::Legacy => &LEGACY,
            Dictionary::V2 => &V2,
        }
    }
}

/// Decompresses one zstd frame against `dict`.
pub fn decompress(frame: &[u8], dict: Dictionary) -> Result<Vec<u8>, DecompressError> {
    let mut decoder =
        zstd::stream::read::Decoder::with_prepared_dictionary(frame, dict.prepared())?;
    decoder.window_log_max(WINDOW_LOG_MAX)?;
    // A guess at the expanded size, never more than a frame may be: the
    // frame's length is the sender's.
    let guess = frame.len().saturating_mul(4).min(MAX_FRAME_BYTES as usize);
    let mut out = Vec::with_capacity(guess);
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
        for dict in [Dictionary::Legacy, Dictionary::V2] {
            let mut c = zstd::bulk::Compressor::with_dictionary(3, dict.bytes()).unwrap();
            let frame = c.compress(text).unwrap();
            // Frame after frame from the one prepared dictionary.
            for _ in 0..3 {
                assert_eq!(decompress(&frame, dict).unwrap(), text);
            }
            assert!(decompress(b"garbage", dict).is_err());
        }
        // A frame made with one dictionary does not expand with the other.
        let mut c = zstd::bulk::Compressor::with_dictionary(3, LEGACY_DICT).unwrap();
        let frame = c.compress(text).unwrap();
        assert!(decompress(&frame, Dictionary::V2).is_err());
    }

    #[test]
    fn a_frame_that_asks_for_a_window_past_the_frame_bound_is_refused() {
        use std::io::Write;
        let text = vec![b'a'; 4096];
        let frame_with_window = |log: u32| {
            let mut e =
                zstd::stream::write::Encoder::with_dictionary(Vec::new(), 3, LEGACY_DICT).unwrap();
            e.window_log(log).unwrap();
            // Streamed, so the frame states its window instead of its
            // content size.
            e.write_all(&text).unwrap();
            e.finish().unwrap()
        };
        assert_eq!(
            decompress(&frame_with_window(WINDOW_LOG_MAX), Dictionary::Legacy).unwrap(),
            text
        );
        assert!(decompress(&frame_with_window(WINDOW_LOG_MAX + 2), Dictionary::Legacy).is_err());
    }
}
