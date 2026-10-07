//! Websocket connections to one Jetstream instance (see
//! `docs/design/firehose.md`).
//!
//! [`connect`] opens either protocol at a cursor. The v2 handshake is the
//! detection probe: an instance that does not offer `subscribeEvents`
//! answers it with 404, and the caller falls back to v1. v2 rejects some
//! requests before the upgrade with an XRPC error body (`CursorTooOld`,
//! `UnknownZstdDictionary`); those come back as typed [`ConnectError`]s.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::dict::{self, LEGACY_DICT, V2_DICT};
use crate::frame::{self, Frame, Protocol};
use crate::resume::Cursor;

/// The four indexed collections (`wantedCollections` / `collections`).
pub const COLLECTIONS: [&str; 4] = farsight_core::nsid::INDEXED_COLLECTIONS;

/// v2 subprotocol.
pub const V2_SUBPROTOCOL: &str = "xrpc.v1.json";

/// Handshake timeout.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// Why a connection was not established.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConnectError {
    /// The instance does not serve this protocol's endpoint (404).
    #[error("endpoint not offered (HTTP {0})")]
    NotOffered(u16),
    /// v2: the seq cursor is below the retention floor.
    #[error("CursorTooOld: {0}")]
    CursorTooOld(String),
    /// v2: the dictionary ID is unknown or retired.
    #[error("UnknownZstdDictionary: {0}")]
    UnknownDictionary(String),
    /// Any other HTTP rejection.
    #[error("HTTP {0}: {1}")]
    Http(u16, String),
    /// Network / TLS / websocket failure or timeout.
    #[error("connect: {0}")]
    Transport(String),
}

/// Why reading stopped.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReadError {
    /// Socket or protocol error.
    #[error("websocket: {0}")]
    Socket(String),
    /// A message that is not a decodable frame.
    #[error("{0}")]
    Frame(#[from] frame::FrameError),
    /// zstd failure.
    #[error("decompress: {0}")]
    Decompress(String),
}

/// Strips a trailing `/` and a legacy `/subscribe` path from a configured
/// instance URL, leaving `wss://host[:port]`.
pub fn instance_base(url: &str) -> String {
    let u = url.trim_end_matches('/');
    u.strip_suffix("/subscribe").unwrap_or(u).to_owned()
}

/// The endpoint URL for `protocol` at `cursor`.
pub fn endpoint(base: &str, protocol: Protocol, cursor: Cursor, compress: bool) -> String {
    let base = instance_base(base);
    let mut q: Vec<String> = Vec::new();
    let key = match protocol {
        Protocol::V1 => "wantedCollections",
        Protocol::V2 => "collections",
    };
    for c in COLLECTIONS {
        q.push(format!("{key}={c}"));
    }
    match cursor {
        Cursor::Live => {}
        Cursor::Seq(s) => q.push(format!("cursor={s}")),
        Cursor::TimeUs(t) => q.push(format!("cursor={t}")),
    }
    if compress {
        match protocol {
            Protocol::V1 => q.push("compress=true".to_owned()),
            Protocol::V2 => {
                if let Some(id) = dict::dictionary_id(V2_DICT) {
                    q.push(format!("zstdDictionary={id}"));
                }
            }
        }
    }
    let path = match protocol {
        Protocol::V1 => "/subscribe",
        Protocol::V2 => "/xrpc/network.bsky.jetstream.subscribeEvents",
    };
    format!("{base}{path}?{}", q.join("&"))
}

/// An open session.
pub struct Session {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    /// Negotiated protocol.
    pub protocol: Protocol,
    /// Dictionary for binary frames, if compression was requested.
    dict: Option<&'static [u8]>,
    /// The URL connected to.
    pub url: String,
    /// The instance base URL (as passed to [`connect`]).
    pub url_base: String,
}

fn xrpc_error_name(body: &[u8]) -> Option<(String, String)> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    Some((
        v.get("error")?.as_str()?.to_owned(),
        v.get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .to_owned(),
    ))
}

/// Opens `protocol` on instance `base` at `cursor`.
pub async fn connect(
    base: &str,
    protocol: Protocol,
    cursor: Cursor,
    compress: bool,
) -> Result<Session, ConnectError> {
    let url = endpoint(base, protocol, cursor, compress);
    let mut req = url
        .as_str()
        .into_client_request()
        .map_err(|e| ConnectError::Transport(e.to_string()))?;
    if protocol == Protocol::V2 {
        req.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_static(V2_SUBPROTOCOL),
        );
    }
    // No message, compressed or not, may be larger than a decompressed
    // frame may be: the library's own limit is four times that.
    let limit = usize::try_from(dict::MAX_FRAME_BYTES).unwrap_or(usize::MAX);
    let config = WebSocketConfig::default()
        .max_message_size(Some(limit))
        .max_frame_size(Some(limit));
    let res = tokio::time::timeout(
        CONNECT_TIMEOUT,
        tokio_tungstenite::connect_async_with_config(req, Some(config), false),
    )
    .await;
    let ws = match res {
        Err(_) => return Err(ConnectError::Transport("handshake timed out".into())),
        Ok(Ok((ws, _))) => ws,
        Ok(Err(WsError::Http(resp))) => {
            let status = resp.status().as_u16();
            let body = resp.body().clone().unwrap_or_default();
            if status == 404 || status == 405 || status == 501 {
                return Err(ConnectError::NotOffered(status));
            }
            return Err(match xrpc_error_name(&body) {
                Some((name, msg)) if name == "CursorTooOld" => ConnectError::CursorTooOld(msg),
                Some((name, msg)) if name == "UnknownZstdDictionary" => {
                    ConnectError::UnknownDictionary(msg)
                }
                _ => ConnectError::Http(status, String::from_utf8_lossy(&body).into_owned()),
            });
        }
        Ok(Err(e)) => return Err(ConnectError::Transport(e.to_string())),
    };
    let dict = match (compress, protocol) {
        (false, _) => None,
        (true, Protocol::V1) => Some(LEGACY_DICT),
        (true, Protocol::V2) => Some(V2_DICT),
    };
    Ok(Session {
        ws,
        protocol,
        dict,
        url,
        url_base: base.to_owned(),
    })
}

impl Session {
    /// The next decoded frame; `None` when the server closed the stream.
    pub async fn next_frame(&mut self) -> Option<Result<Frame, ReadError>> {
        Some(match self.next_raw().await? {
            Ok(raw) => self.decode(&raw),
            Err(e) => Err(e),
        })
    }

    /// The next frame as the source sent it, decompressed and not
    /// decoded; `None` when the server closed the stream.
    pub async fn next_raw(&mut self) -> Option<Result<Vec<u8>, ReadError>> {
        loop {
            let msg = match self.ws.next().await? {
                Ok(m) => m,
                Err(e) => return Some(Err(ReadError::Socket(e.to_string()))),
            };
            let raw = match msg {
                Message::Text(t) => Ok(t.as_bytes().to_vec()),
                Message::Binary(b) => match self.dict {
                    Some(d) => {
                        dict::decompress(&b, d).map_err(|e| ReadError::Decompress(e.to_string()))
                    }
                    None => Ok(b.to_vec()),
                },
                Message::Close(_) => return None,
                // Pings are answered by the library while reading.
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
            };
            return Some(raw);
        }
    }

    fn decode(&self, bytes: &[u8]) -> Result<Frame, ReadError> {
        Ok(match self.protocol {
            Protocol::V1 => frame::decode_v1(bytes)?,
            Protocol::V2 => frame::decode_v2(bytes)?,
        })
    }

    /// Closes the socket (best effort).
    pub async fn close(mut self) {
        let _ = tokio::time::timeout(Duration::from_secs(2), self.ws.close(None)).await;
        let _ = self.ws.flush().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints() {
        let v1 = endpoint(
            "wss://js.example/subscribe/",
            Protocol::V1,
            Cursor::TimeUs(5),
            true,
        );
        assert_eq!(
            v1,
            "wss://js.example/subscribe?wantedCollections=app.bsky.graph.block\
             &wantedCollections=app.bsky.graph.listblock&wantedCollections=app.bsky.graph.list\
             &wantedCollections=app.bsky.graph.listitem&cursor=5&compress=true"
        );
        let v2 = endpoint("wss://js.example", Protocol::V2, Cursor::Seq(9), true);
        let id = dict::dictionary_id(V2_DICT).unwrap();
        assert!(v2.starts_with("wss://js.example/xrpc/network.bsky.jetstream.subscribeEvents?"));
        assert!(v2.contains("collections=app.bsky.graph.listitem"));
        assert!(v2.ends_with(&format!("&cursor=9&zstdDictionary={id}")));
        let live = endpoint("wss://js.example", Protocol::V2, Cursor::Live, false);
        assert!(!live.contains("cursor=") && !live.contains("zstd"));
    }

    #[test]
    fn xrpc_errors() {
        assert_eq!(
            xrpc_error_name(br#"{"error":"CursorTooOld","message":"floor 12"}"#),
            Some(("CursorTooOld".into(), "floor 12".into()))
        );
        assert_eq!(xrpc_error_name(b"plain text"), None);
    }
}
