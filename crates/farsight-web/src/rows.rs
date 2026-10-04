//! One page of a UI section that sorts by its rows' shown time (design
//! §7.6, §8.6), for the public and the admin pages alike.
//!
//! A section reads its rows through [`page`] whether or not its sort index
//! is ready: the index decides the order ([`SortIndexes`]), never the
//! columns or the filters. A cursor is accepted only in the order the
//! section is using. Cursors of the shown-time order are tagged, so one of
//! the other order never parses; a link made before a section switched
//! therefore fails once, visibly, instead of being read as a position it
//! does not mean.

use chrono::{DateTime, Utc};
use farsight_api::cursor;
use farsight_api::error::XrpcError;
use farsight_storage::ui_rows::{
    self, Filter, NamingPosition, NamingRow, Order, Position, Row, Section,
};

use crate::pages::WebState;

/// The rows of one page and the cursor of the next.
#[derive(Debug, Clone, Default)]
pub struct Page {
    /// Rows, in the section's order.
    pub rows: Vec<Row>,
    /// Cursor continuing after the last row; `None` when the query ran
    /// out.
    pub next: Option<String>,
}

fn time_of(micros: Option<i64>) -> Result<Option<DateTime<Utc>>, XrpcError> {
    match micros {
        None => Ok(None),
        Some(us) => DateTime::<Utc>::from_timestamp_micros(us)
            .map(Some)
            .ok_or_else(|| XrpcError::invalid("invalid cursor")),
    }
}

/// Reads a cursor of `section` in `order`.
pub fn decode(
    section: Section,
    order: Order,
    raw: Option<&str>,
) -> Result<Option<Position>, XrpcError> {
    let by_party = section.ties_by_party();
    Ok(match order {
        Order::Stored if section == Section::OutgoingBlocks => {
            cursor::rkey(raw)?.map(|rkey| Position {
                time: None,
                party_id: 0,
                rkey,
            })
        }
        Order::Stored => cursor::id_rkey(raw)?.map(|(party_id, rkey)| Position {
            time: None,
            party_id,
            rkey,
        }),
        Order::Shown if by_party => match cursor::shown_id_rkey(raw)? {
            Some((t, party_id, rkey)) => Some(Position {
                time: time_of(t)?,
                party_id,
                rkey,
            }),
            None => None,
        },
        Order::Shown => match cursor::shown_rkey(raw)? {
            Some((t, rkey)) => Some(Position {
                time: time_of(t)?,
                party_id: 0,
                rkey,
            }),
            None => None,
        },
    })
}

/// The cursor after `row` of `section` in `order`.
pub fn encode(section: Section, order: Order, row: &Row) -> String {
    use serde_json::json;
    match order {
        Order::Stored if section == Section::OutgoingBlocks => cursor::encode(&[json!(row.rkey)]),
        Order::Stored => cursor::encode(&[json!(row.party_id), json!(row.rkey)]),
        Order::Shown => cursor::encode_shown(
            row.shown_time().map(|t| t.timestamp_micros()),
            section.ties_by_party().then_some(row.party_id),
            &row.rkey,
        ),
    }
}

/// One page of `section` for `key`, in the order the section uses now.
pub async fn page(
    st: &WebState,
    section: Section,
    key: i64,
    filter: Filter<'_>,
    raw_cursor: Option<&str>,
    limit: i64,
) -> Result<Page, XrpcError> {
    let order = st.sort.order(section);
    let after = decode(section, order, raw_cursor)?;
    let mut tx = st.api.read_tx().await?;
    let rows = ui_rows::rows(&mut tx, section, key, order, filter, after.as_ref(), limit).await?;
    tx.rollback().await?;
    let next = (rows.len() as i64 == limit)
        .then(|| rows.last())
        .flatten()
        .map(|r| encode(section, order, r));
    Ok(Page { rows, next })
}

/// One page of the lists naming an account and the cursor of the next.
#[derive(Debug, Clone, Default)]
pub struct NamingPage {
    /// Rows, newest first by shown time.
    pub rows: Vec<NamingRow>,
    /// Cursor continuing after the last row; `None` when the query ran
    /// out.
    pub next: Option<String>,
}

/// Reads a cursor of the lists section: a tagged shown time and the list
/// id. A cursor of the order the section had before (a bare list id) does
/// not parse.
pub fn decode_naming(raw: Option<&str>) -> Result<Option<NamingPosition>, XrpcError> {
    Ok(match cursor::shown_id_rkey(raw)? {
        Some((t, list_id, _)) => Some((time_of(t)?, list_id)),
        None => None,
    })
}

/// The cursor after `row` of the lists section.
pub fn encode_naming(row: &NamingRow) -> String {
    cursor::encode_shown(
        row.shown_time().map(|t| t.timestamp_micros()),
        Some(row.list_id),
        "",
    )
}

/// One page of the lists naming the account `subject` (`None`: an account
/// this instance has no row for), newest first by shown time. The cursor
/// is checked either way.
pub async fn naming_page(
    st: &WebState,
    subject: Option<i64>,
    excluded: &[i64],
    raw_cursor: Option<&str>,
    limit: i64,
) -> Result<NamingPage, XrpcError> {
    let after = decode_naming(raw_cursor)?;
    let Some(subject) = subject else {
        return Ok(NamingPage::default());
    };
    let mut tx = st.api.read_tx().await?;
    let rows = ui_rows::lists_naming(&mut tx, subject, excluded, after, limit).await?;
    tx.rollback().await?;
    let next = (rows.len() as i64 == limit)
        .then(|| rows.last())
        .flatten()
        .map(encode_naming);
    Ok(NamingPage { rows, next })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn row() -> Row {
        Row {
            party_id: 42,
            did: "did:plc:x".into(),
            rkey: "3l2x".into(),
            // Stated in the future: the position is where it arrived.
            created_at: Some(Utc.with_ymd_and_hms(9999, 12, 31, 0, 0, 0).unwrap()),
            first_seen: Some(Utc.with_ymd_and_hms(2026, 10, 3, 1, 2, 3).unwrap()),
        }
    }

    #[test]
    fn cursors_round_trip_in_their_own_order() {
        let r = row();
        for s in Section::ALL {
            for o in [Order::Stored, Order::Shown] {
                let c = encode(s, o, &r);
                let p = decode(s, o, Some(&c)).unwrap().unwrap();
                assert_eq!(p.rkey, "3l2x");
                if o == Order::Shown {
                    assert_eq!(p.time, r.first_seen, "{s:?}");
                }
                assert_eq!(decode(s, o, None).unwrap(), None);
            }
        }
    }

    #[test]
    fn a_cursor_of_the_other_order_is_refused() {
        let r = row();
        for s in Section::ALL {
            let old = encode(s, Order::Stored, &r);
            let new = encode(s, Order::Shown, &r);
            assert!(decode(s, Order::Shown, Some(&old)).is_err(), "{s:?}");
            assert!(decode(s, Order::Stored, Some(&new)).is_err(), "{s:?}");
        }
        assert!(decode(Section::ListMembers, Order::Shown, Some("!!")).is_err());
    }

    #[test]
    fn a_lists_cursor_carries_the_clamped_time_and_the_list() {
        let r = NamingRow {
            list_id: 7,
            owner_did: "did:plc:x".into(),
            rkey: "3l2x".into(),
            name: None,
            listblock_count: 0,
            added_at: Some(Utc.with_ymd_and_hms(9999, 12, 31, 0, 0, 0).unwrap()),
            first_seen: Some(Utc.with_ymd_and_hms(2026, 10, 3, 1, 2, 3).unwrap()),
        };
        let c = encode_naming(&r);
        assert_eq!(decode_naming(Some(&c)).unwrap(), Some((r.first_seen, 7)));
        assert_eq!(decode_naming(None).unwrap(), None);
        // The cursor the section had before: a bare list id.
        let old = cursor::encode(&[serde_json::json!(7)]);
        assert!(decode_naming(Some(&old)).is_err());
        assert!(decode_naming(Some("bm9wZQ")).is_err());
    }

    #[test]
    fn a_row_without_a_time_sorts_last() {
        let mut r = row();
        r.created_at = None;
        r.first_seen = None;
        let c = encode(Section::ListMembers, Order::Shown, &r);
        let p = decode(Section::ListMembers, Order::Shown, Some(&c))
            .unwrap()
            .unwrap();
        assert_eq!(p.time, None);
    }
}
