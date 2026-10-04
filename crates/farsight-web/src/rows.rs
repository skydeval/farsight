//! One page of a UI section that sorts by its rows' shown time (design
//! §7.6, §8.6), for the public and the admin pages alike.
//!
//! The admin pages page by cursor ([`page`]); the public tables by page
//! number ([`numbered`], [`numbered_naming`]), in the same order.
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
use farsight_storage::ui_rows::{self, Filter, NamingRow, Order, Position, Row, Section};

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

/// A numbered page of a public table: its rows and whether a page
/// follows.
#[derive(Debug, Clone)]
pub struct Numbered<R> {
    /// Rows, in the section's order; at most the page size.
    pub rows: Vec<R>,
    /// The query had a row beyond this page.
    pub more: bool,
}

impl<R> Default for Numbered<R> {
    fn default() -> Self {
        Numbered {
            rows: Vec::new(),
            more: false,
        }
    }
}

impl<R> Numbered<R> {
    /// `rows` read with one row more than the page holds.
    fn of(mut rows: Vec<R>, limit: i64) -> Numbered<R> {
        let more = rows.len() as i64 > limit;
        rows.truncate(limit as usize);
        Numbered { rows, more }
    }
}

/// Page `number` (from 1) of `section` for `key`, `limit` rows a page, in
/// the order the section uses now. Reads one row more than the page to
/// know whether another follows.
pub async fn numbered(
    st: &WebState,
    section: Section,
    key: i64,
    filter: Filter<'_>,
    number: i64,
    limit: i64,
) -> Result<Numbered<Row>, XrpcError> {
    let order = st.sort.order(section);
    let mut tx = st.api.read_tx().await?;
    let rows = ui_rows::rows_at(
        &mut tx,
        section,
        key,
        order,
        filter,
        (number - 1) * limit,
        limit + 1,
    )
    .await?;
    tx.rollback().await?;
    Ok(Numbered::of(rows, limit))
}

/// Page `number` of the lists naming the account `subject`, newest first
/// by shown time.
pub async fn numbered_naming(
    st: &WebState,
    subject: i64,
    excluded: &[i64],
    number: i64,
    limit: i64,
) -> Result<Numbered<NamingRow>, XrpcError> {
    let mut tx = st.api.read_tx().await?;
    let rows =
        ui_rows::lists_naming(&mut tx, subject, excluded, (number - 1) * limit, limit + 1).await?;
    tx.rollback().await?;
    Ok(Numbered::of(rows, limit))
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
    fn a_numbered_page_knows_whether_another_follows() {
        let full = Numbered::of(vec![1, 2, 3], 2);
        assert_eq!((full.rows, full.more), (vec![1, 2], true));
        let last = Numbered::of(vec![1, 2], 2);
        assert_eq!((last.rows, last.more), (vec![1, 2], false));
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
