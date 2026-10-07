//! One page of a UI section that sorts by its rows' shown time (see
//! `docs/design/storage.md`), for the public and the admin pages alike.
//!
//! Every table turns by page number ([`numbered`], [`numbered_naming`]).
//! A section reads its rows the same way whether or not its sort index is
//! ready: the index decides the order ([`SortIndexes`]), never the columns
//! or the filters.
//!
//! [`SortIndexes`]: farsight_storage::ui_rows::SortIndexes

use farsight_api::error::XrpcError;
use farsight_storage::ui_rows::{self, Filter, NamingRow, Row, Section};

use crate::pages::WebState;

/// A numbered page of a table: its rows and whether a page
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
    find: Option<&ui_rows::Find>,
    number: i64,
    limit: i64,
) -> Result<Numbered<NamingRow>, XrpcError> {
    let mut tx = st.api.read_tx().await?;
    let rows = ui_rows::lists_naming(
        &mut tx,
        subject,
        excluded,
        find,
        (number - 1) * limit,
        limit + 1,
    )
    .await?;
    tx.rollback().await?;
    Ok(Numbered::of(rows, limit))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_numbered_page_knows_whether_another_follows() {
        let full = Numbered::of(vec![1, 2, 3], 2);
        assert_eq!((full.rows, full.more), (vec![1, 2], true));
        let last = Numbered::of(vec![1, 2], 2);
        assert_eq!((last.rows, last.more), (vec![1, 2], false));
    }
}
