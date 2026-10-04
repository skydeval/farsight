//! Numbered pages of the public tables (design §8.6).
//!
//! Every table holds [`PAGE_ROWS`](super::PAGE_ROWS) rows a page and ends
//! with page controls: `← 1 2 3 4 5 … 21 →`. Each control is a plain
//! link; the page is a query parameter of its section, so that the two or
//! three tables of a page turn independently. Page 1 has no parameter:
//! `?page=1` and the cursor parameters of earlier versions redirect to
//! the address without them.
//!
//! The last page comes from the section's count, which is exact up to
//! [`COUNT_CAP`](super::COUNT_CAP). Beyond it, or when the count could
//! not be read in time, the controls end in an ellipsis and the next
//! arrow, which works for as long as rows follow.

use askama::Template;
use farsight_api::params::Params;

use super::Fail;

/// The highest page number an address may name. A page past a section's
/// end is an empty table; this bound only keeps the offset a sane number.
pub const MAX_PAGE: i64 = 1_000_000;
/// Pages linked on each side of the current one.
pub const NEIGHBOURS: i64 = 2;

/// The page `key` asks for: 1 when the parameter is absent.
pub fn number(q: &Params, key: &str, base: &str) -> Result<i64, Fail> {
    let Some(raw) = q.get(key) else {
        return Ok(1);
    };
    match raw.parse::<i64>() {
        Ok(n) if (1..=MAX_PAGE).contains(&n) && raw.bytes().all(|b| b.is_ascii_digit()) => Ok(n),
        _ => Err(Fail::Bad {
            message: "This link names a page that cannot be read.".into(),
            link: Some((base.to_owned(), "Open the first page".to_owned())),
        }),
    }
}

/// The query string of a page of this address: the sections in `keys`
/// that are past their first page, in that order, with `key` set to
/// `page`. Empty when every section is on page 1.
fn query(q: &Params, keys: &[&str], set: Option<(&str, i64)>) -> String {
    let mut s = url::form_urlencoded::Serializer::new(String::new());
    for k in keys {
        let value = match set {
            Some((key, page)) if key == *k => Some(page.to_string()),
            _ => q.get(k).map(str::to_owned),
        };
        if let Some(v) = value.filter(|v| v != "1") {
            s.append_pair(k, &v);
        }
    }
    s.finish()
}

/// Where a request should have gone instead, if its address is not the
/// canonical one: it carries a cursor parameter of an earlier version
/// (`retired`), or names page 1 of a section outright. The other
/// sections keep their pages; a retired cursor's section starts over.
pub fn canonical(q: &Params, keys: &[&str], retired: &[&str]) -> Option<String> {
    let stale = retired.iter().any(|k| q.get(k).is_some());
    let first = keys.iter().any(|k| q.get(k) == Some("1"));
    (stale || first).then(|| query(q, keys, None))
}

/// `base?…` for page `page` of the section `key`, keeping the pages of
/// the other sections.
pub fn link(base: &str, q: &Params, keys: &[&str], key: &str, page: i64) -> String {
    let query = query(q, keys, Some((key, page)));
    if query.is_empty() {
        base.to_owned()
    } else {
        format!("{base}?{query}")
    }
}

/// What is known about a section's length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Total {
    /// This many rows, so the last page is known.
    Rows(i64),
    /// More than this many rows: the pages up to here exist, and one more.
    MoreThan(i64),
    /// The count was not available.
    Unknown,
}

/// One control.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Item {
    /// A page that is not the current one.
    Page(i64),
    /// The current page.
    Current(i64),
    /// Pages left out.
    Gap,
}

/// The controls of a section on page `current`, whose query found a row
/// beyond the page (`more`): the first page, the current one with its
/// neighbours, and the last page when it is known. `open`: the last page
/// is not known and the list ends in a gap.
pub fn items(current: i64, total: Total, more: bool, size: i64) -> (Vec<Item>, bool) {
    // The highest page known to exist.
    let seen = current + i64::from(more);
    let (last, open) = match total {
        Total::Rows(n) => (((n + size - 1) / size).max(seen).max(1), false),
        Total::MoreThan(n) => ((n / size + 1).max(seen), true),
        Total::Unknown => (seen, more),
    };
    // Past the cap only the next page is known to exist.
    let open = open && (more || current < last);
    let mut pages: Vec<i64> = vec![1];
    pages.extend((current - NEIGHBOURS).max(1)..=(current + NEIGHBOURS).min(last));
    if !open {
        pages.push(last);
    }
    pages.push(current);
    pages.sort_unstable();
    pages.dedup();
    let mut out = Vec::new();
    let mut before = 0;
    for p in pages {
        if p > before + 1 {
            out.push(Item::Gap);
        }
        out.push(if p == current {
            Item::Current(p)
        } else {
            Item::Page(p)
        });
        before = p;
    }
    if open {
        out.push(Item::Gap);
    }
    (out, open)
}

/// A control as the template prints it.
#[derive(Debug, Clone)]
pub struct Control {
    /// The page number; `None`: a gap.
    pub page: Option<i64>,
    /// Its address; `None`: the current page or a gap.
    pub href: Option<String>,
}

/// The page controls of a section. Rendered only when the section has
/// more than one page.
#[derive(Debug, Clone, Template)]
#[template(path = "_pagination.html")]
pub struct Pager {
    /// The section's fragment id.
    pub section: &'static str,
    /// What the section lists, for the label.
    pub label: &'static str,
    /// The current page.
    pub current: i64,
    /// The numbers and gaps.
    pub controls: Vec<Control>,
    /// The previous page, on every page but the first.
    pub prev: Option<String>,
    /// The next page, while one follows.
    pub next: Option<String>,
}

impl Pager {
    /// The controls of the section `key` at `base`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        section: &'static str,
        label: &'static str,
        base: &str,
        q: &Params,
        keys: &[&str],
        key: &str,
        current: i64,
        total: Total,
        more: bool,
        size: i64,
    ) -> Pager {
        let (items, open) = items(current, total, more, size);
        let last = items
            .iter()
            .filter_map(|i| match i {
                Item::Page(p) | Item::Current(p) => Some(*p),
                Item::Gap => None,
            })
            .max()
            .unwrap_or(1);
        let to = |p: i64| link(base, q, keys, key, p);
        Pager {
            section,
            label,
            current,
            controls: items
                .into_iter()
                .map(|i| match i {
                    Item::Page(p) => Control {
                        page: Some(p),
                        href: Some(to(p)),
                    },
                    Item::Current(p) => Control {
                        page: Some(p),
                        href: None,
                    },
                    Item::Gap => Control {
                        page: None,
                        href: None,
                    },
                })
                .collect(),
            prev: (current > 1).then(|| to(current - 1)),
            next: (more || (!open && current < last)).then(|| to(current + 1)),
        }
    }

    /// Whether there is anything to turn: more than the one page.
    pub fn shown(&self) -> bool {
        self.prev.is_some() || self.next.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Item::{Current as C, Gap as G, Page as P};

    #[test]
    fn known_totals_show_first_last_and_neighbours() {
        // 1,023 rows: 21 pages.
        let t = Total::Rows(1_023);
        assert_eq!(
            items(1, t, true, 50),
            (vec![C(1), P(2), P(3), G, P(21)], false)
        );
        assert_eq!(
            items(10, t, true, 50).0,
            vec![P(1), G, P(8), P(9), C(10), P(11), P(12), G, P(21)]
        );
        assert_eq!(
            items(21, t, false, 50).0,
            vec![P(1), G, P(19), P(20), C(21)]
        );
        // No gap is printed for a single missing step.
        assert_eq!(
            items(4, Total::Rows(300), true, 50).0,
            vec![P(1), P(2), P(3), C(4), P(5), P(6)]
        );
        // One page: nothing to turn.
        assert_eq!(items(1, Total::Rows(50), false, 50).0, vec![C(1)]);
        assert_eq!(items(1, Total::Rows(0), false, 50).0, vec![C(1)]);
        // 51 rows are two pages.
        assert_eq!(items(1, Total::Rows(51), true, 50).0, vec![C(1), P(2)]);
    }

    #[test]
    fn past_the_cap_the_list_stays_open() {
        let t = Total::MoreThan(1_000);
        // Pages 1–21 are known to exist; there is no last page.
        assert_eq!(items(1, t, true, 50), (vec![C(1), P(2), P(3), G], true));
        assert_eq!(
            items(20, t, true, 50).0,
            vec![P(1), G, P(18), P(19), C(20), P(21), G]
        );
        // Beyond the cap only the next page is known.
        assert_eq!(
            items(40, t, true, 50).0,
            vec![P(1), G, P(38), P(39), C(40), P(41), G]
        );
        // The real end closes the list.
        assert_eq!(
            items(400, t, false, 50),
            (vec![P(1), G, P(398), P(399), C(400)], false)
        );
    }

    #[test]
    fn a_count_that_lags_the_rows_never_hides_a_page() {
        // The count says one page; the query found a 51st row.
        assert_eq!(items(1, Total::Rows(50), true, 50).0, vec![C(1), P(2)]);
        // No count at all: the next page is all that is known.
        assert_eq!(
            items(3, Total::Unknown, true, 50),
            (vec![P(1), P(2), C(3), P(4), G], true)
        );
        assert_eq!(
            items(3, Total::Unknown, false, 50),
            (vec![P(1), P(2), C(3)], false)
        );
    }

    #[test]
    fn addresses_keep_the_other_sections_and_drop_page_one() {
        let keys = ["page", "lists", "out"];
        let q = Params::parse("lists=3&utm=x");
        assert_eq!(
            link("/did/x", &q, &keys, "page", 2),
            "/did/x?page=2&lists=3"
        );
        assert_eq!(link("/did/x", &q, &keys, "lists", 1), "/did/x");
        assert_eq!(link("/did/x", &q, &keys, "lists", 4), "/did/x?lists=4");
        // Canonical: no `=1`, no cursor of an earlier version.
        let retired = ["bc", "nc", "oc"];
        assert_eq!(canonical(&q, &keys, &retired), None);
        assert_eq!(
            canonical(&Params::parse("page=1&lists=3"), &keys, &retired).as_deref(),
            Some("lists=3")
        );
        assert_eq!(
            canonical(&Params::parse("bc=abc"), &keys, &retired).as_deref(),
            Some("")
        );
        assert_eq!(
            canonical(&Params::parse("page=2&nc=abc"), &keys, &retired).as_deref(),
            Some("page=2")
        );
    }

    #[test]
    fn page_numbers_are_plain_positive_integers() {
        let n = |s: &str| number(&Params::parse(s), "page", "/did/x").ok();
        assert_eq!(n(""), Some(1));
        assert_eq!(n("page=7"), Some(7));
        for bad in [
            "page=0",
            "page=-1",
            "page=x",
            "page=+2",
            "page=",
            "page=1000001",
        ] {
            assert_eq!(n(bad), None, "{bad}");
        }
    }

    #[test]
    fn the_arrows_stop_at_the_bounds() {
        let q = Params::default();
        let keys = ["page"];
        let p = |current, total, more| {
            Pager::new("s", "x", "/b", &q, &keys, "page", current, total, more, 50)
        };
        let first = p(1, Total::Rows(120), true);
        assert!(first.prev.is_none() && first.next.as_deref() == Some("/b?page=2"));
        assert!(first.shown());
        let last = p(3, Total::Rows(120), false);
        assert!(last.prev.as_deref() == Some("/b?page=2") && last.next.is_none());
        assert_eq!(p(2, Total::Rows(120), true).prev.as_deref(), Some("/b"));
        assert!(!p(1, Total::Rows(50), false).shown());
        // Open-ended: next for as long as rows follow.
        assert!(p(30, Total::MoreThan(1_000), true).next.is_some());
        assert!(p(30, Total::MoreThan(1_000), false).next.is_none());
    }
}
