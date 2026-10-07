//! The task that counts the home page's top lists (see
//! `docs/design/web-ui.md`).
//!
//! It runs while the server does. Once a day, at 05:00 EST, the lists
//! that `public_ui.show_top_blockers` / `show_top_blocked` ask for are
//! counted. A switch turned on during the day gets its lists at the
//! task's next look. The log the day's lists count from is trimmed
//! either way.

use std::sync::Arc;
use std::time::Duration;

use farsight_storage::top::{self, Kind};
use tokio::sync::watch;

use crate::pages::WebState;

/// How often the task looks.
pub const EVERY: Duration = Duration::from_secs(60);
/// How many accounts a list is computed with. A page shows [`SHOWN`];
/// the rest stand in for accounts the page hides.
pub const COMPUTED: i64 = 60;
/// How many accounts a list shows.
pub const SHOWN: usize = 20;
/// How many of them before "Show more".
pub const FIRST: usize = 10;
/// The longest a list's query may run.
pub const QUERY_LIMIT: Duration = Duration::from_secs(20 * 60);

async fn refresh(st: &WebState, kind: Kind) -> farsight_storage::Result<bool> {
    let mut conn = st.api.pool.acquire().await?;
    let end = top::day_end(chrono::Utc::now());
    if top::stored_day(&mut conn, kind).await? == Some(end) {
        return Ok(false);
    }
    let started = std::time::Instant::now();
    let mut tx = sqlx::Connection::begin(&mut *conn).await?;
    sqlx::query(&format!(
        "SET LOCAL statement_timeout = {}",
        QUERY_LIMIT.as_millis()
    ))
    .execute(&mut *tx)
    .await?;
    let rows = top::compute(&mut tx, kind, end, COMPUTED).await?;
    top::save(&mut tx, kind, end, &rows).await?;
    tx.commit().await?;
    tracing::info!(
        list = kind.key(),
        accounts = rows.len(),
        secs = started.elapsed().as_secs(),
        "top lists: counted"
    );
    Ok(true)
}

/// The task: one long-running loop, started with the server's
/// background tasks. Ends when `stop` flips.
pub async fn run(st: Arc<WebState>, mut stop: watch::Receiver<bool>) {
    let mut since_trim = Duration::from_secs(3600);
    loop {
        tokio::select! {
            _ = tokio::time::sleep(EVERY) => {}
            r = stop.changed() => if r.is_err() || *stop.borrow() { return },
        }
        if *stop.borrow() {
            return;
        }
        since_trim += EVERY;
        if since_trim >= Duration::from_secs(3600) {
            since_trim = Duration::ZERO;
            match st.api.pool.acquire().await {
                Ok(mut conn) => {
                    if let Err(e) = top::trim(&mut conn).await {
                        tracing::warn!(error = %e, "top lists: the log could not be trimmed");
                    }
                }
                Err(e) => tracing::warn!(error = %e, "top lists: no connection"),
            }
        }
        let cfg = st.api.config.current();
        let p = &cfg.config.public_ui;
        for kind in Kind::ALL {
            let wanted = if kind.blockers() {
                p.show_top_blockers
            } else {
                p.show_top_blocked
            };
            if !wanted {
                continue;
            }
            if let Err(e) = refresh(&st, kind).await {
                tracing::warn!(list = kind.key(), error = %e, "top lists: not computed");
            }
            if *stop.borrow() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_list_is_counted_with_room_for_hidden_accounts() {
        assert!(COMPUTED as usize >= SHOWN * 2);
        assert_eq!(SHOWN, FIRST * 2);
    }
}
