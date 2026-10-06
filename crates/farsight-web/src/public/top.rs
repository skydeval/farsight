//! The task that computes the home page's top lists (design §8.6).
//!
//! It runs while the server does. A list is computed only while one of
//! `public_ui.show_top_blockers` / `show_top_blocked` asks for it, and
//! only when the stored one is older than its period. The log the "last
//! 24 hours" lists count from is trimmed either way.

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
/// The longest a list's query may run.
pub const QUERY_LIMIT: Duration = Duration::from_secs(20 * 60);

/// How old a stored list may be before it is computed again. The
/// all-time "most blocked" list reads every stored block, so it is
/// computed once a day; the others cost seconds.
pub fn period(kind: Kind) -> Duration {
    match kind {
        Kind::BlockedAll => Duration::from_secs(24 * 3600),
        Kind::BlockersAll => Duration::from_secs(3600),
        Kind::BlockersDay | Kind::BlockedDay => Duration::from_secs(600),
    }
}

async fn refresh(st: &WebState, kind: Kind) -> farsight_storage::Result<bool> {
    let mut conn = st.api.pool.acquire().await?;
    if let Some(at) = top::computed_at(&mut conn, kind).await? {
        let age = (chrono::Utc::now() - at).to_std().unwrap_or_default();
        if age < period(kind) {
            return Ok(false);
        }
    }
    let started = std::time::Instant::now();
    let mut tx = sqlx::Connection::begin(&mut *conn).await?;
    sqlx::query(&format!(
        "SET LOCAL statement_timeout = {}",
        QUERY_LIMIT.as_millis()
    ))
    .execute(&mut *tx)
    .await?;
    let rows = top::compute(&mut tx, kind, COMPUTED).await?;
    top::save(&mut tx, kind, &rows).await?;
    tx.commit().await?;
    tracing::info!(
        list = kind.key(),
        accounts = rows.len(),
        secs = started.elapsed().as_secs(),
        "top lists: computed"
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
    fn the_list_that_reads_every_block_is_computed_least_often() {
        assert!(period(Kind::BlockedAll) > period(Kind::BlockersAll));
        assert!(period(Kind::BlockersAll) > period(Kind::BlockersDay));
        assert_eq!(period(Kind::BlockersDay), period(Kind::BlockedDay));
        assert!(COMPUTED as usize >= SHOWN * 2);
    }
}
