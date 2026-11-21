//! Accounts that withheld their own public pages (see
//! `docs/design/web-ui.md`): `public_optouts`, one row per DID in
//! canonical form and nothing else.
//!
//! The table is written only by `POST /account/set`, after the account's
//! holder has signed in, and read by the public pages, the profile cards
//! and the admin dashboard's count. It is keyed by DID rather than
//! `actors.id`: an account may opt out before this instance has interned
//! it, and a page view never interns.
//!
//! All functions take a connection so the caller can run them inside the
//! transaction or on the connection its other reads use.

use std::collections::HashSet;

use farsight_core::Did;
use sqlx::PgConnection;

use crate::error::Result;

/// Whether `did` has opted out: one primary-key read.
pub async fn opted_out(conn: &mut PgConnection, did: &Did) -> Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM public_optouts WHERE did = $1)")
            .bind(did.as_str())
            .fetch_one(conn)
            .await?,
    )
}

/// Which of `dids` have opted out.
pub async fn among(conn: &mut PgConnection, dids: &[String]) -> Result<HashSet<String>> {
    if dids.is_empty() {
        return Ok(HashSet::new());
    }
    let rows: Vec<String> =
        sqlx::query_scalar("SELECT did FROM public_optouts WHERE did = ANY($1)")
            .bind(dids)
            .fetch_all(conn)
            .await?;
    Ok(rows.into_iter().collect())
}

/// Records the account's choice: `hidden` opts out (a repeat changes
/// nothing), otherwise its row is removed.
pub async fn set(conn: &mut PgConnection, did: &Did, hidden: bool) -> Result<()> {
    let q = if hidden {
        "INSERT INTO public_optouts (did) VALUES ($1) ON CONFLICT DO NOTHING"
    } else {
        "DELETE FROM public_optouts WHERE did = $1"
    };
    sqlx::query(q).bind(did.as_str()).execute(conn).await?;
    Ok(())
}

/// How many accounts have opted out (the admin dashboard).
pub async fn count(conn: &mut PgConnection) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT count(*) FROM public_optouts")
        .fetch_one(conn)
        .await?)
}
