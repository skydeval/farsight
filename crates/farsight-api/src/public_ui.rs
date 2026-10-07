//! The query interface the public UI calls in-process (no NSID, no XRPC,
//! outside the stable contract; see `docs/design/web-ui.md`).
//!
//! Most public page sections are built from the handlers behind the stable
//! read queries, called as the API calls them. Two sections have no NSID
//! of their own; their coverage is computed here, by the API's own
//! coverage code from the same kind of snapshot, so the UI adds no rule:
//!
//! - an account's **outgoing blocks**: the X side that `checkBlocks`
//!   reports at response level for `actor = subject` (see
//!   `docs/design/coverage.md`);
//! - the **listblocks on a list**: network scope for `listblock`.

use std::sync::Arc;

use chrono::Utc;
use farsight_core::{Collection, Did};
use farsight_storage::queries;
use serde_json::Value;

use crate::ApiState;
use crate::error::XrpcError;
use crate::handlers::{NO_ACTOR, actor_side_coverage, snapshot, view};

/// `freshness` for the blocks authored by `actor`, as `checkBlocks`
/// reports the actor side with default filtering. It weighs the actor's
/// listblocked lists as well as its direct blocks, so it can be lower than
/// the coverage of direct blocks alone; it is never higher.
pub async fn outgoing_freshness(st: &Arc<ApiState>, actor: &Did) -> Result<Value, XrpcError> {
    let snap = snapshot(st)?;
    let v = view(st, &snap);
    let mut tx = st.read_tx().await?;
    let a = queries::actor(&mut tx, actor).await?;
    let x_id = a.map_or(NO_ACTOR, |a| a.id);
    let rows = queries::check_rows(&mut tx, x_id, &[]).await?;
    let debt = match a {
        Some(a) => farsight_storage::debts::debts_for(&mut *tx, &[a.id])
            .await?
            .get(&a.id)
            .is_some_and(|d| !d.is_empty()),
        None => false,
    };
    let ac = match a {
        Some(a) => Some(queries::actor_coverage(&mut tx, a.id).await?),
        None => None,
    };
    tx.rollback().await?;
    let c = actor_side_coverage(&v, ac.as_ref(), x_id, debt, &rows, false);
    Ok(v.render(&c, Utc::now(), st.source_lag()))
}

/// `freshness` for the listblocks stored on any one list: network scope
/// for `listblock`.
pub fn listblock_freshness(st: &Arc<ApiState>) -> Result<Value, XrpcError> {
    let snap = snapshot(st)?;
    let v = view(st, &snap);
    let c = v.network(Collection::ListBlock);
    Ok(v.render(&c, Utc::now(), st.source_lag()))
}
