//! API keys (`api_tokens`) and admin UI sessions (`admin_sessions`)
//! (design §3.5, §8.6). Secrets are stored only as SHA-256 hashes; the
//! API compares hashes in constant time.

use chrono::{DateTime, Utc};
use sqlx::{PgExecutor, PgPool};

use crate::error::Result;

/// One API key.
#[derive(Debug, Clone, PartialEq)]
pub struct ApiToken {
    /// `api_tokens.id`.
    pub id: i32,
    /// Operator-chosen name.
    pub name: String,
    /// SHA-256 of the token.
    pub sha256: Vec<u8>,
    /// Scopes: `read`, `backfill`, `backfill:high`.
    pub scopes: Vec<String>,
    /// Per-key read rate override (requests/s).
    pub read_rps: Option<f32>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last authenticated use (approximate).
    pub last_used_at: Option<DateTime<Utc>>,
    /// Revocation time.
    pub revoked_at: Option<DateTime<Utc>>,
}

type TokenRow = (
    i32,
    String,
    Vec<u8>,
    Vec<String>,
    Option<f32>,
    DateTime<Utc>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
);

fn token(r: TokenRow) -> ApiToken {
    ApiToken {
        id: r.0,
        name: r.1,
        sha256: r.2,
        scopes: r.3,
        read_rps: r.4,
        created_at: r.5,
        last_used_at: r.6,
        revoked_at: r.7,
    }
}

const TOKEN_COLUMNS: &str =
    "id, name, sha256, scopes, read_rps, created_at, last_used_at, revoked_at";

/// Every key, newest first (operations page).
pub async fn list_tokens<'e>(ex: impl PgExecutor<'e>) -> Result<Vec<ApiToken>> {
    let rows: Vec<TokenRow> = sqlx::query_as(&format!(
        "SELECT {TOKEN_COLUMNS} FROM api_tokens ORDER BY id DESC"
    ))
    .fetch_all(ex)
    .await?;
    Ok(rows.into_iter().map(token).collect())
}

/// Keys not revoked (the API's in-memory key table).
pub async fn active_tokens<'e>(ex: impl PgExecutor<'e>) -> Result<Vec<ApiToken>> {
    let rows: Vec<TokenRow> = sqlx::query_as(&format!(
        "SELECT {TOKEN_COLUMNS} FROM api_tokens WHERE revoked_at IS NULL"
    ))
    .fetch_all(ex)
    .await?;
    Ok(rows.into_iter().map(token).collect())
}

/// Stores a new key; returns its id.
pub async fn create_token<'e>(
    ex: impl PgExecutor<'e>,
    name: &str,
    sha256: &[u8],
    scopes: &[String],
    read_rps: Option<f32>,
) -> Result<i32> {
    Ok(sqlx::query_scalar(
        "INSERT INTO api_tokens (name, sha256, scopes, read_rps) VALUES ($1, $2, $3, $4)
         RETURNING id",
    )
    .bind(name)
    .bind(sha256)
    .bind(scopes)
    .bind(read_rps)
    .fetch_one(ex)
    .await?)
}

/// Revokes one key. Returns whether a live key was revoked.
pub async fn revoke_token<'e>(ex: impl PgExecutor<'e>, id: i32) -> Result<bool> {
    Ok(
        sqlx::query(
            "UPDATE api_tokens SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL",
        )
        .bind(id)
        .execute(ex)
        .await?
        .rows_affected()
            > 0,
    )
}

/// Revokes every key (config reset, §8.6).
pub async fn revoke_all_tokens<'e>(ex: impl PgExecutor<'e>) -> Result<u64> {
    Ok(
        sqlx::query("UPDATE api_tokens SET revoked_at = now() WHERE revoked_at IS NULL")
            .execute(ex)
            .await?
            .rows_affected(),
    )
}

/// Records last use of the given keys (batched by the API).
pub async fn touch_tokens<'e>(ex: impl PgExecutor<'e>, ids: &[i32]) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query("UPDATE api_tokens SET last_used_at = now() WHERE id = ANY($1)")
        .bind(ids)
        .execute(ex)
        .await?;
    Ok(())
}

/// One admin session row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminSession {
    /// SHA-256 of the session id (the cookie value).
    pub id_sha256: Vec<u8>,
    /// Per-session CSRF token.
    pub csrf: Vec<u8>,
    /// Login time (absolute expiry base).
    pub created_at: DateTime<Utc>,
    /// Last request (idle expiry base).
    pub last_seen: DateTime<Utc>,
}

/// Creates a session.
pub async fn create_session(
    pool: &PgPool,
    id_sha256: &[u8],
    csrf: &[u8],
    ip: Option<std::net::IpAddr>,
    user_agent: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO admin_sessions (id_sha256, csrf, ip, user_agent)
         VALUES ($1, $2, $3::inet, $4)",
    )
    .bind(id_sha256)
    .bind(csrf)
    .bind(ip.map(|i| i.to_string()))
    .bind(user_agent)
    .execute(pool)
    .await?;
    Ok(())
}

/// The session with this id hash if it has not expired (`idle` since last
/// request, `absolute` since login); refreshes `last_seen`. Expired rows
/// are deleted on sight.
pub async fn touch_session(
    pool: &PgPool,
    id_sha256: &[u8],
    idle: std::time::Duration,
    absolute: std::time::Duration,
) -> Result<Option<AdminSession>> {
    type Row = (Vec<u8>, Vec<u8>, DateTime<Utc>, DateTime<Utc>, bool);
    let row: Option<Row> = sqlx::query_as(
        "SELECT id_sha256, csrf, created_at, last_seen,
                (last_seen < now() - make_interval(secs => $2)
                 OR created_at < now() - make_interval(secs => $3))
         FROM admin_sessions WHERE id_sha256 = $1",
    )
    .bind(id_sha256)
    .bind(idle.as_secs_f64())
    .bind(absolute.as_secs_f64())
    .fetch_optional(pool)
    .await?;
    let Some((id, csrf, created_at, last_seen, expired)) = row else {
        return Ok(None);
    };
    if expired {
        delete_session(pool, &id).await?;
        return Ok(None);
    }
    sqlx::query("UPDATE admin_sessions SET last_seen = now() WHERE id_sha256 = $1")
        .bind(&id)
        .execute(pool)
        .await?;
    Ok(Some(AdminSession {
        id_sha256: id,
        csrf,
        created_at,
        last_seen,
    }))
}

/// Ends one session (logout).
pub async fn delete_session<'e>(ex: impl PgExecutor<'e>, id_sha256: &[u8]) -> Result<()> {
    sqlx::query("DELETE FROM admin_sessions WHERE id_sha256 = $1")
        .bind(id_sha256)
        .execute(ex)
        .await?;
    Ok(())
}

/// Revokes every admin session (password change, admin-token rotation,
/// reset; §8.6).
pub async fn delete_all_sessions<'e>(ex: impl PgExecutor<'e>) -> Result<u64> {
    Ok(sqlx::query("DELETE FROM admin_sessions")
        .execute(ex)
        .await?
        .rows_affected())
}

/// Deletes expired sessions (periodic).
pub async fn expire_sessions(
    pool: &PgPool,
    idle: std::time::Duration,
    absolute: std::time::Duration,
) -> Result<u64> {
    Ok(sqlx::query(
        "DELETE FROM admin_sessions
         WHERE last_seen < now() - make_interval(secs => $1)
            OR created_at < now() - make_interval(secs => $2)",
    )
    .bind(idle.as_secs_f64())
    .bind(absolute.as_secs_f64())
    .execute(pool)
    .await?
    .rows_affected())
}

/// Records an operational error (`op_errors`).
pub async fn record_op_error<'e>(
    ex: impl PgExecutor<'e>,
    component: &str,
    did: Option<&str>,
    message: &str,
) -> Result<()> {
    sqlx::query("INSERT INTO op_errors (component, did, message) VALUES ($1, $2, $3)")
        .bind(component)
        .bind(did)
        .bind(message)
        .execute(ex)
        .await?;
    Ok(())
}
