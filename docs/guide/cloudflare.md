# Running behind Cloudflare

1. Create the DNS record as **proxied**.
2. Set SSL/TLS to **Full (strict)**, or use Cloudflare Tunnel.
3. Add cache rules:
   - Make `/xrpc/app.nearhorizon.farsight.query.*` eligible for cache
     and respect origin headers, except
     `query.getBackfillStatus`.
   - If the public UI is on, everything else may be cached per the
     origin's headers: the public pages are at the root.
   - Keep the query string in the cache key (Cloudflare's default).
     Pages ask for the stylesheets and scripts as
     `/static/<file>?v=<fingerprint of this build>`, so that a new
     version's files are fetched at once and not after the hour they
     may be cached. A cache that ignores the query string would keep
     serving the old files for that hour.
   - Bypass the cache for `/admin*`, `/enter*` (the sign-in and its
     callback), `/setup*`, `/xrpc/app.nearhorizon.farsight.admin.*`,
     `/health` and `/livez`. That list is complete: no admin page lives
     outside `/admin*`.
4. Lock the origin with Cloudflare Tunnel or Authenticated Origin
   Pulls. Firewalling the origin to Cloudflare's IP ranges alone is not
   enough.
5. In the wizard's reverse-proxy step, choose **Cloudflare**. Farsight
   then trusts `CF-Connecting-IP` only from Cloudflare addresses.
