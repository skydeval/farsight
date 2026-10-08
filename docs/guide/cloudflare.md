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
   Trusting the proxy is also what tells Farsight that visitors arrive
   over HTTPS: only then are its cookies `Secure`, and the admin
   session cookie `__Host-farsight_admin`. Behind a proxy it does not
   trust, they are set without `Secure`.

   With Cloudflare Tunnel the connection to Farsight comes from
   `cloudflared`, not from a Cloudflare address, so the ranges the
   preset trusts match nothing. Keep `proxy.mode = "cloudflare"` and
   add the address `cloudflared` connects from to `proxy.trusted` in
   Settings (its container's address, or `127.0.0.1/32` when it runs
   on the same host). Nothing but `cloudflared` may then be able to
   reach Farsight's port: whoever can, is believed about the client
   address.
