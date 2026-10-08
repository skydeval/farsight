# Running behind Cloudflare

Farsight serves plain HTTP on one port (8080) and does not terminate
TLS. Something else has to: a reverse proxy of your choice with a
certificate, or Cloudflare Tunnel, which needs neither a certificate
nor an open port. Keep Farsight's own port off the public internet,
for example with `FARSIGHT_PORT=127.0.0.1:8080` in `.env`.

1. Create the DNS record as **proxied**.
2. Set SSL/TLS to **Full (strict)** with a reverse proxy in front of
   Farsight that holds a certificate Cloudflare accepts, or use
   Cloudflare Tunnel. Do not use Flexible: it sends the admin sign-in
   from Cloudflare to the server unencrypted.
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

   With Cloudflare Tunnel, or a reverse proxy of your own between
   Cloudflare and Farsight, the connection to Farsight comes from
   `cloudflared` or that proxy, not from a Cloudflare address, so the
   ranges the preset trusts match nothing. The wizard's choice for
   this is **Cloudflare Tunnel or a local reverse proxy**, which asks
   for the address it connects from; its preview shows the address
   Farsight sees. Afterwards the same is done in Settings: keep
   `proxy.mode = "cloudflare"` and add that address to `proxy.trusted`
   (the container's address, or `127.0.0.1/32` when it runs on the
   same host). Nothing but `cloudflared` or the proxy may then be able
   to reach Farsight's port: whoever can, is believed about the client
   address. A proxy of your own must itself accept connections from
   Cloudflare only, since it passes `CF-Connecting-IP` on.
