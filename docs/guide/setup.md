# Setting up

The short version is in the [README](../../README.md#quick-start). This
page has the rest.

## The setup token

The setup token is re-printed every 10 minutes, and
`docker exec farsight farsight setup-token` prints it on demand
(`--rotate` replaces it). To keep the wizard off the network until it is
done, publish the port on loopback only and use an SSH tunnel:

```sh
FARSIGHT_PORT=127.0.0.1:8080 docker compose up -d
ssh -L 8080:127.0.0.1:8080 your-server   # then open http://127.0.0.1:8080
```

## The wizard

The wizard asks for:

- the setup token;
- the public hostname and an admin contact;
- the Jetstream source (it tests the connection and checks for v2
  support). The default is Bluesky's public v2 instance
  `wss://jetstream.us-east.bsky.network`;
  `wss://jetstream.us-west.bsky.network` is the other. On a v1
  instance (Bluesky's older `jetstream1` and `jetstream2`) coverage
  stays `partial`;
- backfill preferences and the disk space available to Postgres;
- who may read the API, and which web interfaces to serve. Both are
  off unless ticked: the **public UI** (the wizard shows what it makes
  public and asks you to confirm) and the **admin UI**, with the DID of
  the ATProto account that will administer the instance. With neither,
  the instance is API-only. The step shows the admin token once;
- reverse-proxy trust (it has a preset for Cloudflare);
- the Postgres connection string.

It then writes `/etc/farsight/config.toml` and switches Farsight to
normal mode in-process: it runs migrations, connects to the firehose and
starts serving the API. A config reset (Settings → Reset) returns it to
the wizard; the database is kept.

Set `POSTGRES_PASSWORD` in the environment before the first start; the
compose file passes the matching connection string to Farsight, and the
wizard's storage step is prefilled with it.

## The backfill container

The `farsight-backfill` container starts with the others. It idles
until the wizard has written the config and the server has migrated the
database, then works through on-demand requests, recently active
accounts, list fetches and (if enabled in the wizard) the systematic
sweep. It talks to PDS hosts at a polite per-host rate, to the PLC
directory and to the relay set in `backfill.relay_url`; progress, ETA
and queue depths are on the dashboard and on its metrics port (9465,
not published).

## Unattended deployment

For automated deployments, set `FARSIGHT_SKIP_WIZARD=1` and supply the
config as a file, or entirely through environment variables. Nested
config keys use one double underscore per level, for example
`FARSIGHT__BACKFILL__SWEEP__ENABLED=true`.

## Health checks

`/health` returns 200 when the firehose is connected and the database
answers within a second (compose uses it as a status check); `/livez`
returns 200 while the process serves HTTP and is the right probe for
orchestrators that restart unhealthy containers.
