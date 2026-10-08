# Setting up

The short version is in the [README](../../README.md#quick-start). This
page has the rest.

## The setup token

The setup token is printed in the server log at start-up
(`docker compose logs farsight`) and again every 10 minutes, and
`docker compose exec farsight farsight setup-token` prints it on demand
(`--rotate` replaces it). Both commands go by the service name in
`compose.yml`, so they work whatever the container is called; run them
in the directory that holds `compose.yml`. Replacing the token also ends every wizard
session that was opened with the old one, so that is what to do if the
token may have been seen by someone else. A wizard session lasts 12
hours at most; enter the token again to go on.

To keep the wizard off the network until it is
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
  support). The default is Bluesky's two public v2 instances,
  `wss://jetstream.us-east.bsky.network` and
  `wss://jetstream.us-west.bsky.network`, one per line: the second
  takes over if the first fails. On a v1
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

The Jetstream and Postgres tests connect to whatever address you
enter, private addresses included, since both may well be on your own
network. Each attempt is limited to 10 seconds, and both can be run
only by someone who has entered the setup token. That is one more
reason to keep the wizard off the network, as above, until it is
done.

It then writes `/etc/farsight/config.toml` and switches Farsight to
normal mode in-process: it runs migrations, connects to the firehose and
starts serving the API. A config reset (Settings → Reset) returns it to
the wizard; the database is kept.

Set `POSTGRES_PASSWORD` before the first start, in a file named `.env`
beside `compose.yml` (`POSTGRES_PASSWORD=…` on a line of its own).
Compose reads that file on every command, so a later
`docker compose up -d` from another shell uses the same password. A
password that is only exported in one shell is missing in the next.
The compose file has no default for it: without a password every
`docker compose` command stops with an error that names
`POSTGRES_PASSWORD`, and nothing is started. Use letters and digits
only: the password is placed in a connection URL, where `/`, `#`, `?`
and `%` mean something else. The compose file passes the matching
connection string to Farsight, and the wizard's storage step is
prefilled with it. The storage step shows the string with its password
redacted. Left as shown, or edited only in its database name, it keeps
the password; a string that names another host, port, user or
parameter must carry its own. The bundled Postgres is not published
outside the compose network. A password of `farsight` for the user
`farsight` is accepted, and Farsight logs a warning at every start for
as long as it connects with that. To change the password later, change it in Postgres first
(`ALTER ROLE farsight PASSWORD '…'`), then change `POSTGRES_PASSWORD`
in `.env` and restart.

## The image

The compose file names the image by version
(`ghcr.io/skydeval/farsight:0.6.2`), not `latest`. `docker compose
pull` fetches the published image; when the image is not on the
machine, `docker compose up -d` builds it from the checkout. A checkout
and its compose file therefore always run the version they describe;
to move to another version, check it out and run `docker compose pull`
and `docker compose up -d`, or `docker compose up -d --build` to build
it from source. The
Dockerfile names its two base images by digest, so a rebuild starts
from the same images until the Dockerfile changes.

## The backfill container

The `farsight-backfill` container starts with the others. It idles
until the wizard has written the config and the server has migrated the
database, then works through on-demand requests, recently active
accounts, list fetches and (if enabled in the wizard) the systematic
sweep. It talks to PDS hosts at a polite rate per host and per domain,
to the PLC directory and to the relay set in `backfill.relay_url`;
progress, ETA and queue depths are on the dashboard and on its metrics
port (9465, not published).

Both containers can be stopped and updated at any time. They take up
to 40 seconds to stop, and the compose file gives them 45
(`stop_grace_period`). Work that was under way goes back to the queue
and is taken up after the start, also when a container was killed.

Outside the compose file, the metrics listeners are on loopback
(`127.0.0.1:9464` and `127.0.0.1:9465`). To scrape them from another
host, set `metrics.bind` and `metrics.backfill_bind`, and set
`metrics.bearer_token_sha256` with them.

## Unattended deployment

For automated deployments, set `FARSIGHT_SKIP_WIZARD=1` and supply the
config as a file, or entirely through environment variables. Nested
config keys use one double underscore per level, for example
`FARSIGHT__BACKFILL__SWEEP__ENABLED=true`.

## Health checks

`/health` returns 200 when the database answers within a second, the
firehose is connected and what it has applied is at most
`firehose.tuning.synthetic_gap_lag` (5 minutes) behind (compose uses
it as a status check); `/livez`
returns 200 while the process serves HTTP and is the right probe for
orchestrators that restart unhealthy containers.
