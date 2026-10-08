# Farsight image: one image, two binaries.
#
# Both base images are named by digest, so a rebuild starts from the same
# bytes until this file changes. The tag before each digest says which
# image it is; to move to a newer one, read its digest with
# `docker buildx imagetools inspect <tag>` and replace it here.
FROM rust:1.88-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0 AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock rustfmt.toml ./
COPY crates crates
COPY lexicons lexicons
RUN cargo build --release --locked --bin farsight --bin farsight-backfill

FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates wget \
 && rm -rf /var/lib/apt/lists/*
# The config volume must be writable by the service user: a new named
# volume copies this directory's ownership.
RUN groupadd --system --gid 10001 farsight \
 && useradd --system --uid 10001 --gid 10001 --no-create-home farsight \
 && mkdir -p /etc/farsight \
 && chown 10001:10001 /etc/farsight \
 && chmod 0700 /etc/farsight
COPY --from=build /src/target/release/farsight /src/target/release/farsight-backfill /usr/local/bin/
USER 10001:10001
EXPOSE 8080
CMD ["farsight"]
