# Farsight image: one image, two binaries.
FROM rust:1.88-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock rustfmt.toml ./
COPY crates crates
COPY lexicons lexicons
RUN cargo build --release --locked --bin farsight --bin farsight-backfill

FROM debian:bookworm-slim
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
