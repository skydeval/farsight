#!/usr/bin/env sh
# Wipes what the stage-1 harness leaves behind so it can run repeatedly.
#
#   scripts/stage1-harness-teardown.sh              remove harness containers + volumes
#   scripts/stage1-harness-teardown.sh DATABASE_URL also drop the harness databases
#                                                   on that server (--database-url runs)
#
# The harness normally cleans up after itself; this is for --keep runs and
# interrupted ones. It only touches containers labelled
# farsight.stage1-harness=1, volumes named farsight-stage1-*, and databases
# named farsight_stage1_s*.
set -eu

ids=$(docker ps -aq --filter label=farsight.stage1-harness=1)
if [ -n "$ids" ]; then
  # shellcheck disable=SC2086
  docker rm -f -v $ids
fi
vols=$(docker volume ls -q --filter name=farsight-stage1-)
if [ -n "$vols" ]; then
  # shellcheck disable=SC2086
  docker volume rm -f $vols
fi

if [ "${1:-}" != "" ]; then
  for db in $(psql "$1" -Atc "SELECT datname FROM pg_database WHERE datname LIKE 'farsight\_stage1\_s%'"); do
    psql "$1" -c "DROP DATABASE IF EXISTS \"$db\" WITH (FORCE)"
  done
fi
echo "stage-1 harness state removed"
