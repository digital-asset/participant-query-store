# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
function log() {
( set -euo pipefail
    echo "$(date) | " "$@" )
}
: "${PRUNING_INTERVAL_SECONDS:?PRUNING_INTERVAL_SECONDS environment variable must be set}"
: "${PGHOST:?PGHOST environment variable must be set}"
: "${PGDATABASE:?PGDATABASE environment variable must be set}"
: "${PGUSER:?PGUSER environment variable must be set}"
: "${PGSCHEMA:?PGSCHEMA environment variable must be set}"
: "${PQS_MAX_AGE_DAYS:?PQS_MAX_AGE_DAYS environment variable must be set}"
readonly PRUNING_INTERVAL_SECONDS PGHOST PGDATABASE PGUSER PGSCHEMA PQS_MAX_AGE_DAYS
declare QUERY

QUERY="SELECT * FROM prune_archived_to_offset(nearest_offset(interval '${PQS_MAX_AGE_DAYS} days'));"
readonly QUERY

export PGOPTIONS="--search_path=${PGSCHEMA}"

echo
echo 'starting PQS pruning service'
echo "plan: prune every ${PRUNING_INTERVAL_SECONDS} seconds, deleting all data older than ${PQS_MAX_AGE_DAYS} days"
echo "query: ${QUERY}"
echo "host: ${PGHOST}"
echo "database: ${PGDATABASE}"
echo "user: ${PGUSER}"
echo "schema: ${PGSCHEMA}"
echo "pruning interval: ${PRUNING_INTERVAL_SECONDS}"
echo

while :; do
    log 'pruning PQS'
    until timeout 600 psql -tc "${QUERY}"; do
    log 'prune failed, retrying in 5 minutes'
    sleep 300
    done
    echo
    log 'prune complete'

    sleep "${PRUNING_INTERVAL_SECONDS}"
done