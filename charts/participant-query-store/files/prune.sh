# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Force working directory to writable /tmp so scribe can write output.log
cd /tmp || exit 1


echo "Initiating pqs pruning"

java -jar /scribe.jar datastore postgres-document prune \
    --prune-target "${PRUNE_TARGET}" \
    --postgres-schema "${PGSCHEMA}" \
    --postgres-host "${PGHOST}" \
    --postgres-database "${PGDATABASE}" \
    --postgres-username "${PGUSER}" \
    --prune-mode Force \

echo "Pruning operation executed"

