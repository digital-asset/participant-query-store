-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- TODO #79: the column is populated for every update but stays private to
-- __transactions; exposing it via the public views and SQL functions is tracked separately.
alter table __transactions rename column domain_id to synchronizer_id;
