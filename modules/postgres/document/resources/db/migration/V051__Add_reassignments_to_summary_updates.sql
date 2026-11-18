-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- We change the return type of this function, so we need to drop it first and then let R__functions.sql recreate it.
drop function if exists summary_updates(bigint, bigint);
