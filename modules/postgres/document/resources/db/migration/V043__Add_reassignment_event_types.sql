-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- The events of a reassignment update are recorded in __events, alongside the events of a
-- transaction, so __event_type gains a label for each of them.
--
-- Adding an enum value inside a transaction is supported since Postgres 12, as long as the new
-- value is not used in that same transaction. No existing row is rewritten.
alter type __event_type add value if not exists 'unassign';
alter type __event_type add value if not exists 'assign';
