-- Copyright (c) 2024, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create index if not exists __events_parent_event_pk_idx on __events using btree (parent_event_pk);
