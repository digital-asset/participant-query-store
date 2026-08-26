-- Copyright (c) 2025, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create view transactions as
select t.ix,
       t."offset",
       t.transaction_id,
       t.effective_at,
       t.workflow_id,
       t.trace_context,
       t.external_transaction_hash
from __transactions t
where t."offset" between oldest_offset() and latest_offset();
