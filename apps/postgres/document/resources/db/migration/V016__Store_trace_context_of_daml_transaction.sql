-- Copyright (c) 2024, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create type trace_context as
(
    trace_parent text,
    trace_state  text
);
comment on type trace_context is 'Propagated trace context for the associated Daml transaction (as defined by https://www.w3.org/TR/trace-context/)';

alter table __transactions
    add column trace_context trace_context;
