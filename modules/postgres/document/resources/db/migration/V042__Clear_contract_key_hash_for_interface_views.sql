-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Interface view rows used to store the contract key hash of the underlying template, even though
-- contract_key itself was left empty. The hash describes the template, not the interface view, so
-- clear it from history to match what the pipeline now writes.
--
-- `contract_key is null` is redundant with the payload_type filter, but it makes the intent clearer.
-- `contract_key_hash is not null` keeps already-clean rows from being rewritten for nothing.
update __contracts c
set contract_key_hash = null
where c.tpe_pk in (select tpe.pk from __contract_tpe tpe where tpe.payload_type = 'interface')
  and c.contract_key is null
  and c.contract_key_hash is not null;
