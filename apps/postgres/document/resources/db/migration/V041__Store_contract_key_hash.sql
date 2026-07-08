-- Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

alter table __contracts add column contract_key_hash bytea;

alter type contract add attribute contract_key_hash bytea;
