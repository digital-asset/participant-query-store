-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

alter table __exercises add column synchronizer_id text;

alter type exercise
    add attribute exercised_at_record_time timestamp with time zone,
    add attribute synchronizer_id text;
