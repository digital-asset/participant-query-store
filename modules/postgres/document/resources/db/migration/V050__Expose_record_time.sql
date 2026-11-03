-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

alter type contract
    add attribute created_record_time timestamp with time zone,
    add attribute archived_record_time timestamp with time zone,
    add attribute assigned_record_time timestamp with time zone,
    add attribute unassigned_record_time timestamp with time zone;

alter type exercise
    add attribute exercised_record_time timestamp with time zone;
