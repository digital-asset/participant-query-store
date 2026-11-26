-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

alter type contract
    add attribute activated_at_record_time timestamp with time zone,
    add attribute deactivated_at_record_time timestamp with time zone;

alter type activation add attribute activated_at_record_time timestamp with time zone;

alter type deactivation add attribute deactivated_at_record_time timestamp with time zone;

alter type exercise add attribute exercised_at_record_time timestamp with time zone;
