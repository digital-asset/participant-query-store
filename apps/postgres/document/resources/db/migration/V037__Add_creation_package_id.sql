-- Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE __contracts ADD COLUMN creation_package_id text NULL;

-- we use coalesce to ensure that creation_package_id is not null
ALTER TYPE contract ADD ATTRIBUTE creation_package_id text;
