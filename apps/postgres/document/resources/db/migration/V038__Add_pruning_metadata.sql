-- Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

-- Pruning metadata is stored in a separate table to avoid lock contention with the
-- pipeline's __watermark updates. Pruning locks __pruning_metadata (SHARE UPDATE EXCLUSIVE)
-- while the pipeline locks __watermark (ROW EXCLUSIVE via UPDATE). Since they are different
-- tables, no row-level contention occurs.
CREATE TABLE IF NOT EXISTS __pruning_metadata (
    singleton      bool   NOT NULL PRIMARY KEY DEFAULT true,
    pruned_offset  bigint,  -- the max offset that has been pruned
    CONSTRAINT singleton_pruning CHECK (singleton)
);

INSERT INTO __pruning_metadata (singleton) VALUES (true);
