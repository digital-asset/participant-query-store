-- Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Transition: drop ALL functions, procedures, and views so R__ repeatable
-- migrations own them going forward.
-- After this migration, functions/procedures/views are NEVER redefined in V__ files again.

DROP VIEW IF EXISTS transactions;
DROP VIEW IF EXISTS __archives;

DROP FUNCTION IF EXISTS creates(text, bigint, bigint);
DROP FUNCTION IF EXISTS exercises(text, bigint, bigint);
DROP FUNCTION IF EXISTS active(text, bigint);
DROP FUNCTION IF EXISTS archives(text, bigint, bigint);
DROP FUNCTION IF EXISTS summary_active(bigint);
DROP FUNCTION IF EXISTS summary_creates(bigint, bigint);
DROP FUNCTION IF EXISTS summary_archives(bigint, bigint);
DROP FUNCTION IF EXISTS summary_transients(bigint, bigint);
DROP FUNCTION IF EXISTS summary_exercises(bigint, bigint);
DROP FUNCTION IF EXISTS summary_updates(bigint, bigint);
DROP FUNCTION IF EXISTS stakeholders(contract);
DROP FUNCTION IF EXISTS stakeholders(exercise);
DROP FUNCTION IF EXISTS lookup_contract(text, text);
DROP FUNCTION IF EXISTS lookup_exercises(text, text);
DROP FUNCTION IF EXISTS __insert_archive_fn();
DROP PROCEDURE IF EXISTS create_index_for_contract(text, text, text, text, text);

DROP FUNCTION IF EXISTS oldest_checkpoint();
DROP FUNCTION IF EXISTS latest_checkpoint();
DROP FUNCTION IF EXISTS set_oldest(bigint);
DROP FUNCTION IF EXISTS set_latest(bigint);
DROP FUNCTION IF EXISTS oldest_offset();
DROP FUNCTION IF EXISTS latest_offset();
DROP FUNCTION IF EXISTS validate_offset_exists(bigint);
DROP FUNCTION IF EXISTS nearest_offset(timestamptz);
DROP FUNCTION IF EXISTS nearest_offset(interval);
DROP FUNCTION IF EXISTS validate_pruning_offset(bigint);
DROP FUNCTION IF EXISTS validate_reset_offset(bigint);
DROP FUNCTION IF EXISTS reset_to_offset(bigint);
DROP FUNCTION IF EXISTS prune_to_offset(bigint);
DROP FUNCTION IF EXISTS __validate_max_pruned_offset(bigint);
DROP FUNCTION IF EXISTS prune_archived_to_offset(bigint);
DROP FUNCTION IF EXISTS prune_archived_to_offset_dry_run(bigint);
DROP FUNCTION IF EXISTS redact_contract(text, text);
DROP FUNCTION IF EXISTS redact_exercise(event_id, text);
DROP PROCEDURE IF EXISTS __validate_redaction_contract(text);
DROP PROCEDURE IF EXISTS __validate_redaction_exercise(event_id);
DROP TRIGGER IF EXISTS __update_watermark_trg ON __watermark;
DROP TRIGGER IF EXISTS __insert_watermark_trg ON __watermark;
DROP FUNCTION IF EXISTS __update_watermark_fn();
DROP PROCEDURE IF EXISTS __cleanup_transactions_after_watermark();
DROP PROCEDURE IF EXISTS __delete_transactions_before(bigint);
DROP PROCEDURE IF EXISTS __delete_transactions_after(bigint);
DROP PROCEDURE IF EXISTS __ensure_writer_valid();

DROP FUNCTION IF EXISTS __contracts(text);
DROP FUNCTION IF EXISTS __exercises(text);

DROP PROCEDURE IF EXISTS __initialize_contract_tpe(text, text, text, __payload_type);
DROP PROCEDURE IF EXISTS __initialize_exercise_tpe(text, text, text, text, bool);
DROP PROCEDURE IF EXISTS __initialize_contract_implements(text, text);
DROP PROCEDURE IF EXISTS __initialize_package(text, text, text);
DROP FUNCTION IF EXISTS __make_aliases(text, text, text);
DROP FUNCTION IF EXISTS __make_aliases(text, text, text, text);

DROP FUNCTION IF EXISTS __contract_tpe4name(text);
DROP FUNCTION IF EXISTS __exercise_tpe4name(text);
DROP FUNCTION IF EXISTS __contract_tpe_from_exercise_name(text);
DROP FUNCTION IF EXISTS __nearest_ix(bigint);
DROP FUNCTION IF EXISTS __current_writer();
