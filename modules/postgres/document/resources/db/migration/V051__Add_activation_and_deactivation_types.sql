-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create type activation as
(
    template_fqn             text,
    payload_type             __payload_type,
    contract_id              text,
    payload                  jsonb,
    contract_key             jsonb,
    metadata                 bytea,
    redaction_id             text,
    package_name             text,
    package_version          text,
    package_id               text,
    signatories              text[],
    observers                text[],
    witnesses                text[],
    creation_package_id      text,
    contract_key_hash        bytea,
    activate_event_pk        bigint,
    activate_event_id        event_id,
    activated_at_ix          bigint,
    activated_at_offset      bigint,
    created_effective_at     timestamp with time zone,
    activation_type          activation_type,
    deactivate_event_pk      bigint,
    deactivate_event_id      event_id,
    deactivated_at_ix        bigint,
    deactivated_at_offset    bigint,
    archived_effective_at    timestamp with time zone,
    deactivation_type        deactivation_type,
    reassignment_counter     bigint,
    source_synchronizer_id   text,
    synchronizer_id          text
);
comment on type activation is 'Row returned by the activations SQL function.';

create type deactivation as
(
    template_fqn             text,
    payload_type             __payload_type,
    contract_id              text,
    payload                  jsonb,
    contract_key             jsonb,
    metadata                 bytea,
    redaction_id             text,
    package_name             text,
    package_version          text,
    package_id               text,
    signatories              text[],
    observers                text[],
    creation_package_id      text,
    contract_key_hash        bytea,
    activate_event_pk        bigint,
    activate_event_id        event_id,
    activated_at_ix          bigint,
    activated_at_offset      bigint,
    created_effective_at     timestamp with time zone,
    activation_type          activation_type,
    deactivate_event_pk      bigint,
    deactivate_event_id      event_id,
    deactivated_at_ix        bigint,
    deactivated_at_offset    bigint,
    archived_effective_at    timestamp with time zone,
    deactivation_type        deactivation_type,
    reassignment_counter     bigint,
    synchronizer_id          text,
    target_synchronizer_id   text
);
comment on type deactivation is 'Row returned by the deactivations SQL function.';
