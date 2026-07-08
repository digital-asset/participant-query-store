-- Copyright (c) 2025, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create function __contract_tpe_from_exercise_name(qname text) returns __contract_tpe.pk%type as
$$
select __contract_tpe4name(template_fqn)
from __exercise_tpe
where pk = __exercise_tpe4name(qname);
$$ language sql immutable
                parallel safe
                strict;
