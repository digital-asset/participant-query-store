// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.backend

opaque type InstanceId <: String = String

object InstanceId:
  inline def apply(instanceId: String): InstanceId = instanceId
