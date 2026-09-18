// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canonical

import com.digitalasset.transcode.schema.*
import zio.Chunk

import java.time.Instant

final case class Contract(
    representativePackageId: PackageId,
    templateQualifiedName: String,
    contractId: ContractId,
    contractKey: Option[DynamicValue],
    contractKeyHash: Option[Array[Byte]],
    payloads: Chunk[(Identifier, DynamicValue)],
    signatories: Chunk[Party],
    observers: Chunk[Party],
    witnesses: Chunk[Party],
    created_at: Option[Instant],
    metadata: Option[Array[Byte]],
    acsDelta: Boolean,
    creationPackageId: Option[String]
)
