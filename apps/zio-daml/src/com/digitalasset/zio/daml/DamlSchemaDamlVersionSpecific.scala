// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml

import com.digitalasset.transcode.schema.Identifier
import com.digitalasset.zio.daml.DamlSchema.{findMissingInterfaceImplementations, pretty}
import zio.Task
import zio.ZIO.logInfo

import scala.collection.immutable

object DamlSchemaDamlVersionSpecific:
  def interfaceImplementationsIntegrityAction(ids: KnownEntityIdentifiers): Task[Set[Identifier]] =
    val implements       = ids.implements
    val filteredEntities = ids.filtered.entities
    val (
      inconsistentIncluded: immutable.Iterable[Set[Identifier]],
      inconsistentExcluded: immutable.Iterable[Set[Identifier]]
    ) = findMissingInterfaceImplementations(ids)
    logInfo(
      s"Extending filter to match missing entities. Filter selects [${pretty(inconsistentExcluded)}]  which need to be included along with [${pretty(inconsistentIncluded)}]"
    ).map(_ => inconsistentExcluded.flatten.toSet)
