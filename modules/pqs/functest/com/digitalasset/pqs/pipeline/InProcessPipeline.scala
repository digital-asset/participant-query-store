// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.pipeline

import com.digitalasset.pqs.backend.Datastore
import com.digitalasset.canonical.specific.*
import com.digitalasset.pqs.postgres.backend.SchemaConfig
import com.digitalasset.pqs.postgres.document.DocumentPostgres
import com.digitalasset.pqs.postgres.document.SqlSchema
import com.digitalasset.pqs.services.postgres.*
import com.digitalasset.zio.daml.{DamlSchema, JsonCodecs}
import zio.*
import zio.stream.ZStream

/** This helper enables running the pipeline in-process.
  *
  * This is contrary to the standard way in works functest - where pqs is run in docker.
  */
object InProcessPipeline:
  def destinationLayer(
      schemaConfig: SchemaConfig = SchemaConfig()
  ): ZLayer[Database & DamlSchema & SqlSchema & JsonCodecs, Throwable, Datastore] =
    ZLayer.succeed(schemaConfig) ++ Database.config ++ Database.connectionPool ++ Database.instanceId
      >>> DocumentPostgres.live

  def processTransactions(
      transactions: Chunk[Transaction[Event | TreeEvent | ReassignmentEvent]]
  ): ZIO[Datastore, Throwable, Unit] =
    for
      datastore    <- ZIO.service[Datastore]
      (offset, ix) <- datastore.getLastCheckpoint
      _ <- ZStream
        .from(transactions)
        .mapAccum(ix + 1)((index, a) => (index + 1, (a, index)))
        .run(datastore.processTransactions)
    yield ()
