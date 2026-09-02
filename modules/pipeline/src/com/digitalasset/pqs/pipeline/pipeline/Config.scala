// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.pipeline.pipeline
import com.digitalasset.auth
import com.digitalasset.canonical.{ContractFilter, MetadataFilter}
import com.digitalasset.pqs.configuration.filter.PartyFilterParser.PartyFilter
import com.digitalasset.pqs.pipeline.pipeline.ledger.Config as LedgerConfig
import com.digitalasset.transcode.schema.IdentifierFilter
import zio.config.magnolia.describe

case class Config(
    @describe("Ledger API service to use as data source")
    datasource: Config.TransactionApi = Config.TransactionApi.TransactionStream,
    @describe("Ledger config")
    ledger: LedgerConfig,
    filter: Config.Filters = Config.Filters(),
    oauth: auth.Config.OAuth
)
object Config:
  sealed trait TransactionApi
  object TransactionApi:
    case object TransactionStream     extends TransactionApi
    case object TransactionTreeStream extends TransactionApi
  end TransactionApi

  case class Filters(
      // TODO: Add a link to the public documentation of ContractFilter syntax
      @describe("Filter expression determining Daml party identifiers to filter on")
      parties: PartyFilter = PartyFilter.All,
      @describe("Filter expression determining which templates and interfaces to include")
      contracts: ContractFilter = ContractFilter(IdentifierFilter.AcceptAll),
      @describe("Filter expression determining which templates and interfaces to capture metadata for")
      metadata: MetadataFilter = MetadataFilter(IdentifierFilter.RejectAll)
  )

end Config
