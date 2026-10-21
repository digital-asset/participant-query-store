// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs

import com.digitalasset.pqs.services.daml.DamlSource

private[pqs] object Dars:
  val pingPongTransact = DamlSource(
    "PingPong" ->
      """|module PingPong where
         |
         |import Daml.Script
         |import DA.Functor (void)
         |
         |template Ping
         |  with
         |    owner: Party
         |  where
         |    signatory owner
         |
         |transact : Party -> Script ()
         |transact party = void do
         |  submit party $ createCmd Ping with owner = party
         |""".stripMargin
  )

  val blobIface = DamlSource(
    "BlobIface" ->
      """|module BlobIface where
         |
         |data BlobView = BlobView { value: Int }  
         |
         |interface Blob where viewtype BlobView
         |""".stripMargin
  )

  val seedAndBlobs = DamlSource(
    "Blob" ->
      """|module Blob where
         |
         |import BlobIface
         |
         |import DA.Action
         |import DA.Foldable
         |
         |template Seed
         |  with owner : Party
         |  where
         |    signatory owner
         |
         |    nonconsuming choice CreateMany: ([ContractId Blob])
         |      with
         |        as: [Int]
         |        bs: [Int]
         |        cs: [Int]
         |      controller owner
         |      do
         |        blobAs <- mapA (\v -> fmap coerceContractId (create (BlobA owner v))) as
         |        blobBs <- mapA (\v -> fmap coerceContractId (create (BlobB owner v))) bs
         |        blobCs <- mapA (\v -> fmap coerceContractId (create (BlobC owner v))) cs
         |        return $ blobAs ++ blobBs ++ blobCs
         |
         |    nonconsuming choice ArchiveMany: ()
         |      with blobs: [ContractId Blob]
         |      controller owner
         |      do mapA_ archive blobs
         |
         |template BlobA
         |  with 
         |    owner: Party
         |    value: Int
         |  where
         |    signatory owner
         |    interface instance Blob for BlobA where view = BlobView value
         |
         |template BlobB
         |  with 
         |    owner: Party
         |    value: Int
         |  where
         |    signatory owner
         |    interface instance Blob for BlobB where view = BlobView value
         |
         |template BlobC
         |  with 
         |    owner: Party
         |    value: Int
         |  where
         |    signatory owner
         |    interface instance Blob for BlobC where view = BlobView value
         |""".stripMargin
  ).dependsOn(blobIface)
