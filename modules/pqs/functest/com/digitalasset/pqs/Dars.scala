// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs

import com.digitalasset.pqs.services.daml.DamlSource

/** Daml sources shared between specs; each is compiled once per test run. */
private[pqs] object Dars:
  /** Ping(sender) and `transact1 : Party -> Script ()` creating one Ping. */
  val pingPong = DamlSource(
    "PingPong" ->
      """|module PingPong where
         |
         |import Daml.Script
         |import DA.Functor (void)
         |
         |template Ping
         |  with
         |    sender: Party
         |  where
         |    signatory sender
         |
         |transact1 : Party -> Script ()
         |transact1 party = void do
         |  submit party $ createCmd Ping with sender = party
         |""".stripMargin
  )

  /** Message(sender, receiver, text) created by `createMessage`; `createAndArchiveMessage` also archives it. */
  val message = DamlSource(
    "Message" ->
      """|module Message where
         |
         |import Daml.Script
         |import DA.Functor (void)
         |
         |template Message
         |  with
         |    sender: Party
         |    receiver: Party
         |    text: Text
         |  where
         |    signatory sender
         |    observer receiver
         |
         |createMessage : (Party, Party, Text) -> Script ()
         |createMessage (alice, bob, text) = void do
         |  submitMulti [alice, bob] [] $ createCmd Message with sender = alice, receiver = bob, text = text
         |
         |-- test Archive edge case, create another contract with implicit Archive in the same module
         |template ArchiveTest
         |  with nobody: Party
         |  where signatory nobody
         |
         |createAndArchiveMessage : (Party, Party, Text) -> Script ()
         |createAndArchiveMessage (alice, bob, text) = void do
         |  cid <- submitMulti [alice, bob] [] $ createCmd Message with sender = alice, receiver = bob, text = text
         |  submit alice $ archiveCmd cid
         |""".stripMargin
  )

  /** Label(owner, label) with a ChangeLabel choice; `setup` creates, relabels and archives a few. */
  val label = DamlSource(
    "Label" ->
      """|module Label where
         |
         |import Daml.Script
         |
         |template Label
         |  with
         |    owner : Party
         |    label : Text
         |  where
         |    signatory owner
         |    choice ChangeLabel : ContractId Label
         |      with
         |        newLabel : Text
         |      controller owner
         |      do
         |        create Label with label = newLabel, ..
         |
         |setup: Party -> Script ()
         |setup alice = do
         |  one <- submit alice $ createCmd (Label with owner = alice, label = "one")
         |  submit alice $ exerciseCmd one ChangeLabel with newLabel = "one updated"
         |  two <- submit alice $ createCmd (Label with owner = alice, label = "two")
         |  submit alice $ exerciseCmd two Archive
         |  -- ^ pruning cut-off ^ --
         |  three <- submit alice $ createCmd (Label with owner = alice, label = "three")
         |  submit alice $ exerciseCmd three ChangeLabel with newLabel = "three updated"
         |  pure ()
         |""".stripMargin
  )

  /** IPingable interface with VPingable(sender) view. */
  val pingableInterfaces = DamlSource(
    "Interfaces" ->
      """|module Interfaces where
         |
         |interface IPingable
         |  where
         |    viewtype VPingable
         |
         |data VPingable = VPingable with sender: Party
         |  deriving (Eq, Ord, Show)
         |""".stripMargin
  )

  /** IKeyed interface with VKeyed(label) view. */
  val iKeyed = DamlSource(
    "IKeyed" ->
      """|module IKeyed where
         |
         |interface IKeyed
         |  where
         |    viewtype VKeyed
         |
         |data VKeyed = VKeyed with label: Text
         |  deriving (Eq, Show)
         |""".stripMargin
  )

  /** Labelable interface with a SetLabelChoice and LabelableView(owner, label) view. */
  val labelableInterfaces = DamlSource(
    "Interfaces" ->
      """|module Interfaces where
         |
         |interface Labelable
         |  where
         |    viewtype LabelableView
         |
         |    setLabel : Text -> Labelable
         |
         |    choice SetLabelChoice : ContractId Labelable
         |      with newLabel : Text
         |      controller (view this).owner
         |      do create (setLabel this newLabel)
         |
         |data LabelableView = LabelableView with owner: Party, label: Optional Text
         |  deriving (Eq, Ord, Show)
         |
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
