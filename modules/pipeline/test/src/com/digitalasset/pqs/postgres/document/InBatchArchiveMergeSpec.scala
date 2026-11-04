// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.document

import com.digitalasset.canonical.{ContractId, Offset, SynchronizerId}
import zio.Chunk
import zio.test.*

object InBatchArchiveMergeSpec extends ZIOSpecDefault:
  private val ids    = IdPlaceholder.factory(100)
  private val sync1  = SynchronizerId("sync1")
  private val sync2  = SynchronizerId("sync2")
  private val tpl    = EntityTypePk(1)
  private val iface  = EntityTypePk(2)
  private val cid    = ContractId("cid")
  private val other  = ContractId("other")
  private val metric = MetricsContext("M:T", sync1)

  private def activate(
      ix: Long,
      tpe: EntityTypePk = tpl,
      sync: SynchronizerId = sync1,
      tpeOf: ActivationType = ActivationType.Create,
      contractId: ContractId = cid
  ) =
    Contract(
      entityType = tpe,
      activateEventPk = ids.mk,
      activatedAtIx = ix,
      activationType = tpeOf,
      contractId = contractId,
      synchronizerId = sync,
      reassignmentCounter = 0,
      signatories = Seq.empty,
      observers = Seq.empty,
      witnesses = Seq.empty,
      payload = ujson.Obj(),
      contractKey = None,
      contractKeyHash = None,
      metadata = None,
      acsDelta = true,
      packagePk = 1,
      creationPackageId = None,
      metrics = metric
    )

  private def deactivate(
      ix: Long,
      tpe: EntityTypePk = tpl,
      sync: SynchronizerId = sync1,
      tpeOf: DeactivationType = DeactivationType.Archive,
      contractId: ContractId = cid
  ) =
    DeactivatedContract(tpe, contractId, ids.mk, ix, tpeOf, sync, metric)

  private def archived(c: Contract, d: DeactivatedContract) = ArchivedContract(c, d)

  private def rows(copies: Iterable[Copy]) = copies.map(c => (c.table.name, c.table.columns, c.row)).toList

  private def mergesTo(batch: Model*)(expected: Copy*) =
    assertTrue(rows(Model.mergeInBatchArchives(batch)) == rows(expected))

  override def spec = suite("in-batch archive merge")(
    test("an archive whose create is in the batch is written into the create's row and not staged") {
      val c = activate(1)
      val d = deactivate(3)
      mergesTo(c, d)(archived(c, d))
    },
    test("the merged row is the create's row followed by the archive's deactivation columns") {
      val c   = activate(1)
      val d   = deactivate(3)
      val row = Model.mergeInBatchArchives(Seq(c, d)).map(_.row)
      assertTrue(
        row == Chunk(s"${c.row}\t${d.deactivationRow}"),
        d.deactivationRow.split('\t').toList.drop(1) == List("3", "archive"),
        ArchivedContract.name == "__contracts",
        ArchivedContract.columns.takeRight(3) == Seq("deactivate_event_pk", "deactivated_at_ix", "deactivation_type")
      )
    },
    test("a create and archive in the same transaction are merged") {
      val c = activate(5)
      val d = deactivate(5)
      mergesTo(c, d)(archived(c, d))
    },
    test("an archive whose create is in an earlier batch is staged") {
      val d = deactivate(3)
      mergesTo(d)(d)
    },
    test("rows other than contracts and deactivations are kept in order") {
      val t  = Transaction(1, Offset.Absolute(10))
      val c  = activate(1)
      val d  = deactivate(1)
      val wm = Watermark(1, Offset.Absolute(10), Seq.empty)
      mergesTo(t, c, d, wm)(t, archived(c, d))
    },
    test("each interface view row is merged with the archive row of its own tpe_pk") {
      val ct = activate(1, tpe = tpl)
      val ci = activate(1, tpe = iface)
      val dt = deactivate(2, tpe = tpl)
      val di = deactivate(2, tpe = iface)
      mergesTo(ct, ci, di, dt)(archived(ct, dt), archived(ci, di))
    },
    test("an archive row for an interface view the create did not write is staged") {
      val ct = activate(1, tpe = tpl)
      val dt = deactivate(2, tpe = tpl)
      val di = deactivate(2, tpe = iface)
      mergesTo(ct, di, dt)(archived(ct, dt), di)
    },
    test("an archive on another synchronizer is not merged") {
      val c = activate(1, sync = sync1)
      val d = deactivate(3, sync = sync2)
      mergesTo(c, d)(c, d)
    },
    test("an archive of another contract is not merged") {
      val c = activate(1)
      val d = deactivate(3, contractId = other)
      mergesTo(c, d)(c, d)
    },
    test("an archive before the create is not merged") {
      val c = activate(4)
      val d = deactivate(3)
      mergesTo(c, d)(c, d)
    },
    test("an unassign is never merged") {
      val c = activate(1)
      val u = deactivate(2, tpeOf = DeactivationType.Unassign)
      mergesTo(c, u)(c, u)
    },
    test("an archive whose latest activation is an assign is not merged") {
      val a = activate(1, tpeOf = ActivationType.Assign)
      val d = deactivate(2)
      mergesTo(a, d)(a, d)
    },
    test("an archive after an unassign and an assign back is matched to the assign, so it is not merged") {
      val c = activate(1)
      val u = deactivate(2, tpeOf = DeactivationType.Unassign)
      val a = activate(3, tpeOf = ActivationType.Assign)
      val d = deactivate(4)
      mergesTo(c, u, a, d)(c, u, a, d)
    },
    test("an archive on the target synchronizer of a reassignment is not merged with the source's create") {
      val c = activate(1, sync = sync1)
      val u = deactivate(2, sync = sync1, tpeOf = DeactivationType.Unassign)
      val a = activate(3, sync = sync2, tpeOf = ActivationType.Assign)
      val d = deactivate(4, sync = sync2)
      mergesTo(c, u, a, d)(c, u, a, d)
    },
    test("only the contracts created in the batch are merged") {
      val c  = activate(1)
      val d  = deactivate(2)
      val d2 = deactivate(2, contractId = other)
      mergesTo(c, d2, d)(archived(c, d), d2)
    }
  )
