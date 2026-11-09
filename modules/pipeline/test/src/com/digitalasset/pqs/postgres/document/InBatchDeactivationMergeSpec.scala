// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.postgres.document

import com.digitalasset.canonical.{ContractId, Offset, SynchronizerId}
import zio.Chunk
import zio.test.*

object InBatchDeactivationMergeSpec extends ZIOSpecDefault:
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

  private def merged(c: Contract, d: DeactivatedContract) = c.deactivatedBy(d)

  private def rows(copies: Iterable[Copy]) = copies.map(c => (c.table.copyQuery, c.row)).toList

  private def mergesTo(batch: Model*)(expected: Copy*) =
    assertTrue(rows(Model.mergeInBatchDeactivations(batch)) == rows(expected))

  override def spec = suite("in-batch deactivation merge")(
    test("a deactivation whose activation is in the batch is written into the activation's row and not staged") {
      val c = activate(1)
      val d = deactivate(3)
      mergesTo(c, d)(merged(c, d))
    },
    test("the merged row is the activation's row with the deactivation columns filled in") {
      val c      = activate(1)
      val d      = deactivate(3)
      val fields = (row: String) => row.split('\t').toList
      val result = Model.mergeInBatchDeactivations(Seq(c, d)).map(r => fields(r.row))
      assertTrue(
        Contract.copyQuery.endsWith("deactivate_event_pk, deactivated_at_ix, deactivation_type) from stdin"),
        fields(c.row).takeRight(3) == List("\\N", "\\N", "\\N"),
        result.map(_.dropRight(3)) == Chunk(fields(c.row).dropRight(3)),
        result.map(_.takeRight(3)) == Chunk(List(d.deactivateEventPk.id.toString, "3", "archive"))
      )
    },
    test("an activation and deactivation in the same transaction are merged") {
      val c = activate(5)
      val d = deactivate(5)
      mergesTo(c, d)(merged(c, d))
    },
    test("a deactivation whose activation is in an earlier batch is staged") {
      val d = deactivate(3)
      mergesTo(d)(d)
    },
    test("rows other than contracts and deactivations are kept in order") {
      val t  = Transaction(1, Offset.Absolute(10))
      val c  = activate(1)
      val d  = deactivate(1)
      val wm = Watermark(1, Offset.Absolute(10), Seq.empty)
      mergesTo(t, c, d, wm)(t, merged(c, d))
    },
    test("each interface view row is merged with the deactivation row of its own tpe_pk") {
      val ct = activate(1, tpe = tpl)
      val ci = activate(1, tpe = iface)
      val dt = deactivate(2, tpe = tpl)
      val di = deactivate(2, tpe = iface)
      mergesTo(ct, ci, di, dt)(merged(ct, dt), merged(ci, di))
    },
    test("a deactivation row for an interface view the activation did not write is staged") {
      val ct = activate(1, tpe = tpl)
      val dt = deactivate(2, tpe = tpl)
      val di = deactivate(2, tpe = iface)
      mergesTo(ct, di, dt)(merged(ct, dt), di)
    },
    test("a deactivation on another synchronizer is not merged") {
      val c = activate(1, sync = sync1)
      val d = deactivate(3, sync = sync2)
      mergesTo(c, d)(c, d)
    },
    test("a deactivation of another contract is not merged") {
      val c = activate(1)
      val d = deactivate(3, contractId = other)
      mergesTo(c, d)(c, d)
    },
    test("a deactivation before the activation is not merged") {
      val c = activate(4)
      val d = deactivate(3)
      mergesTo(c, d)(c, d)
    },
    test("an unassign is merged into its create's row") {
      val c = activate(1)
      val u = deactivate(2, tpeOf = DeactivationType.Unassign)
      mergesTo(c, u)(merged(c, u))
    },
    test("an archive is merged into its assign's row") {
      val a = activate(1, tpeOf = ActivationType.Assign)
      val d = deactivate(2)
      mergesTo(a, d)(merged(a, d))
    },
    test("after an unassign and an assign back, each deactivation is merged into its own activation's row") {
      val c = activate(1)
      val u = deactivate(2, tpeOf = DeactivationType.Unassign)
      val a = activate(3, tpeOf = ActivationType.Assign)
      val d = deactivate(4)
      mergesTo(c, u, a, d)(merged(c, u), merged(a, d))
    },
    test("a reassignment between synchronizers is merged on each synchronizer separately") {
      val c = activate(1, sync = sync1)
      val u = deactivate(2, sync = sync1, tpeOf = DeactivationType.Unassign)
      val a = activate(3, sync = sync2, tpeOf = ActivationType.Assign)
      val d = deactivate(4, sync = sync2)
      mergesTo(c, u, a, d)(merged(c, u), merged(a, d))
    },
    test("a contract moved S1 -> S2 -> S1 has each deactivation merged into the activation it closes") {
      val c  = activate(1, sync = sync1)
      val u1 = deactivate(2, sync = sync1, tpeOf = DeactivationType.Unassign)
      val a2 = activate(3, sync = sync2, tpeOf = ActivationType.Assign)
      val u2 = deactivate(4, sync = sync2, tpeOf = DeactivationType.Unassign)
      val a1 = activate(5, sync = sync1, tpeOf = ActivationType.Assign)
      val d  = deactivate(6, sync = sync1)
      mergesTo(c, u1, a2, u2, a1, d)(merged(c, u1), merged(a2, u2), merged(a1, d))
    },
    test("only the contracts activated in the batch are merged") {
      val c  = activate(1)
      val d  = deactivate(2)
      val d2 = deactivate(2, contractId = other)
      mergesTo(c, d2, d)(merged(c, d), d2)
    }
  )
