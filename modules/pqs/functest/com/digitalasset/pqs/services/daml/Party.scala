// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services.daml

import java.util.concurrent.atomic.AtomicReference
import com.digitalasset.pqs.utils.safeequals.=/=

/** Allocated Daml Party. Party ID is populated after the party is allocated. */
class Party(private[daml] val prefix: String):
  private val idAndName = new AtomicReference[Option[(String, String)]](None)
  def set(id: String, name: String): this.type =
    idAndName.getAndUpdate:
      case None    => Some((id, name))
      case Some(_) => throw new RuntimeException(s"Party $prefix is already allocated")
    this

  def id: String             = idAndName.get.getOrElse(throw notInitialized)._1
  def name: String           = idAndName.get.getOrElse(throw notInitialized)._2
  private def notInitialized = new RuntimeException(s"Party $prefix is not allocated")
end Party

/** Service representing allocated parties */
final case class Parties(get: Seq[Party])

final case class User(
    primaryParty: Party,
    canActAs: Seq[Party] = Seq.empty,
    canReadAs: Seq[Party] = Seq.empty,
    canReadAsAnyParty: Boolean = false
):
  def id = primaryParty.id.takeWhile(_ =/= ':')

final case class Users(get: Seq[User])
