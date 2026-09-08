// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services.daml

import java.util.concurrent.atomic.AtomicReference

class Synchronizer(private[daml] val name: String):
  private val _id = new AtomicReference[Option[String]](None)
  def set(id: String): this.type =
    _id.getAndUpdate:
      case None    => Some((id))
      case Some(_) => throw new RuntimeException(s"Synchronizer $name is already connected")
    this

  def id: String = _id.get.getOrElse(throw RuntimeException(s"Synchronizer $name is not connected"))
