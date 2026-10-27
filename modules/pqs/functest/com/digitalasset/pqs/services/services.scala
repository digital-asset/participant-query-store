// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services

import zio.*

@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
def inspectMaybe[T: Tag]: UIO[Option[T]] =
  ZIO.environment[Any].mapAttempt(_.asInstanceOf[ZEnvironment[T]].get[T]).option
