// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.functest
import com.digitalasset.scribe.docker.Docker
import zio.ZLayer

/** Tests don't require expensive shared infrastructure and might be run in a common pool in parallel */
abstract class FuncTestDefault extends FuncTest[Any]:
  def shared: ZLayer[Docker, Throwable, Any] = ZLayer.empty
