// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.functest

import com.digitalasset.pqs.docker.Docker
import zio.ZLayer

/** Don't share infrastructure with any other tests. Tests run in their own designated pool. */
abstract class FuncTestStandalone extends FuncTest[Any]:
  def shared: ZLayer[FTEnv & Docker, Throwable, Any] = ZLayer.empty.fresh
