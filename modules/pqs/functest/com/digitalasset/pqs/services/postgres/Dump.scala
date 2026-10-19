// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pqs.services.postgres

final case class Dump(databaseName: String, file: os.Path):
  def containerPath: os.Path = os.root / "dump" / file.last
