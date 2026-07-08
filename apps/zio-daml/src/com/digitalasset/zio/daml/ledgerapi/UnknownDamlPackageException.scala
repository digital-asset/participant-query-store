// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

class UnknownDamlPackageException(packageId: String, moduleName: String, entityName: String)
    extends Exception(
      s"No package for $packageId:$moduleName:$entityName was seen on initialization. " +
        s"Retrying to discover new packages."
    )
