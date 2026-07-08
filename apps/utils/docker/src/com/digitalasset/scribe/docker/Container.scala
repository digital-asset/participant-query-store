// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.scribe.docker

case class Container(
    image: String,
    containerId: String,
    hostName: String,
    ports: Set[Int]
)
