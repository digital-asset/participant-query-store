// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio

import com.digitalasset.transcode.schema.Dictionary
import com.digitalasset.transcode.Codec

package object daml:
  val LedgerScope = "daml_ledger_api"

  type ProtobufCodecs = Dictionary[Codec[com.digitalasset.transcode.codec.proto.Value]]
  type JsonCodecs     = Dictionary[Codec[ujson.Value]]
