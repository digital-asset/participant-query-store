// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package zio

import zio.Differ.SetPatch

/** A backdoor object to access package-private currentLoggers. This is strictly reserved for FuncTest to swap the
  * global test logger with a test-local silent logger.
  */
object FiberRefAccess:
  val currentLoggers: FiberRef.WithPatch[Set[ZLogger[String, Any]], SetPatch[ZLogger[String, Any]]] =
    FiberRef.currentLoggers
