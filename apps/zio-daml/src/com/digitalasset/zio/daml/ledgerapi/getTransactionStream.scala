// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.zio.daml.ledgerapi

import com.digitalasset.canonical.specific.{Offset, Transaction}
import com.digitalasset.scribe.utils.safeequals.===
import io.grpc.Status.Code
import io.grpc.StatusException
import zio.ZIO.{logDebug, logFatal, logInfo}
import zio.stm.TRef
import zio.stream.ZStream
import zio.{Schedule, ZIO, stream}

object getTransactionStream:
  def apply[A, B](
      beginExclusive: Offset,
      mkRequest: Offset => B,
      call: B => stream.Stream[Throwable, Transaction[A]]
  ) =
    ZStream.unwrap(
      TRef.make(beginExclusive).commit.map { latestOffset =>
        ZStream
          .fromZIO(
            latestOffset.get.commit
              .map(mkRequest)
              .tap(req => logDebug(s"Starting transaction stream with ${pprint(req, height = Int.MaxValue)}"))
          )
          .flatMap(call)
          .mapChunksZIO(chunk =>
            chunk.lastOption match
              case None     => ZIO.succeed(chunk)
              case Some(tx) => latestOffset.set(tx.offset).commit.as(chunk)
          )
          .retry(Schedule.recurWhileZIO { e =>
            if isTokenExpiredException(e)
            then logInfo(s"Restarting transaction stream due to token expiration").as(true)
            else logFatal(s"No retry of fatal exception $e").as(false)
          })
      }
    )

  private def isTokenExpiredException(e: Throwable) = e match
    case ex: StatusException =>
      ex.getStatus.getCode === Code.ABORTED && ex.getStatus.getDescription.startsWith("ACCESS_TOKEN_EXPIRED")
    case _ =>
      false
