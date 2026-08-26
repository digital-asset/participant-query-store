// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.diagnostics.util

import scala.collection.immutable.Queue

/** A ring buffer with fixed capacity.
  *
  * @param capacity
  *   total capacity
  * @param size
  *   initial size
  * @param queue
  *   underlying queue
  * @tparam A
  *   element type
  */
case class Ring[+A] private (capacity: Int, size: Int, queue: Queue[A]):
  def push[B >: A](b: B): (Option[A], Ring[B]) =
    if size < capacity
    then (None, Ring(capacity, size + 1, queue.enqueue(b)))
    else
      queue.dequeue match
        case (h, t) => (Some(h), Ring(capacity, size, t.enqueue(b)))

  def pop: Option[(A, Ring[A])] = queue.dequeueOption.map {
    case (h, t) => (h, Ring(capacity, size - 1, t))
  }

  def iterator: Iterator[A] = queue.iterator

object Ring:
  def empty[A](capacity: Int): Ring[A] = Ring(capacity, 0, Queue.empty)

  def apply[A](capacity: Int)(xs: A*): Ring[A] =
    val elems = if xs.size <= capacity then xs else xs.takeRight(capacity)
    Ring(capacity, elems.size, Queue(elems*))
