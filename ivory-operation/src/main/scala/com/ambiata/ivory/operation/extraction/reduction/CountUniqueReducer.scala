package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.NotImplemented

case class CountUniqueState[A](set: collection.mutable.HashSet[A])

class CountUniqueReducer[A] extends ReductionFold[CountUniqueState[A], A, Long] {

  NotImplemented.reducerPerformance("count_unique")

  def initial: CountUniqueState[A] =
    CountUniqueState(collection.mutable.HashSet[A]())

  def fold(a: CountUniqueState[A], b: A): CountUniqueState[A] = {
    a.set += b
    a
  }

  def tombstone(a: CountUniqueState[A]): CountUniqueState[A] =
    a

  def aggregate(a: CountUniqueState[A]): Long =
    a.set.size
}
