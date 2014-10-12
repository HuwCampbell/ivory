package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.NotImplemented

class CountByReducer[A] extends ReductionFold[KeyValue[A, Long], A, KeyValue[A, Long]] {

  NotImplemented.reducerPerformance("count_by")

  def initial: KeyValue[A, Long] =
    new KeyValue[A, Long]

  def fold(state: KeyValue[A, Long], value: A): KeyValue[A, Long] = {
    val old = state.getOrElse(value, 0)
    state.put(value, old + 1)
    state
  }

  def tombstone(a: KeyValue[A, Long]): KeyValue[A, Long] =
    a

  def aggregate(a: KeyValue[A, Long]): KeyValue[A, Long] =
    a
}
