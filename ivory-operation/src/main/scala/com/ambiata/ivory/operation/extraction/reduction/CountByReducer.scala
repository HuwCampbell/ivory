package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.NotImplemented


class CountByReductio[A] extends Reductio[A, KeyValue[A, Long]] {
  type X = KeyValue[A, Long]

  val state = new KeyValue[A, Long]

  def seed: X = {
    state.clear
    state
  }

  def step(state: X, value: A): X = {
    val old = state.getOrElse(value, 0)
    state.put(value, old + 1)
    state
  }

  def stop: X =
    state
}

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
