package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.ThriftFactPrimitiveValue
import scala.collection.mutable.{Set => MSet}

class CountBySecondaryReducer[A, B] extends ReductionFold2[KeyValue[A, MSet[B]], A, B, KeyValue[A, MSet[B]]] {

  NotImplemented.reducerPerformance("count_by_secondary")

  def initial: KeyValue[A, MSet[B]] =
    new KeyValue[A, MSet[B]]

  def fold(state: KeyValue[A, MSet[B]], value: A, secondary: B, d: Date): KeyValue[A, MSet[B]] = {
    var old = state.getOrNull(value)
    if (old == null) {
      old = MSet.empty
      state.put(value, old)
    }
    old.add(secondary)
    state
  }

  def tombstone(a: KeyValue[A, MSet[B]], date: Date): KeyValue[A, MSet[B]] =
    a

  def aggregate(a: KeyValue[A, MSet[B]]): KeyValue[A, MSet[B]] =
    a
}

// A hard-coded value instance for Set so we don't have to create another temporary instance of KeyValue with the counts
case class ReductionValueSetSize[A]() extends ReductionValueToPrim[MSet[A]] {
  def toPrim(d: MSet[A]) = ThriftFactPrimitiveValue.i(d.size)
}
