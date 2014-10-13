package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.Fact
import com.ambiata.ivory.core.thrift.ThriftFactValue

/** A common pattern for a few of the reducers */
class DateReduction[@specialized(Int, Long, Double) A](offsets: DateOffsets, reducer: DateReducer[A], out: ReductionValueTo[A]) extends Reduction {

  val value = new ThriftFactValue()
  val days = offsets.createSet

  def clear(): Unit = {
    value.clear()
    days.clear()
  }

  def update(f: Fact): Unit =
    if (!f.isTombstone) days.inc(f.date)

  def save: ThriftFactValue = {
    out.to(reducer.aggregate(days), value)
    value
  }
}

trait DateReducer[A] {
  def aggregate(set: MutableDateSet): A
}
