package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.core.Fact

/*
 * Note: These implement the less pure Reduction trait, because ReductionFoldWithDate requires a primitive
 *       type for the fact being examined (structs have one of their components specified and rolled in).
 *       Here, we don't mind or know what type to use, and one can look at intervals between events which
 *       come in as structs without trying to infer a type we would ignore anyway.
 */

class IntervalReducer[A, B](dates: DateOffsets, r: ReductionFoldWithDate[A, Long, B], to: ReductionValueTo[B]) extends Reduction {
  val value           = new ThriftFactValue
  var lastDate        = -1
  var a               = r.initial

  def clear(): Unit = {
    value.clear()
    lastDate  = -1
    a         = r.initial
  }

  def update(fact: Fact): Unit = {
    if (!fact.isTombstone) {
      val x = dates.get(fact.date).value
      if (lastDate != -1) {
        a = r.foldWithDate(a, x - lastDate, fact.date)
      }
      lastDate = x
    }
  }

  def save: ThriftFactValue = {
    to.to(r.aggregate(a), value)
    value
  }
}
