package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.{Date, Fact, DateValue, TombstoneValue, Crash}
import com.ambiata.ivory.core.thrift.ThriftFactValue

class DaysSinceReducer(dates: DateOffsets) extends Reduction {

  var date = Date.minValue
  var tombstone = true
  val value = new ThriftFactValue

  def clear(): Unit = {
    value.clear()
    date = Date.minValue
    tombstone = true
  }

  //TODO: Make this faster... maybe just store the whole fact rather than going in and out of the case classes.
  def update(fv: Fact): Unit = {
    date = fv.value match {
      case DateValue(v)   => v
      case TombstoneValue => Date.minValue
      case _              => Crash.error(Crash.Invariant, s"Wrong value for this reducer")
    }
    tombstone = fv.isTombstone
  }

  def save: ThriftFactValue =
    if (!tombstone) {
      // The dateOffsets array doesn't neccesarily contain the value date. 
      // e.g., We could have been saving facts for 6 months, and a person was born in 1965.
      // value.setI(dates.untilEnd(date).value)
      value.setI(DateTimeUtil.toDays(dates.end) - DateTimeUtil.toDays(date))
      value
    } else null
}
