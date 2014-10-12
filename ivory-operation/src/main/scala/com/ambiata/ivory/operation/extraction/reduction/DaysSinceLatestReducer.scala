package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.{Date, Fact}
import com.ambiata.ivory.core.thrift.ThriftFactValue

class DaysSinceLatestReducer(dates: DateOffsets) extends Reduction {

  var date = Date.minValue
  var tombstone = true
  val value = new ThriftFactValue

  def clear(): Unit = {
    value.clear()
    date = Date.minValue
    tombstone = true
  }

  def update(fv: Fact): Unit = {
    date = fv.date
    tombstone = fv.isTombstone
  }

  def save: ThriftFactValue =
    if (!tombstone) {
      value.setI(dates.untilEnd(date).value)
      value
    } else null
}
