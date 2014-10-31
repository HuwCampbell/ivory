package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.thrift.ThriftFactValue
import com.ambiata.ivory.core.{Date, Fact}

class DaysSinceEarliestReducer(dates: DateOffsets) extends Reduction {

  var sentinelDate = Date.unsafeFromInt(-1)
  var date = sentinelDate
  var tombstone = true
  val value = new ThriftFactValue

  def clear(): Unit = {
    value.clear()
    date = sentinelDate
    tombstone = true
  }

  def update(fv: Fact): Unit = {
    if (date == sentinelDate) {
      date = fv.date
      tombstone = fv.isTombstone
    }
  }

  def save: ThriftFactValue =
    if (date != sentinelDate && !tombstone) {
      value.setI(dates.untilEnd(date).value)
      value
    } else null
}
