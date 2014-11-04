package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.{Date, Fact}
import com.ambiata.ivory.core.thrift.ThriftFactValue

class DaysSinceLatestReducer(dates: DateOffsets) extends Reduction {

  val sentinelDate = Date.unsafeFromInt(-1)
  var date = sentinelDate
  val value = new ThriftFactValue

  def clear(): Unit = {
    value.clear()
    date = sentinelDate
  }

  def update(fv: Fact): Unit = {
    // FIX It shouldn't be possible to check for tombstone in sets
    if (!fv.isTombstone)
      date = fv.date
  }

  def save: ThriftFactValue =
    if (date != sentinelDate) {
      value.setI(dates.untilEnd(date).value)
      value
    } else null
}
