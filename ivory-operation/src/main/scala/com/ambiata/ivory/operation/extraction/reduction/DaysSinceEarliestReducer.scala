package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.thrift.ThriftFactValue
import com.ambiata.ivory.core.{Date, Fact}

class DaysSinceEarliestReducer(dates: DateOffsets) extends Reduction {
  val sentinelDate = Date.unsafeFromInt(-1)
  var date = sentinelDate
  val value = new ThriftFactValue

  def clear(): Unit = {
    value.clear()
    date = sentinelDate
  }

  def update(fv: Fact): Unit = {
    // FIX the types, !fv.isTombstone should never be possible for sets
    //     and this function only makes sense for sets, should be fixed
    //     with #376.
    if (date == sentinelDate && !fv.isTombstone)
      date = fv.date
  }

  def save: ThriftFactValue =
    if (date != sentinelDate) {
      value.setI(dates.untilEnd(date).value)
      value
    } else null
}
