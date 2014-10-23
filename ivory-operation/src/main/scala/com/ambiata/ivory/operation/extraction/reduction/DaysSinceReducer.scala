package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.{Date, Fact, DateValue, TombstoneValue, Crash}
import com.ambiata.ivory.core.thrift.ThriftFactValue

class DaysSinceReducer(dates: DateOffsets) extends ReductionFold[ValueOrTombstone[Int], Int, ValueOrTombstone[Int]] {

  val daysEndOfWindow = DateTimeUtil.toDays(dates.end)

  def initial: ValueOrTombstone[Int] =
    ValueOrTombstone(0, true)

  def fold(s: ValueOrTombstone[Int], y: Int): ValueOrTombstone[Int] = {
    s.value = y
    s.tombstone = false
    s
  }

  def tombstone(s: ValueOrTombstone[Int]): ValueOrTombstone[Int] = {
    s.tombstone = true
    s
  }

  def aggregate(s: ValueOrTombstone[Int]): ValueOrTombstone[Int] = {
    if (!s.tombstone) {
      // The dateOffsets array doesn't necessarily contain the value date.
      // e.g., We could have been saving facts for 6 months, and a person was born in 1965.
      s.value = daysEndOfWindow - DateTimeUtil.toDays(Date.unsafeFromInt(s.value))
    }
    s
  }
}
