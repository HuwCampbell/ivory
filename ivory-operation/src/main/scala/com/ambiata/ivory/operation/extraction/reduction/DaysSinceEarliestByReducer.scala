package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.{Date, NotImplemented}

class DaysSinceEarliestByReducer(dates: DateOffsets) extends ReductionFoldWithDate[KeyValue[String, Int], String, KeyValue[String, Int]] {

  NotImplemented.reducerPerformance("days_since_earliest_by")

  def initial: KeyValue[String, Int] =
    new KeyValue[String, Int]

  def foldWithDate(a: KeyValue[String, Int], b: String, date: Date): KeyValue[String, Int] = {
    if (a.getOrElse(b, 0) == 0) a.put(b, dates.untilEnd(date).value)
    a
  }

  def tombstoneWithDate(a: KeyValue[String, Int], B: Date): KeyValue[String, Int] =
    a

  def aggregate(a: KeyValue[String, Int]): KeyValue[String, Int] =
    a
}
