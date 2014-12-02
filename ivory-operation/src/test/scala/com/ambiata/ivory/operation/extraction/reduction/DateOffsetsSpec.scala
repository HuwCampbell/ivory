package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.operation.extraction.reduction.ReductionArbitraries._
import org.joda.time.{Days => JodaDays}
import org.scalacheck.Arbitrary
import org.specs2.{ScalaCheck, Specification}

class DateOffsetsSpec extends Specification with ScalaCheck { def is = s2"""
  Can calculate the number of days since the turn of the century (sparse)      $daysSparse
  Can calculate the number of days since the turn of the century (compact)     $daysCompact
  Can calculate the number of days until the end of the date range             $untilEnd
  Can increment the number of unit days in a range of dates                    $dateSetInc
  Can increment the number of unit weeks in a range of dates                   $weekSetInc
  Offset should return the window offset as a minimum for earlier dates        $dateBefore
"""

  def daysSparse = prop((tds: UniqueDates) => {
    val offsets = DateOffsets.sparse(tds.earlier, tds.later)
    offsets.get(tds.now).value ==== DateTimeUtil.toDays(tds.now)
  })

  def daysCompact = prop((tds: UniqueDates) => {
    val offsets = DateOffsets.compact(tds.earlier, tds.later)
    offsets.get(tds.now).value ==== DateTimeUtil.toDays(tds.now)
  })

  def untilEnd = prop((tds: UniqueDates) => {
    val offsets = DateOffsets.compact(tds.earlier, tds.later)
    offsets.untilEnd(tds.now).value ==== JodaDays.daysBetween(tds.now.localDate, tds.later.localDate).getDays
  })

  def dateSetInc = prop((doc: DatesOfCount) => {
    val set = doc.offsets.createSet
    doc.allDates.foreach(set.inc)
    set.fold(0)(_ + _) ==== doc.dates.map(_._1).sum
  })

  def weekSetInc = prop((doc: DatesOfCount) => {
    var sum = 0
    var count = 0
    val set = doc.offsets.createSet
    doc.allDates.foreach(set.inc)
    set.foreachWeeks { i =>
      sum += i
      count += 1
    }
    (sum, count) ==== ((doc.datesWithFullWeeks.map(_._1).sum, doc.noOfWeeksFloor))
  })

  def dateBefore = prop((doc: DatesOfCount) => {
    doc.offsets.get(Date.minValue).value ==== doc.offsets.offsets(0)
  })
}
