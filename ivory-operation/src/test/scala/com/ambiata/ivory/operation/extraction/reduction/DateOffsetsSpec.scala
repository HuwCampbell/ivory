package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.extraction.reduction.ReductionArbitraries._
import org.joda.time.{Days => JodaDays}
import org.scalacheck.Arbitrary
import org.specs2.{ScalaCheck, Specification}

class DateOffsetsSpec extends Specification with ScalaCheck { def is = s2"""
  Can calculate the number of days since the turn of the century (sparse)      $daysSparse
  Can calculate the number of days since the turn of the century (compact)     $daysCompact
  Can calculate the number of days until the end of the date range             $untilEnd
  Can increment the number of unit days in a range of dates                    $dateSetInc
"""

  case class TestDates(earlier: Date, now: Date, later: Date)

  implicit def TestDatesArbitrary: Arbitrary[TestDates] = Arbitrary(for {
    d1 <- Arbitrary.arbitrary[TestDate].map(_.d)
    d2 <- Arbitrary.arbitrary[TestDate].map(_.d)
    d3 <- Arbitrary.arbitrary[TestDate].map(_.d)
  } yield List(d1, d2, d3).sorted match {
      case List(sd1, sd2, sd3) => TestDates(sd1, sd2, sd3)
    })

  def daysSparse = prop((tds: TestDates) => {
    val offsets = DateOffsets.sparse(tds.earlier, tds.later)
    offsets.get(tds.now).value ==== DateTimeUtil.toDays(tds.now)
  })

  def daysCompact = prop((tds: TestDates) => {
    val offsets = DateOffsets.compact(tds.earlier, tds.later)
    offsets.get(tds.now).value ==== DateTimeUtil.toDays(tds.now)
  })

  def untilEnd = prop((tds: TestDates) => {
    val offsets = DateOffsets.compact(tds.earlier, tds.later)
    offsets.untilEnd(tds.now).value ==== JodaDays.daysBetween(tds.now.localDate, tds.later.localDate).getDays
  })

  def dateSetInc = prop((doc: DatesOfCount) => {
    val set = doc.offsets.createSet
    doc.allDates.foreach(set.inc)
    set.fold(0)(_ + _) ==== doc.dates.map(_._1).sum
  })
}
