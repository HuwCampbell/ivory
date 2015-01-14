package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._
import org.joda.time.Days
import org.scalacheck.{Gen, Arbitrary}

object ReductionArbitraries {
  case class DatesOfCount(dates: List[(Short, Date)]) {

    lazy val offsets: DateOffsets =
      if (dates.isEmpty)
        DateOffsets.compact(Date.minValue, Date.minValue)
      else
        DateOffsets.compact(dates.head._2, dates.last._2)

    lazy val allDates: List[Date] =
      dates.flatMap { case (n, d) => List.fill(Math.abs(n))(d)}.sorted

    lazy val noOfDays: Int =
      Days.daysBetween(allDates.head.localDate, allDates.last.localDate).getDays + 1

    lazy val noOfWeeksFloor: Int =
      noOfDays / 7

    // We ignore the days not contained in a full week
    // NOTE: This is date _before_ the full week, and comparisons should be > _not_ >=
    // So for 14/2/2014 with 2 full week range this would be 31/1/2014, which lies on the same day of the week
    lazy val dateBeforeFirstFullWeek: Date =
      dates.lastOption.map(_._2).map(_.localDate.minusDays(noOfWeeksFloor * 7)).map(Date.fromLocalDate).getOrElse(Date.minValue)

    lazy val datesWithFullWeeks: List[(Short, Date)] =
      dates.filter(_._2.int > dateBeforeFirstFullWeek.int)
  }

  implicit def ArbitraryDatesOfCount: Arbitrary[DatesOfCount] = Arbitrary(for {
    n  <- Gen.choose(1, 20)
    ds <- Gen.listOfN(n, for {
      s  <- Gen.choose[Short](1, 10)
      // FIX ARB This fails with a wider range (i.e. 1991)
      dt <- GenDate.dateIn(Date(2001, 1, 1), Date(2050, 12, 31))
    } yield s -> dt)
    // Need to make sure we remove duplicates, we could also generate one date and then plus random intervals
    dates = ds.groupBy(_._2).values.map(_.head).toList.sortBy(_._2)
  } yield DatesOfCount(dates))
}
