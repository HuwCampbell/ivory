package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.core._
import org.joda.time.Days
import org.scalacheck.{Gen, Arbitrary}

object ReductionArbitraries {

  case class TestDate(d: Date)

  /* The normal arbitrary goes to year 3000, which overflows the JodaTime test version (but not the actual version) */
  implicit def TestDateArbitrary: Arbitrary[TestDate] =
    Arbitrary(genDate(Date(2001, 1, 1), Date(2050, 12, 31)).map(TestDate.apply))

  case class DatesOfCount(dates: List[(Short, Date)]) {

    lazy val offsets: DateOffsets =
      DateOffsets.compact(dates.head._2, dates.last._2)

    lazy val allDates: List[Date] =
      dates.flatMap { case (n, d) => List.fill(Math.abs(n))(d)}.sorted

    lazy val noOfDays: Int =
      Days.daysBetween(allDates.head.localDate, allDates.last.localDate).getDays + 1

    lazy val noOfWeeksFloor: Int =
      noOfDays / 7
  }

  implicit def ArbitraryDatesOfCount: Arbitrary[DatesOfCount] = Arbitrary(for {
    n  <- Gen.choose(1, 20)
    ds <- Gen.listOfN(n, for {
      s  <- Gen.choose[Short](1, 10)
      dt <- Arbitrary.arbitrary[TestDate]
    } yield s -> dt.d)
    // Need to make sure we remove duplicates, we could also generate one date and then plus random intervals
    dates = ds.groupBy(_._2).values.map(_.head).toList.sortBy(_._2)
  } yield DatesOfCount(dates))
}
