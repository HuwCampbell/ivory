package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._

import org.scalacheck._


/** set of 3 unique dates so that earlier < now < later */
case class UniqueDates(earlier: Date, now: Date, later: Date)

object UniqueDates {
  implicit def UniqueDatesArbitrary: Arbitrary[UniqueDates] =
    Arbitrary(for {
      d1 <- GenDate.dateIn(Date(1970, 1, 1), Date(2050, 12, 31))
      d2 <- Gen.frequency(5 -> Date.minValue, 95 -> Gen.choose(1, 100).map(o => Date.fromLocalDate(d1.localDate.minusDays(o))))
      d3 <- Gen.frequency(5 -> Date.maxValue, 95 -> Gen.choose(1, 100).map(o => Date.fromLocalDate(d1.localDate.plusDays(o))))
    } yield UniqueDates(d2, d1, d3))
}
