package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core.gen._
import org.scalacheck._

case class Year(year: Short)

object Year {
  implicit def YearArbitrary: Arbitrary[Year] =
    Arbitrary(GenDate.year.map(Year.apply))
}
