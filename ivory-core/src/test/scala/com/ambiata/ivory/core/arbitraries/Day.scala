package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core.gen._
import org.scalacheck._

case class Day(day: Byte)

object Day {
  implicit def DayArbitrary: Arbitrary[Day] =
    Arbitrary(GenDate.day.map(Day.apply))
}
