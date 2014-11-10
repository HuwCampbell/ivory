package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core.gen._
import org.scalacheck._

case class Month(month: Byte)

object Month {
  implicit def MonthArbitrary: Arbitrary[Month] =
    Arbitrary(GenDate.month.map(Month.apply))
}
