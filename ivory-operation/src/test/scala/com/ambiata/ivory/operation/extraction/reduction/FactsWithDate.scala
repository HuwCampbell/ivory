package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.scalacheck._

case class FactsWithDate(xs: List[Fact]) {

  lazy val ds: List[Fact] =
    xs.sortBy(_.datetime.long)

  lazy val offsets: DateOffsets =
    DateOffsets.compact(
      ds.headOption.map(_.date).getOrElse(Date.minValue),
      ds.lastOption.map(_.date).getOrElse(Date.minValue)
    )
}

object FactsWithDate {

  implicit def FactsWithDateArbitrary: Arbitrary[FactsWithDate] =
    Arbitrary(Arbitrary.arbitrary[List[Fact]].map(FactsWithDate.apply))
}
