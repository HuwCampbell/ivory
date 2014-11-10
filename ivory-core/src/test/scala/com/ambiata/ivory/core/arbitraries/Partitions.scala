package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._
import org.scalacheck._

case class Partitions(partitions: List[Partition])

object Partitions {
  implicit def PartitionsArbitrary: Arbitrary[Partitions] =
    Arbitrary(GenRepository.partitions.map(Partitions.apply))
}
