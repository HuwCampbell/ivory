package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._

import org.scalacheck._


/* Non-empty, contiguous list of Factsets. */
case class Factsets(factsets: List[Factset])

object Factsets {
  implicit def FactsetsArbitrary: Arbitrary[Factsets] =
    Arbitrary(GenRepository.factsets.map(Factsets.apply))
}
