package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._

import org.scalacheck._, Arbitrary._
import Arbitraries._


case class RandomName(name: String)

object RandomName {
  implicit def RandomNameArbitrary: Arbitrary[RandomName] =
    Arbitrary(Gen.oneOf(
        arbitrary[BadName].map(_.name)
      , arbitrary[Name].map(_.name)
      ).map(RandomName.apply))
}
