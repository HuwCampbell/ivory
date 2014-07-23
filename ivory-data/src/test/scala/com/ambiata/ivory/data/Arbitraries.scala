package com.ambiata.ivory.data

import org.scalacheck._, Arbitrary._

object Arbitraries {

  implicit def IdentifierArbitrary: Arbitrary[Identifier] =
    Arbitrary(Gen.choose(0, 200) map (n => (1 to n).foldLeft(Identifier.initial)((acc, _) => acc.next.get)))
}
