package com.ambiata.ivory.data

import org.scalacheck._, Arbitrary._

object Arbitraries {

  implicit def IdentifierArbitrary: Arbitrary[Identifier] =
    Arbitrary(arbitrary[IdentifierList].map(_.ids.last))

  case class IdentifierList(ids: List[Identifier])
  implicit def IdentifierListArbitrary: Arbitrary[IdentifierList] =
    Arbitrary(Gen.choose(0, 200) map (n => IdentifierList((1 to n).scanLeft(Identifier.initial)((acc, _) => acc.next.get).toList)))
}
