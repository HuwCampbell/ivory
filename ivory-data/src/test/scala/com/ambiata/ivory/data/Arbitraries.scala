package com.ambiata.ivory.data

import org.scalacheck._, Arbitrary._

object Arbitraries {

  implicit def IdentifierArbitrary: Arbitrary[Identifier] =
    Arbitrary(arbitrary[IdentifierList].map(_.ids.last))

  case class IdentifierList(ids: List[Identifier])
  implicit def IdentifierListArbitrary: Arbitrary[IdentifierList] =
    Arbitrary(Gen.choose(0, 200) map (n => IdentifierList((1 to n).scanLeft(Identifier.initial)((acc, _) => acc.next.get).toList)))

  implicit def OldIdentifierArbitrary: Arbitrary[OldIdentifier] =
    Arbitrary(arbitrary[OldIdentifierList].map(_.ids.last))

  case class OldIdentifierList(ids: List[OldIdentifier])
  implicit def OldIdentifierListArbitrary: Arbitrary[OldIdentifierList] =
    Arbitrary(Gen.choose(0, 200) map (n => OldIdentifierList((1 to n).scanLeft(OldIdentifier.initial)((acc, _) => acc.next.get).toList)))

  case class SmallOldIdentifierList(ids: List[OldIdentifier])
  implicit def SmallOldIdentifierListArbitrary: Arbitrary[SmallOldIdentifierList] =
    Arbitrary(Gen.choose(0, 20) map (n => SmallOldIdentifierList((1 to n).scanLeft(OldIdentifier.initial)((acc, _) => acc.next.get).toList)))
}
