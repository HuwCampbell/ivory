package com.ambiata.ivory.data

import org.scalacheck._, Arbitrary._

object Arbitraries {

  implicit def IdentifierArbitrary: Arbitrary[Identifier] =
    Arbitrary(arbitrary[IdentifierList].map(_.ids.last))

  def createIdentifiers(n: Int): List[Identifier] =
    (1 to n).scanLeft(Identifier.initial)((acc, _) => acc.next.get).toList

  case class IdentifierList(ids: List[Identifier])
  implicit def IdentifierListArbitrary: Arbitrary[IdentifierList] =
    Arbitrary(Gen.choose(0, 200) map (n => IdentifierList(createIdentifiers(n))))

  case class SmallIdentifierList(ids: List[Identifier])
  implicit def SmallIdentifierListArbitrary: Arbitrary[SmallIdentifierList] =
    Arbitrary(Gen.choose(0, 20) map (n => SmallIdentifierList(createIdentifiers(n))))

  def createOldIdentifiers(n: Int): List[OldIdentifier] =
    (1 to n).scanLeft(OldIdentifier.initial)((acc, _) => acc.next.get).toList

  implicit def OldIdentifierArbitrary: Arbitrary[OldIdentifier] =
    Arbitrary(arbitrary[OldIdentifierList].map(_.ids.last))

  case class OldIdentifierList(ids: List[OldIdentifier])
  implicit def OldIdentifierListArbitrary: Arbitrary[OldIdentifierList] =
    Arbitrary(Gen.choose(0, 200) map (n => OldIdentifierList(createOldIdentifiers(n))))

  case class SmallOldIdentifierList(ids: List[OldIdentifier])
  implicit def SmallOldIdentifierListArbitrary: Arbitrary[SmallOldIdentifierList] =
    Arbitrary(Gen.choose(0, 20) map (n => SmallOldIdentifierList(createOldIdentifiers(n))))
}
