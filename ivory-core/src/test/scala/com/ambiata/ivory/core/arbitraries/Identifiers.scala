package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._

import org.scalacheck._


case class Identifiers(ids: List[Identifier])

object Identifiers {
  implicit def IdentifiersArbitrary: Arbitrary[Identifiers] =
    Arbitrary(GenIdentifier.identifiers.map(Identifiers.apply))
}
