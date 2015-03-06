package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._
import org.scalacheck._

case class ConcreteGroupFact(cg: ConcreteGroupFeature, fact: Fact) {

  def onDefinition(f: ConcreteDefinition => ConcreteDefinition): ConcreteGroupFact =
    copy(cg = cg.onDefinition(f))

  def dictionary: Dictionary =
    cg.dictionary
}

object ConcreteGroupFact {
  implicit def ConcreteGroupFactArbitrary: Arbitrary[ConcreteGroupFact] = Arbitrary(for {
    cg <- ConcreteGroupFeature.ConcreteGroupFeatureArbitrary.arbitrary
    f  <- GenFact.factWithZone(GenEntity.entity, Gen.const(cg.cg.definition))
  } yield ConcreteGroupFact(cg, f._2.withFeatureId(cg.fid)))
}
