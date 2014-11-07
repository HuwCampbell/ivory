package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._

import org.scalacheck._, Arbitrary._
import Arbitraries._


/** Facts for a _single_ [[ConcreteDefinition]] (feel free to generate a [[List]] of them if you need more) */
case class FactsWithDictionary(cg: ConcreteGroupFeature, facts: List[Fact]) {
  def dictionary: Dictionary = cg.dictionary
}

object FactsWithDictionary {

  /**
   * Create an arbitrary fact and timezone such that the time in the fact is valid given the timezone
   */
  implicit def FactsWithDictionaryArbitrary: Arbitrary[FactsWithDictionary] =
    Arbitrary(for {
      cg <- arbitrary[ConcreteGroupFeature]
      n <- Gen.choose(2, 10)
      dt <- arbitrary[DateTime]
      facts <- Gen.listOfN(n, GenFact.factWith(GenEntity.entity, cg.fid, cg.cg.definition, dt))
    } yield FactsWithDictionary(cg, facts))
}
