package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core.gen.GenPlus
import com.ambiata.ivory.core.{Dictionary, Fact}
import org.scalacheck.Arbitrary

case class FactsWithDictionaryMulti(fwd: List[FactsWithDictionary]) {

  lazy val dictionary: Dictionary =
    fwd.map(_.dictionary).foldLeft(Dictionary.empty)(_.append(_))

  lazy val facts: List[Fact] =
    fwd.flatMap(_.facts)
}

object FactsWithDictionaryMulti {

  implicit def FactsWithDictionaryMultiArbitrary: Arbitrary[FactsWithDictionaryMulti] =
    Arbitrary(GenPlus.nonEmptyListOf(Arbitrary.arbitrary[FactsWithDictionary]).map(FactsWithDictionaryMulti.apply))
}
