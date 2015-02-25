package com.ambiata.ivory.storage.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen.GenDictionary
import org.scalacheck.Arbitrary

/** Should be removed when KeyedSet is fully implemented */
case class DictionaryWithoutKeyedSet(value: Dictionary)

object DictionaryWithoutKeyedSet {

  implicit def DictionaryWithoutKeyedSetArbitrary: Arbitrary[DictionaryWithoutKeyedSet] =
    Arbitrary(GenDictionary.dictionary
      .map(d => d.copy(definitions = d.definitions.map(_.fold(
      (fid, cd) => Concrete(fid, cd.copy(mode = cd.mode.fold(Mode.State, Mode.Set, _ => Mode.Set))),
      Virtual.apply
    )))).map(DictionaryWithoutKeyedSet.apply))
}
