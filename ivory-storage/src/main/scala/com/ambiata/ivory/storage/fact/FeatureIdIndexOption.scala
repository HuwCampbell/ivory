package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.FeatureIdLookup

/**
 * Specialised version of [[FeatureIdIndex]], taking advantage of the fact that the value should _never_ be negative.
 */
class FeatureIdIndexOption private(val value: Int) extends AnyVal {

  def get: FeatureIdIndex = {
    if (value < 0)
      Crash.error(Crash.Invariant, s"FeatureIdIndexOption is empty: $value")
    new FeatureIdIndex(value & Int.MaxValue)
  }

  def isEmpty: Boolean =
    value < 0

  def isDefined: Boolean =
    !isEmpty
}

object FeatureIdIndexOption {

  def empty: FeatureIdIndexOption =
    new FeatureIdIndexOption(-1)

  def fromInt(i: Int): FeatureIdIndexOption = {
    if (i < 0)
      Crash.error(Crash.Invariant, s"FeatureIdIndexOption value can't be negative: $i")
    new FeatureIdIndexOption(i)
  }

  def lookup(fact: Fact, lookup: FeatureIdLookup): FeatureIdIndexOption = {
    val featureId = lookup.getIds.get(fact.featureId.toString)
    if (featureId == null)
      FeatureIdIndexOption.empty
    else
      FeatureIdIndexOption.fromInt(featureId)
  }
}
