package com.ambiata.ivory.storage.lookup

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup._
import scala.collection.JavaConverters._

object FeatureLookups {
  def isSetTable(dictionary: Dictionary): FlagLookup = {
    val isSet = new FlagLookup
    dictionary.byFeatureIndex.foreach({
      case (n, Concrete(id, definition)) =>
        isSet.putToFlags(n, definition.mode.isSet)
      case (n, Virtual(id, definition)) =>
        ()
    })
    isSet
  }

  def isSetLookupToArray(lookup: FlagLookup): Array[Boolean] = {
    val all = lookup.getFlags.asScala.toList
    val max = all.map(_._1).max
    val out = Array.fill(max + 1)(false)
    all.foreach({ case (i, v) => out(i) = v })
    out
  }
}
