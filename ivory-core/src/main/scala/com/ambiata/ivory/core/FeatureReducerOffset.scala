package com.ambiata.ivory.core

case class FeatureReducerOffset(offset: Short, count: Short) {

  /**
   * Store the feature index (different from the featureId) and number of reducers.
   * The reason for a single [[Int]] is largely to avoid having to generate yet-another bespoke thrift lookup class.
   * We're assuming that the number of features or reducers is < [[Short.MaxValue]].
   */
  def toInt: Int =
    (offset << 16) | count
}

object FeatureReducerOffset {

  /** Calculate the reducer for a given entity based on the encoded [[Int]] (see [[FeatureReducerOffset.toInt( )]]) */
  def getReducer(lookup: Int, entity: Int): Int = {
    val offset = lookup >> 16
    val count = lookup & 0xffff
    ((entity & Int.MaxValue) % count) + offset
  }
}
