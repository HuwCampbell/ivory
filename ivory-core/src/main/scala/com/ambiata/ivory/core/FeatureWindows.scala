package com.ambiata.ivory.core

import scalaz._

case class FeatureWindows(features: List[FeatureWindow]) {
  /** Does any feature have a window. */
  def hasWindows: Boolean =
    features.exists(!_.windows.isEmpty)

  /** Determine the mapping of namespace to the (exclusive) start date of the window
      for each namespace given the specified (inclusive) end date of the window. */
  def byNamespace(to: Date): Ranges[Namespace] =
    Ranges.toNamespaces(byFeature(to))

  /** Determine the mapping of feature to the (exclusive) start date of the window
      for each namespace given the specified (inclusive) end date of the window. */
  def byFeature(to: Date): Ranges[FeatureId] =
    Ranges(features.map(feature => Range(feature.id, feature.windows.map(Window.startingDate(_, to)), to)))
}

object FeatureWindows {
  implicit def FeatureWindowsEqual: Equal[FeatureWindows] =
    Equal.equalA
}
