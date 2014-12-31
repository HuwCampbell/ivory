package com.ambiata.ivory.core

case class FeatureWindows(features: List[FeatureWindow]) {
  /** Does any feature have a window. */
  def hasWindows: Boolean =
    features.exists(!_.windows.isEmpty)

  /** Determine the mapping of namespace to the (exclusive) start date of the window
      for each namespace given the specified (inclusive) end date of the window. */
  def byNamespace(to: Date): NamespaceRanges =
    byFeature(to).toNamespaces

  /** Determine the mapping of feature to the (exclusive) start date of the window
      for each namespace given the specified (inclusive) end date of the window. */
  def byFeature(to: Date): FeatureRanges
  =
    FeatureRanges(features.map(feature => FeatureRange(feature.id, feature.windows.map(Window.startingDate(_, to)), to)))
}
