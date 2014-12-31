package com.ambiata.ivory.core

/* A date range of interest for a specified feature, from is _exclusive_, to is _inclusive_. */
case class FeatureRange(id: FeatureId, froms: List[Date], to: Date) {
  def from: Option[Date] =
    froms.sorted.headOption

  def fromOrMin: Date =
    from.getOrElse(Date.minValue)

  def fromOrMax: Date =
    from.getOrElse(Date.maxValue)
}
