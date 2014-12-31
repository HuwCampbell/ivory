package com.ambiata.ivory.core

import scalaz._, Scalaz._

case class FeatureRanges(features: List[FeatureRange]) {
  /** Determine if this set of features is fully contained by the
      specified set of ranges, i.e. for each of the features
      does it include data _at or before_ the required date for that
      namespace.*/
  def containedBy(ranges: FeatureRanges): Boolean = {
    val indexed = ranges.features.groupBy(_.id)
    features.forall(required => indexed.get(required.id).exists(_.map(_.fromOrMin).headOption.getOrElse(Date.minValue) <= required.fromOrMin))
  }

  /** Decrease granularity to only care about namespace level
      ranges. */
  def toNamespaces: NamespaceRanges =
    NamespaceRanges(features.groupBy1(_.id.namespace).toList.map({
      case (namespace, ranges) =>
        NamespaceRange(namespace, ranges.toList.flatMap(_.froms), ranges.head.to)
    }))
}
