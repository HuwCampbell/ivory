package com.ambiata.ivory.core

import scalaz._, Scalaz._

case class Ranges[A](values: List[Range[A]]) {
  /** Determine if this set of features is fully contained by the
      specified set of ranges, i.e. for each of the features
      does it include data _at or before_ the required date for that
      namespace.*/
  def containedBy(ranges: Ranges[A]): Boolean = {
    val indexed = ranges.values.groupBy(_.id)
    values.forall(required => indexed.get(required.id).exists(rs =>
      rs.map(_.fromOrMin).headOption.getOrElse(Date.minValue) <= required.fromOrMin))
  }
}

object Ranges {
  implicit def RangesEqual[A: Equal]: Equal[Ranges[A]] =
    Equal.equalBy(_.values)

  /** Decrease granularity to only care about namespace level ranges. */
  def toNamespaces(ranges: Ranges[FeatureId]): Ranges[Namespace] =
    Ranges(ranges.values.groupBy1(r => r.id.namespace -> r.to).toList.map({
      case ((namespace, to), ranges) =>
        Range(namespace, ranges.toList.flatMap(_.froms), to)
    }))
}
