package com.ambiata.ivory.core

import scalaz._, Scalaz._

case class DatasetsSummary(partitions: Int, bytes: Bytes, snapshot: Option[SnapshotId])

object DatasetsSummary {
  implicit def DatasetsSummaryEqual: Equal[DatasetsSummary] =
    Equal.equalBy(d => (d.partitions, d.bytes, d.snapshot))

  implicit def DatasetsSummaryMonoid: Monoid[DatasetsSummary] =
    Monoid.instance(
      (a, b) => DatasetsSummary(a.partitions + b.partitions, a.bytes + b.bytes, a.snapshot.orElse(b.snapshot))
    , DatasetsSummary(0, Bytes(0), None)
    )
}
