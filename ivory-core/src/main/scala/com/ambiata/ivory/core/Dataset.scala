package com.ambiata.ivory.core

import scalaz._, Scalaz._

sealed trait Dataset {
  def fold[X](
    factset: Factset => X
  , snapshot: Snapshot => X
  ): X = this match {
    case FactsetDataset(f) => factset(f)
    case SnapshotDataset(s) => snapshot(s)
  }

  def toFactset: Option[Factset] =
    fold(_.some, _ => none)

  def toSnapshot: Option[Snapshot] =
    fold(_ => none, _.some)

  def isFactset: Boolean =
    fold(_ => true, _ => false)

  def isSnapshot: Boolean =
    fold(_ => false, _ => true)

  def isEmpty: Boolean =
    fold(_.partitions.isEmpty, _ => false)

  def bytes: Bytes =
    fold(_.bytes, _.bytes)
}

case class FactsetDataset(factset: Factset) extends Dataset
case class SnapshotDataset(snapshot: Snapshot) extends Dataset

object Dataset {
  def factset(factset: Factset): Dataset =
    FactsetDataset(factset)

  def snapshot(snapshot: Snapshot): Dataset =
    SnapshotDataset(snapshot)

  def prioritizedSnapshot(snapshot: Snapshot): Prioritized[Dataset] =
    Prioritized(Priority.Max, SnapshotDataset(snapshot))

  def within(features: FeatureStore, after: Date, to: Date): List[Prioritized[Dataset]] =
    features.filterByDate(date => date > after && date <= to).toDataset

  def to(features: FeatureStore, to: Date): List[Prioritized[Dataset]] =
    features.filterByDate(_ <= to).toDataset

  implicit def DatasetEqual: Equal[Dataset] =
    Equal.equalA
}
