package com.ambiata.ivory.core

import scalaz._, Scalaz._

case class Datasets(sets: List[Prioritized[Dataset]]) {
  def byPriority: Datasets =
    copy(sets = sets.sorted)

  def ++(other: Datasets): Datasets =
    copy(sets = sets ++ other.sets)

  def +:(elem: Prioritized[Dataset]): Datasets =
    copy(sets = elem +: sets)

  def add(priority: Priority, dataset: Dataset): Datasets =
    Prioritized(priority, dataset) +: this

  def filter(f: Dataset => Boolean): Datasets =
    copy(sets = sets.filter(e => f(e.value)))

  def prune: Datasets =
    Datasets(sets.filter(p => !p.value.isEmpty))

  def bytes: Bytes =
    sets.foldMap(_.value.bytes)

  def summary: DatasetsSummary  =
    sets.foldMap(p => p.value match {
      case FactsetDataset(factset) =>
        DatasetsSummary(factset.partitions.size, p.value.bytes, None)
      case SnapshotDataset(snapshot) =>
        DatasetsSummary(0, p.value.bytes, snapshot.id.some)
    })
}

object Datasets {
  def empty: Datasets =
    Datasets(Nil)

  implicit def DatasetsEqual: Equal[Datasets] =
    Equal.equalA
}
