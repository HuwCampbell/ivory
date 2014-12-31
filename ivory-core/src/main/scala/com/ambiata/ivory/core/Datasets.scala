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

  def bytes: Long =
    sets.map(_.value.bytes).sum

  def summary: DatasetsSummary  =
    sets.foldLeft(DatasetsSummary(0, 0, None))((acc, p) => p.value match {
      case FactsetDataset(factset) =>
        acc.copy(partitions = factset.partitions.size + acc.partitions, bytes = factset.bytes + acc.bytes)
      case SnapshotDataset(snapshot) =>
        acc.copy(bytes = snapshot.bytes + acc.bytes, snapshot = snapshot.id.some)
    })
}

object Datasets {
  def empty: Datasets =
    Datasets(Nil)
}

case class DatasetsSummary(partitions: Int, bytes: Long, snapshot: Option[SnapshotId])
