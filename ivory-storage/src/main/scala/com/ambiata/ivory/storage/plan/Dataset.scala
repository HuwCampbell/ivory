package com.ambiata.ivory.storage.plan

import com.ambiata.ivory.core._

sealed trait Dataset

case class FactsetDataset(factset: FactsetId, partitions: List[Partition]) extends Dataset {
  def partitionsBefore(date: Date): FactsetDataset =
    copy(partitions = partitions.filter(_.date.isBefore(date)))

  def partitionsBeforeOrEqual(date: Date): FactsetDataset =
    copy(partitions = partitions.filter(_.date.isBeforeOrEqual(date)))

  def partitionsAfter(date: Date): FactsetDataset =
    copy(partitions = partitions.filter(_.date.isAfter(date)))

  def partitionsAfterOrEqual(date: Date): FactsetDataset =
    copy(partitions = partitions.filter(_.date.isAfterOrEqual(date)))

  /** TODO we need to choose either inclusive or exclusive and be consistent across the entire code base */
  def partitionsBetweenInclusive(start: Date, end: Date): FactsetDataset =
    copy(partitions = partitions.filter(d => d.date.isAfterOrEqual(start) && d.date.isBeforeOrEqual(end)))

  def partitionsBetweenExclusive(start: Date, end: Date): FactsetDataset =
    copy(partitions = partitions.filter(d => d.date.isAfter(start) && d.date.isBefore(end)))

  def filter(f: Partition => Boolean): FactsetDataset =
    copy(partitions = partitions.filter(f))
}

case class SnapshotDataset(snapshot: Identifier) extends Dataset
