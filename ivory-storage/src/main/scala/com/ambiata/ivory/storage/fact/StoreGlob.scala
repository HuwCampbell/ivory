package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.mundane.control._

import scalaz._, Scalaz._

// TODO This needs to be removed once we create the plan api
case class FeatureStoreGlob(repository: Repository, store: FeatureStore, globs: List[Prioritized[FactsetGlob]]) {
  def filterPartitions(f: Partition => Boolean): FeatureStoreGlob =
    copy(globs = globs.map(_.map(fg => fg.filterPartitions(f))).collect({ case Prioritized(p, Some(fg)) => Prioritized(p, fg) }))

  def partitions: List[Partition] =
    globs.flatMap(_.value.partitions.partitions)
}

object FeatureStoreGlob {
  def select(repository: Repository, store: FeatureStore): ResultTIO[FeatureStoreGlob] =
    store.factsetIds.traverseU(factset =>
      FactsetGlob.select(repository, factset.value).map(_.map(Prioritized(factset.priority, _)))
    ).map(globs => FeatureStoreGlob(repository, store, globs.flatten))

  def before(repository: Repository, store: FeatureStore, to: Date): ResultTIO[FeatureStoreGlob] =
    filter(repository, store, _.date.isBeforeOrEqual(to))

  def after(repository: Repository, store: FeatureStore, from: Date): ResultTIO[FeatureStoreGlob] =
    filter(repository, store, _.date.isAfterOrEqual(from))

  def between(repository: Repository, store: FeatureStore, from: Date, to: Date): ResultTIO[FeatureStoreGlob] =
    filter(repository, store, p => p.date.isBeforeOrEqual(to) && p.date.isAfterOrEqual(from))

  def strictlyBetween(repository: Repository, store: FeatureStore, from: Date, to: Date): ResultTIO[FeatureStoreGlob] =
    filter(repository, store, p => p.date.isBefore(to) && p.date.isAfter(from))

  def afterAndStrictlyBefore(repository: Repository, store: FeatureStore, from: Date, to: Date): ResultTIO[FeatureStoreGlob] =
    filter(repository, store, p => p.date.isBefore(to) && p.date.isAfterOrEqual(from))

  def strictlyAfterAndBefore(repository: Repository, store: FeatureStore, from: Date, to: Date): ResultTIO[FeatureStoreGlob] =
    filter(repository, store, p => p.date.isBeforeOrEqual(to) && p.date.isAfter(from))

  def filter(repository: Repository, store: FeatureStore, f: Partition => Boolean): ResultTIO[FeatureStoreGlob] =
    select(repository, store).map(_.filterPartitions(f))
}
