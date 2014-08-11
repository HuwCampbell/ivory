package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.control._

import scalaz._, Scalaz._, effect.IO

// TODO This needs to be removed once we create the plan api
case class StoreGlob(repository: Repository, store: FeatureStore, globs: List[Prioritized[FactsetGlob]]) {
  def filterPartitions(f: Partition => Boolean): StoreGlob =
    copy(globs = globs.map(_.map(fg => fg.filterPartitions(f))).filter(!_.value.partitions.isEmpty))
}

object StoreGlob {
  def select(repository: Repository, store: FeatureStore): ResultTIO[StoreGlob] =
    store.factsets.traverseU(factset =>
      FactsetGlob.select(repository, factset.factsetId).map(Prioritized(factset.priority, _))
    ).map(globs => StoreGlob(repository, store, globs))

  def before(repository: Repository, store: FeatureStore, to: Date): ResultTIO[StoreGlob] =
    filter(repository, store, _.date.isBeforeOrEqual(to))

  def after(repository: Repository, store: FeatureStore, from: Date): ResultTIO[StoreGlob] =
    filter(repository, store, _.date.isAfterOrEqual(from))

  def between(repository: Repository, store: FeatureStore, from: Date, to: Date): ResultTIO[StoreGlob] =
    filter(repository, store, p => p.date.isBeforeOrEqual(to) && p.date.isAfterOrEqual(from))

  def filter(repository: Repository, store: FeatureStore, f: Partition => Boolean): ResultTIO[StoreGlob] =
    select(repository, store).map(_.filterPartitions(f))
}
