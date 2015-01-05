package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._

import scalaz._, Scalaz._

// TODO remove once plan api is created
case class FactsetGlob(repo: Repository, factset: Factset) {
  def partitions: List[Sized[Partition]] =
    factset.partitions

  // FIX this type doesn't make sense why not empty list of partitions?
  def filterPartitions(f: Partition => Boolean): Option[FactsetGlob] = {
    val filtered = factset.filterByPartition(f)
    if (filtered.partitions.isEmpty) None else Some(copy(factset = filtered))
  }

  def keys: List[Key] =
    partitions.map(p => Repository.factset(factset.id) / p.value.key)
}

object FactsetGlob {
  def select(repository: Repository, factset: FactsetId): RIO[Option[FactsetGlob]] =
    Factsets.factset(repository, factset).map(f => (!f.partitions.isEmpty).option(FactsetGlob(repository, f)))

  def before(repository: Repository, factset: FactsetId, to: Date): RIO[Option[FactsetGlob]] =
    filter(repository, factset, _.date.isBeforeOrEqual(to))

  def after(repository: Repository, factset: FactsetId, from: Date): RIO[Option[FactsetGlob]] =
    filter(repository, factset, _.date.isAfterOrEqual(from))

  def between(repository: Repository, factset: FactsetId, from: Date, to: Date): RIO[Option[FactsetGlob]] =
    filter(repository, factset, p => p.date.isBeforeOrEqual(to) && p.date.isAfterOrEqual(from))

  def filter(repository: Repository, factset: FactsetId, f: Partition => Boolean): RIO[Option[FactsetGlob]] =
    select(repository, factset).map(_.flatMap(_.filterPartitions(f)))
}
