package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.partition._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._

import scalaz._, effect.IO

// TODO remove once plan api is created
case class FactsetGlob(repo: Repository, factset: FactsetId, version: FactsetVersion, partitions: List[Partition]) {
  def filterPartitions(f: Partition => Boolean): Option[FactsetGlob] = {
    val filtered = partitions.filter(f)
    if (filtered.isEmpty) None else Some(copy(partitions = filtered))
  }

  def keys: List[Key] =
    partitions.map(p => Repository.factset(factset) / p.key)
}

object FactsetGlob {
  def select(repository: Repository, factset: FactsetId): ResultT[IO, Option[FactsetGlob]] = for {
    partitions <- Partitions.getFromFactset(repository, factset)
    version    <- if (partitions.isEmpty) ResultT.ok[IO, Option[FactsetVersion]](None) else Versions.read(repository, factset).map(Some.apply)
  } yield version.map(v => FactsetGlob(repository, factset, v, partitions))

  def before(repository: Repository, factset: FactsetId, to: Date): ResultT[IO, Option[FactsetGlob]] =
    filter(repository, factset, _.date.isBeforeOrEqual(to))

  def after(repository: Repository, factset: FactsetId, from: Date): ResultT[IO, Option[FactsetGlob]] =
    filter(repository, factset, _.date.isAfterOrEqual(from))

  def between(repository: Repository, factset: FactsetId, from: Date, to: Date): ResultT[IO, Option[FactsetGlob]] =
    filter(repository, factset, p => p.date.isBeforeOrEqual(to) && p.date.isAfterOrEqual(from))

  def filter(repository: Repository, factset: FactsetId, f: Partition => Boolean): ResultT[IO, Option[FactsetGlob]] =
    select(repository, factset).map(_.flatMap(_.filterPartitions(f)))
}
