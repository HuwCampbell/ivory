package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import scalaz._, Scalaz._, effect.IO, \&/._

// TODO remove once plan api is created
case class FactsetGlob(repo: Repository, factset: FactsetId, version: FactsetVersion, partitions: List[Partition]) {
  def filterPartitions(f: Partition => Boolean): Option[FactsetGlob] = {
    val filtered = partitions.filter(f)
    if(filtered.isEmpty) None else Some(copy(partitions = filtered))
  }

  def paths: List[FilePath] =
    partitions.map(p => repo.factset(factset) </> p.path)
}

object FactsetGlob {
  def select(repository: Repository, factset: FactsetId): ResultT[IO, Option[FactsetGlob]] = for {
    paths   <- repository.toStore.list(Repository.factset(factset) </> "/*/*/*/*")
    parts   <- paths.traverseU(path => ResultT.fromDisjunction[IO, Partition](Partition.parseFile((repository.root </> path)).disjunction.leftMap(This.apply)))
    version <- if(parts.isEmpty) ResultT.ok[IO, Option[FactsetVersion]](None) else Versions.read(repository, factset).map(Some.apply)
  } yield version.map(v => FactsetGlob(repository, factset, v, parts.distinct))

  def before(repository: Repository, factset: FactsetId, to: Date): ResultT[IO, Option[FactsetGlob]] =
    filter(repository, factset, _.date.isBeforeOrEqual(to))

  def after(repository: Repository, factset: FactsetId, from: Date): ResultT[IO, Option[FactsetGlob]] =
    filter(repository, factset, _.date.isAfterOrEqual(from))

  def between(repository: Repository, factset: FactsetId, from: Date, to: Date): ResultT[IO, Option[FactsetGlob]] =
    filter(repository, factset, p => p.date.isBeforeOrEqual(to) && p.date.isAfterOrEqual(from))

  def filter(repository: Repository, factset: FactsetId, f: Partition => Boolean): ResultT[IO, Option[FactsetGlob]] =
    select(repository, factset).map(_.flatMap(_.filterPartitions(f)))
}
