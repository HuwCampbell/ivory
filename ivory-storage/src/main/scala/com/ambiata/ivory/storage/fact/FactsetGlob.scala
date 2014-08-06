package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import scalaz._, Scalaz._, effect.IO, \&/._

// TODO remove once plan api is created
case class FactsetGlob(repo: Repository, factset: FactsetId, version: FactsetVersion, partitions: List[Partition]) {
  def filterPartitions(f: Partition => Boolean): FactsetGlob =
    copy(partitions = partitions.filter(f))

  def paths: List[FilePath] =
    partitions.map(p => repo.factset(factset) </> p.path)
}

object FactsetGlob {
  def select(repository: Repository, factset: FactsetId): ResultT[IO, FactsetGlob] = for {
    v     <- Versions.read(repository, factset)
    paths <- repository.toStore.list(Repository.factset(factset) </> "/*/*/*/*")
    parts <- paths.traverseU(path => ResultT.fromDisjunction[IO, Partition](Partition.parseFile((repository.root </> path)).disjunction.leftMap(This.apply)))
  } yield FactsetGlob(repository, factset, v, parts.distinct)

  def before(repository: Repository, factset: FactsetId, to: Date): ResultT[IO, FactsetGlob] =
    select(repository, factset).map(_.filterPartitions(_.date.isBeforeOrEqual(to)))

  def after(repository: Repository, factset: FactsetId, from: Date): ResultT[IO, FactsetGlob] =
    select(repository, factset).map(_.filterPartitions(_.date.isAfterOrEqual(from)))

  def between(repository: Repository, factset: FactsetId, from: Date, to: Date): ResultT[IO, FactsetGlob] =
    select(repository, factset).map(_.filterPartitions(p => p.date.isBeforeOrEqual(to) && p.date.isAfterOrEqual(from)))
}
