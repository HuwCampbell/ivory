package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.version._
import com.ambiata.mundane.control._

import scalaz._, Scalaz._, effect.IO

object Versions {
  def read(repository: Repository, factset: FactsetId): ResultT[IO, FactsetVersion] =
    Version.read(repository.toReference(Repository.factset(factset))).flatMap(parse(factset, _))

  def write(repository: Repository, factset: FactsetId, version: FactsetVersion): ResultT[IO, Unit] =
    Version.write(repository.toReference(Repository.factset(factset)), Version(version.toString))

  def readAll(repository: Repository, factsets: List[FactsetId]): ResultT[IO, List[(FactsetId, FactsetVersion)]] =
    factsets.traverseU(factset => read(repository, factset).map(factset -> _))

  def writeAll(repository: Repository, factsets: List[FactsetId], version: FactsetVersion): ResultT[IO, Unit] =
    factsets.traverseU(write(repository, _, version)).void

  def readPrioritized(repository: Repository, factsets: List[PrioritizedFactset]): ResultT[IO, List[(PrioritizedFactset, FactsetVersion)]] =
    factsets.traverseU(factset => read(repository, factset.set).map(factset -> _))

  def parse(factset: FactsetId, version: Version): ResultT[IO, FactsetVersion] =
    FactsetVersion.fromString(version.toString) match {
      case None =>
        ResultT.fail(s"Factset version '${version}' in factset '${factset}' not found.")
      case Some(v) =>
        v.pure[ResultTIO]
    }
}
