package com.ambiata.ivory.storage.fact

import scalaz._, Scalaz._, effect.IO
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.parse._
import com.ambiata.saws.s3.S3
import com.ambiata.saws.core._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.alien.hdfs.HdfsS3Action
import com.ambiata.ivory.alien.hdfs.HdfsS3Action._

object Versions {
  val VersionFile = ".version"

  def read(repository: Repository, factsetId: String): ResultT[IO, FactsetVersion] =
    repository.toStore.utf8.read(Repository.factsetById(factsetId) </> VersionFile).flatMap(parse(factsetId, _))

  def write(repository: Repository, factsetId: String, version: FactsetVersion): ResultT[IO, Unit] =
    repository.toStore.utf8.write(Repository.factsetById(factsetId) </> VersionFile, version.toString)

  def readAll(repository: Repository, factsetIds: List[String]): ResultT[IO, List[(String, FactsetVersion)]] =
    factsetIds.traverseU(factsetId => read(repository, factsetId).map(factsetId -> _))

  def writeAll(repository: Repository, factsetIds: List[String], version: FactsetVersion): ResultT[IO, Unit] =
    factsetIds.traverseU(write(repository, _, version)).void

  def parse(factsetId: String, version: String): ResultT[IO, FactsetVersion] =
    FactsetVersion.fromString(version.trim) match {
      case None =>
        ResultT.fail(s"Factset version '${version}' in factset '${factsetId}' not found.")
      case Some(v) =>
        v.pure[ResultTIO]
    }
}
