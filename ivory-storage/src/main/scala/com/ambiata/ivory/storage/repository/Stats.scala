package com.ambiata.ivory.storage.repository

import com.ambiata.mundane.io.{MemoryConversions, Bytes, BytesQuantity, FilePath}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.poacher.hdfs._

import scalaz.Kleisli._
import scalaz._, Scalaz._
import scalaz.effect.IO
import Hdfs.{totalSize, numberOfFilesRecursively}

/**
 * Computes statistics on ivory repositories.
 *
 * For now only Hdfs repositories are supported
 */
object Stats {
  type StatAction[A] = ReaderT[ResultTIO, StatConfig, A]

  def repositorySize: StatAction[BytesQuantity] =
    (factsetsSize |@| metadataSize |@| snapshotsSize)(Seq(_, _, _).sum)

  def metadataSize: StatAction[BytesQuantity] =
    (dictionariesSize |@| featureStoresSize)(Seq(_, _).sum)

  def factsetSize(factset: FactsetId): StatAction[BytesQuantity] = repository.flatMap {
    case r: HdfsRepository => fromHdfs(totalSize(r.factset(factset).toHdfs))
    case _                 => fail("Unsupported repository!")
  }

  def factsetFiles(factset: FactsetId): StatAction[Int] = repository.flatMap {
    case r: HdfsRepository => fromHdfs(numberOfFilesRecursively(r.factset(factset).toHdfs))
    case _                 => fail("Unsupported repository!")
  }

  def sizeOf(path: Repository => FilePath): StatAction[BytesQuantity] = repository.flatMap {
    case r: HdfsRepository => fromHdfs(totalSize(path(r).toHdfs))
    case _                 => fail("Unsupported repository!")
  }

  def showSizeOfInBytes(path: Repository => FilePath): StatAction[String] =
    sizeOf(path).map(_.show)

  def numberOf(path: Repository => FilePath): StatAction[Int] = repository.flatMap {
    case r: HdfsRepository => fromHdfs(Hdfs.globPaths(path(r).toHdfs).map(_.size))
    case _                 => fail("Unsupported repository!")
  }

  def listOf(path: Repository => FilePath): StatAction[List[String]] = repository.flatMap {
    case r: HdfsRepository => fromHdfs(Hdfs.globPaths(path(r).toHdfs).map(_.map(_.toUri.toString)))
    case _                 => fail("Unsupported repository!")
  }

  def dictionariesSize: StatAction[BytesQuantity]  = sizeOf((_:Repository).dictionaries)
  def dictionaryVersions: StatAction[Int] = numberOf((_:Repository).dictionaries)

  def featureStoresSize: StatAction[BytesQuantity] = sizeOf((_:Repository).featureStores)
  def featureStoreCount: StatAction[Int]  = numberOf((_:Repository).featureStores)

  def factsetsSize: StatAction[BytesQuantity] = sizeOf((_:Repository).factsets)
  def factsetCount: StatAction[Int] = numberOf((_:Repository).factsets)

  def snapshotsSize: StatAction[BytesQuantity] = sizeOf((_:Repository).snapshots)
  def snapshotCount: StatAction[Int]  = numberOf((_:Repository).snapshots)

  /**
   * STAT ACTION methods
   */
  private def repository[A]: StatAction[Repository] =
    (c: StatConfig) => ResultT.ok[IO, Repository](c.repo)

  private def fail[A](message: String): StatAction[A] =
    (c: StatConfig) => ResultT.fail[IO, A](message)

  private def fromHdfs[A](action: Hdfs[A]): StatAction[A] =
    (c: StatConfig) => action.run(c.conf)

  implicit def createKleisli[A](f: StatConfig => ResultTIO[A]): StatAction[A] =
    kleisli[ResultTIO, StatConfig, A](f)

}

case class StatConfig(conf: Configuration, repo: Repository)
