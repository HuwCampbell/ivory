package com.ambiata.ivory.storage.repository

import com.ambiata.mundane.io.{MemoryConversions, Bytes, BytesQuantity, FilePath}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.alien.hdfs._

import scalaz.Kleisli._
import scalaz._, Scalaz._, \&/._
import scalaz.effect.IO
import MemoryConversions._
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
    (dictionariesSize |@| storesSize)(Seq(_, _).sum)

  def factsetSize(factset: Factset): StatAction[BytesQuantity] = repository.flatMap {
    case r: HdfsRepository => fromHdfs(totalSize(r.factset(factset).toHdfs))
    case _                 => fail("Unsupported repository!")
  }

  def factsetFiles(factset: Factset): StatAction[Int] = repository.flatMap {
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

  def storesSize: StatAction[BytesQuantity] = sizeOf((_:Repository).stores)
  def storeCount: StatAction[Int]  = numberOf((_:Repository).stores)

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

  private def fromResultTIO[A](res: ResultTIO[A]): StatAction[A] =
    (c: StatConfig) => res

  implicit def createKleisli[A](f: StatConfig => ResultTIO[A]): StatAction[A] =
    kleisli[ResultTIO, StatConfig, A](f)

}

case class StatConfig(conf: Configuration, repo: Repository)