package com.ambiata.ivory.storage

import java.util.UUID

import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store.{PosixStore, Store}
import com.ambiata.poacher.hdfs.HdfsStore
import com.ambiata.saws.s3.S3Store
import com.nicta.scoobi.impl.ScoobiConfiguration

import scalaz.Scalaz._
import scalaz.effect.{IO, Resource}

case class TemporaryReference(ref: ReferenceIO) {
  def clean: ResultT[IO, Unit] =
    ref.store.deleteAll(ref.path)
}

case class TemporaryStore(store: Store[ResultTIO]) {
  def clean: ResultT[IO, Unit] =
    store.deleteAll(FilePath.root)
}

case class TemporaryRepository(repo: Repository) {
  def clean: ResultT[IO, Unit] =
    repo.toStore.deleteAll(Repository.root)
}

object TemporaryReferences {
  sealed trait TemporaryType
  case object Posix extends TemporaryType
  case object S3    extends TemporaryType
  case object Hdfs  extends TemporaryType

  implicit val TemporaryReferenceResource = new Resource[TemporaryReference] {
    def close(temp: TemporaryReference) = temp.clean.run.void // Squelch errors
  }

  implicit val TemporaryStoreResource = new Resource[TemporaryStore] {
    def close(temp: TemporaryStore) = temp.clean.run.void // Squelch errors
  }

  implicit val TemporaryRepositoryResource = new Resource[TemporaryRepository] {
    def close(temp: TemporaryRepository) = temp.clean.run.void // Squelch errors
  }

  val conf = IvoryConfiguration(ScoobiConfiguration())

  def withReferenceFile[A](storeType: TemporaryType)(f: ReferenceIO => ResultTIO[A]): ResultTIO[A] = {
    val reference = storeType match {
      case Posix =>
        Reference(PosixStore(createLocalTempDirectory), FilePath("temp-file"))
      case S3    =>
        Reference(S3Store("ambiata-dev-view", s3TempPath, conf.s3Client, conf.s3TmpDirectory), FilePath("temp-file"))
      case Hdfs  =>
        Reference(HdfsStore(conf.configuration, createLocalTempDirectory), FilePath("temp-file"))
    }
    runWithReference(reference)(f)
  }

  def withStore[A](storeType: TemporaryType)(f: Store[ResultTIO] => ResultTIO[A]): ResultTIO[A] = {
    val store = storeType match {
      case Posix =>
        PosixStore(createLocalTempDirectory)
      case S3    =>
        S3Store("ambiata-dev-view", s3TempPath, conf.s3Client, conf.s3TmpDirectory)
      case Hdfs  =>
        HdfsStore(conf.configuration, createLocalTempDirectory)
    }
    runWithStore(store)(f)
  }

  def withRepository[A](storeType: TemporaryType)(f: Repository => ResultTIO[A]): ResultTIO[A] = {
    val repo = storeType match {
      case Posix =>
        LocalRepository(createLocalTempDirectory)
      case S3 =>
        S3Repository("ambiata-dev-view", s3TempPath, conf)
      case Hdfs =>
        HdfsRepository(createLocalTempDirectory, conf)
    }
    runWithRepository(repo)(f)
  }

  def runWithRepository[A](repository: Repository)(f: Repository => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryRepository(repository).pure[ResultTIO])(tmp => f(tmp.repo))

  def runWithStore[A](store: Store[ResultTIO])(f: Store[ResultTIO] => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryStore(store).pure[ResultTIO])(tmp => f(tmp.store))

  def runWithReference[A](reference: ReferenceIO)(f: ReferenceIO => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryReference(reference).pure[ResultTIO])(tmp => f(tmp.ref))

  def createLocalTempDirectory: FilePath = {
    val path = FilePath(System.getProperty("java.io.tmpdir", "/tmp")) </> tempUniquePath
    Directories.mkdirs(path)
    path
  }

  def s3TempPath: FilePath =
    FilePath("tests") </> tempUniquePath

  def tempUniquePath: String =
    s"temporary-${UUID.randomUUID()}"
}
