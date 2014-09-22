package com.ambiata.ivory.core

import java.util.UUID

import com.ambiata.ivory.core.IvorySyntax._
import com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store._
import com.ambiata.poacher.hdfs.{Hdfs => PoacherHdfs, HdfsStore}
import com.ambiata.saws.s3.{S3 => SawsS3, S3Path, S3Store}
import com.nicta.scoobi.impl.ScoobiConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scalaz.{Store =>_,_}, Scalaz._, effect._

case class TemporaryReference(ref: ReferenceIO) {
  def clean: ResultT[IO, Unit] =
    ref.store.deleteAll(Key.Root)
}

case class TemporaryStore(store: Store[ResultTIO]) {
  def clean: ResultT[IO, Unit] =
    store.deleteAll(Key.Root)
}

case class TemporaryRepository(repo: Repository) {
  def clean: ResultT[IO, Unit] =
    repo.store.deleteAll(Key.Root)
}

case class TemporaryLocationDir(location: Location, s3client: AmazonS3Client, conf: Configuration) {
  def clean: ResultTIO[Unit] = location match {
    case HdfsLocation(path)      => PoacherHdfs.deleteAll(path.toHdfs).run(conf)
    case LocalLocation(path)     => Directories.delete(path).void
    case S3Location(path)        => SawsS3.deleteAll(path).executeT(s3client)
  }
}

case class TemporaryLocationFile(location: Location, s3client: AmazonS3Client, conf: Configuration) {
  def clean: ResultTIO[Unit] = location match {
    case HdfsLocation(path)      => PoacherHdfs.delete(path.toHdfs).run(conf)
    case LocalLocation(path)     => Files.delete(path.toFilePath).void
    case S3Location(path)        => SawsS3.deleteAll(path).executeT(s3client)
  }
}

case class TemporaryCluster(cluster: Cluster) {
  def clean: ResultTIO[Unit] =
    PoacherHdfs.deleteAll(cluster.root.toHdfs).run(cluster.hdfsConfiguration)
}

object TemporaryReferences {
  sealed trait TemporaryType
  case object Posix extends TemporaryType
  case object S3    extends TemporaryType
  case object Hdfs  extends TemporaryType

  implicit val TemporaryReferenceResource: Resource[TemporaryReference] = new Resource[TemporaryReference] {
    def close(temp: TemporaryReference) = temp.clean.run.void // Squelch errors
  }

  implicit val TemporaryStoreResource: Resource[TemporaryStore] = new Resource[TemporaryStore] {
    def close(temp: TemporaryStore) = temp.clean.run.void // Squelch errors
  }

  implicit val TemporaryRepositoryResource: Resource[TemporaryRepository] = new Resource[TemporaryRepository] {
    def close(temp: TemporaryRepository) = temp.clean.run.void // Squelch errors
  }

  implicit val TemporaryLocationDirResource: Resource[TemporaryLocationDir] = new Resource[TemporaryLocationDir] {
    def close(temp: TemporaryLocationDir) = temp.clean.run.void // Squelch errors
  }

  implicit val TemporaryLocationFileResource: Resource[TemporaryLocationFile] = new Resource[TemporaryLocationFile] {
    def close(temp: TemporaryLocationFile) = temp.clean.run.void // Squelch errors
  }

  implicit val TemporaryClusterResource: Resource[TemporaryCluster] = new Resource[TemporaryCluster] {
    def close(temp: TemporaryCluster) = temp.clean.run.void // Squelch errors
  }

  val conf = IvoryConfiguration.fromScoobiConfiguration(ScoobiConfiguration())

  def testBucket: String = Option(System.getenv("AWS_TEST_BUCKET")).getOrElse("ambiata-dev-view")
  def testBucketDir: DirPath = DirPath.unsafe(testBucket)

  def withReferenceFile[A](storeType: TemporaryType)(f: ReferenceIO => ResultTIO[A]): ResultTIO[A] = {
    val reference = storeType match {
      case Posix =>
        Reference(PosixStore(createUniquePath), FilePath("temp-file"))
      case S3    =>
        Reference(S3Store(testBucket, s3TempPath, conf.s3Client, conf.s3TmpDirectory), FilePath("temp-file"))
      case Hdfs  =>
        Reference(HdfsStore(conf.configuration, createUniquePath), FilePath("temp-file"))
    }
    runWithReference(reference)(f)
  }

  def withStore[A](storeType: TemporaryType)(f: Store[ResultTIO] => ResultTIO[A]): ResultTIO[A] = {
    val store = storeType match {
      case Posix =>
        PosixStore(createUniquePath)
      case S3    =>
        S3Store(testBucket, s3TempPath, conf.s3Client, conf.s3TmpDirectory)
      case Hdfs  =>
        HdfsStore(conf.configuration, createUniquePath)
    }
    runWithStore(store)(f)
  }

  def withRepository[A](storeType: TemporaryType)(f: Repository => ResultTIO[A]): ResultTIO[A] = {
    val repo = storeType match {
      case Posix =>
        LocalRepository(createUniquePath)
      case S3 =>
        S3Repository(testBucket, s3TempPath, conf)
      case Hdfs =>
        HdfsRepository(createUniquePath, conf)
    }
    runWithRepository(repo)(f)
  }

  def withLocationFile[A](storeType: TemporaryType)(f: Location => ResultTIO[A]): ResultTIO[A] = {
    val location = storeType match {
      case Posix =>
        LocalLocation(createUniquePath)
      case S3    =>
        S3Location(testBucketDir </> s3TempPath)
      case Hdfs  =>
        HdfsLocation(createUniquePath)
    }
    runWithLocationFile(location)(f)
  }

  def withLocationDir[A](storeType: TemporaryType)(f: Location => ResultTIO[A]): ResultTIO[A] = {
    val location = storeType match {
      case Posix =>
        LocalLocation(createUniquePath)
      case S3    =>
        S3Location(testBucketDir </> s3TempPath)
      case Hdfs  =>
        HdfsLocation(createUniquePath)
    }
    runWithLocationDir(location)(f)
  }

  def withCluster[A](f: Cluster => ResultTIO[A]): ResultTIO[A] =
    runWithCluster(Cluster(createUniquePath, conf))(f)

  def runWithRepository[A](repository: Repository)(f: Repository => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryRepository(repository).pure[ResultTIO])(tmp => f(tmp.repo))

  def runWithStore[A](store: Store[ResultTIO])(f: Store[ResultTIO] => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryStore(store).pure[ResultTIO])(tmp => f(tmp.store))

  def runWithReference[A](reference: ReferenceIO)(f: ReferenceIO => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryReference(reference).pure[ResultTIO])(tmp => f(tmp.ref))

  def runWithLocationFile[A](location: Location)(f: Location => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryLocationFile(location, conf.s3Client, conf.configuration).pure[ResultTIO])(tmp => f(tmp.location))

  def runWithLocationDir[A](location: Location)(f: Location => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryLocationDir(location, conf.s3Client, conf.configuration).pure[ResultTIO])(tmp => f(tmp.location))

  def runWithCluster[A](cluster: Cluster)(f: Cluster => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryCluster(cluster).pure[ResultTIO])(tmp => f(tmp.cluster))

  def createUniquePath: DirPath =
    DirPath.unsafe(System.getProperty("java.io.tmpdir", "/tmp")) </> tempUniquePath

  def createLocationFile(location: Location): ResultTIO[Unit] = location match {
    case LocalLocation(s) => Files.write(s.toFilePath, "")
    case S3Location(s)    => SawsS3.storeObject(s.toFilePath, s.toFile).executeT(conf.s3Client).void
    case HdfsLocation(s)  => PoacherHdfs.writeWith(s.toHdfs, out => Streams.write(out, "")).run(conf.configuration)
  }

  def createLocationDir(location: Location): ResultTIO[Unit] = location match {
    case LocalLocation(s) => Directories.mkdirs(s)
    case S3Location(s)    => SawsS3.storeObject(s <|> "file", (s <|> "file").toFile).executeT(conf.s3Client).void
    case HdfsLocation(s)  => PoacherHdfs.mkdir(s.toHdfs).void.run(conf.configuration)
  }

  def s3TempPath: DirPath =
    DirPath("tests") </> tempUniquePath

  def tempUniquePath: DirPath =
    DirPath.unsafe(s"temporary-${UUID.randomUUID()}")
}
