package com.ambiata.ivory.core

import java.util.UUID
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store._
import com.ambiata.poacher.hdfs.{Hdfs => PoacherHdfs, HdfsStore}
import com.ambiata.saws.s3.{S3 => SawsS3, S3Store}

import scalaz.{Store =>_,_}, Scalaz._, effect._

case class TemporaryStore(store: Store[ResultTIO]) {
  def clean: ResultT[IO, Unit] =
    store.deleteAll(Key.Root)
}

case class TemporaryRepository(repo: Repository) {
  def clean: ResultT[IO, Unit] =
    repo.store.deleteAll(Key.Root)
}

case class TemporaryLocationDir(location: IvoryLocation) {
  def clean: ResultTIO[Unit] = IvoryLocation.deleteAll(location)
}

case class TemporaryLocationFile(location: IvoryLocation) {
  def clean: ResultTIO[Unit] = IvoryLocation.delete(location)
}

case class TemporaryCluster(cluster: Cluster) {
  def clean: ResultTIO[Unit] = IvoryLocation.deleteAll(cluster.root)
}

object TemporaryLocations {
  sealed trait TemporaryType
  case object Posix extends TemporaryType
  case object S3    extends TemporaryType
  case object Hdfs  extends TemporaryType

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

  val conf = IvoryConfiguration.Empty

  def testBucket: String = Option(System.getenv("AWS_TEST_BUCKET")).getOrElse("ambiata-dev-view")
  def testBucketDir: DirPath = DirPath.unsafe(testBucket)

  def withIvoryLocationDir[A](storeType: TemporaryType)(f: IvoryLocation => ResultTIO[A]): ResultTIO[A] = {
    runWithIvoryLocationDir(createLocation(storeType))(f)
  }

  def createLocation(storeType: TemporaryType) = {
    val uniquePath = createUniquePath
    storeType match {
      case Posix => IvoryLocation(LocalLocation(uniquePath, new java.net.URI("file:/"+uniquePath.path)), conf)
      case S3    => IvoryLocation(S3Location(testBucketDir </> uniquePath, new java.net.URI("s3:/"+uniquePath.path)), conf)
      case Hdfs  => IvoryLocation(HdfsLocation(uniquePath, new java.net.URI("hdfs:/"+uniquePath.path)), conf)
    }
  }

  def withIvoryLocationFile[A](storeType: TemporaryType)(f: IvoryLocation => ResultTIO[A]): ResultTIO[A] =
    runWithIvoryLocationFile(createLocation(storeType))(f)

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
        LocalRepository(createUniqueLocation)
      case S3 =>
        S3Repository((IvoryLocation.fromDirPath(testBucketDir) </> createUniqueIvoryLocation).location, s3TempPath, conf)
      case Hdfs =>
        HdfsRepository(createUniqueLocation, conf)
    }
    runWithRepository(repo)(f)
  }

  def withCluster[A](f: Cluster => ResultTIO[A]): ResultTIO[A] =
    runWithCluster(Cluster(createUniqueIvoryLocation))(f)

  def runWithRepository[A](repository: Repository)(f: Repository => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryRepository(repository).pure[ResultTIO])(tmp => f(tmp.repo))

  def runWithStore[A](store: Store[ResultTIO])(f: Store[ResultTIO] => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryStore(store).pure[ResultTIO])(tmp => f(tmp.store))

  def runWithIvoryLocationFile[A](location: IvoryLocation)(f: IvoryLocation => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryLocationFile(location).pure[ResultTIO])(tmp => f(tmp.location))

  def runWithIvoryLocationDir[A](location: IvoryLocation)(f: IvoryLocation => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryLocationDir(location).pure[ResultTIO])(tmp => f(tmp.location))

  def runWithCluster[A](cluster: Cluster)(f: Cluster => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryCluster(cluster).pure[ResultTIO])(tmp => f(tmp.cluster))

  def createUniquePath: DirPath =
    DirPath.unsafe(System.getProperty("java.io.tmpdir", "/tmp")) </> tempUniquePath

  def createUniqueIvoryLocation: IvoryLocation =
    IvoryLocation.fromDirPath(createUniquePath).copy(ivory = conf)

  def createUniqueLocation: Location =
    createUniqueIvoryLocation.location

  def createLocationFile(location: IvoryLocation): ResultTIO[Unit] =
    saveLocationFile(location, "")

  def saveLocationFile(location: IvoryLocation, content: String): ResultTIO[Unit] =
    IvoryLocation.writeUtf8(location, content)

  def createLocationDir(location: IvoryLocation): ResultTIO[Unit] = location.location match {
    case LocalLocation(s, _) => Directories.mkdirs(s)
    case S3Location(s, _)    => SawsS3.storeObject(s <|> "file", (s <|> "file").toFile).executeT(conf.s3Client).void
    case HdfsLocation(s, _)  => PoacherHdfs.mkdir(location.toHdfs).void.run(conf.configuration)
  }

  def s3TempPath: DirPath =
    DirPath("tests") </> tempUniquePath

  def tempUniquePath: DirPath =
    DirPath.unsafe(s"temporary-${UUID.randomUUID()}")
}
