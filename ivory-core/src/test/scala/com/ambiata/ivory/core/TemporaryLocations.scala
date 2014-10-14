package com.ambiata.ivory.core

import java.io.File
import java.util.UUID
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.{Location => _, HdfsLocation => _, S3Location => _, LocalLocation => _, _}
import com.ambiata.notion.core._
import com.ambiata.poacher.hdfs.{Hdfs => PoacherHdfs}
import com.ambiata.saws.s3.{S3 => SawsS3, S3Address}
import org.apache.hadoop.fs.Path

import scalaz.{Store =>_,_}, Scalaz._, effect._

case class TemporaryStore(store: Store[ResultTIO]) {
  def clean: ResultT[IO, Unit] =
    store.deleteAll(Key.Root)
}

case class TemporaryRepository[R <: Repository](repo: R) {
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

  implicit def TemporaryRepositoryResource[R <: Repository]: Resource[TemporaryRepository[R]] = new Resource[TemporaryRepository[R]] {
    def close(temp: TemporaryRepository[R]) = temp.clean.run.void // Squelch errors
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

  def createLocation(storeType: TemporaryType): IvoryLocation = {
    val uniquePath = createUniquePath.path
    storeType match {
      case Posix  => LocalIvoryLocation(LocalLocation(uniquePath))
      case S3     => S3IvoryLocation(S3Location(testBucket, uniquePath), conf.s3Client)
      case Hdfs   => HdfsIvoryLocation(HdfsLocation(uniquePath), conf.configuration, conf.scoobiConfiguration, conf.codec)
    }
  }

  def withIvoryLocationFile[A](storeType: TemporaryType)(f: IvoryLocation => ResultTIO[A]): ResultTIO[A] =
    runWithIvoryLocationFile(createLocation(storeType))(f)

  def withStore[A](storeType: TemporaryType)(f: Store[ResultTIO] => ResultTIO[A]): ResultTIO[A] = {
    val store = storeType match {
      case Posix =>
        PosixStore(createUniquePath)
      case S3    =>
        S3Store(testBucket, s3TempPathDir, conf.s3Client, conf.s3TmpDirectory)
      case Hdfs  =>
        HdfsStore(conf.configuration, createUniquePath)
    }
    runWithStore(store)(f)
  }

  def withRepository[A](storeType: TemporaryType)(f: Repository => ResultTIO[A]): ResultTIO[A] = {
    val repo = storeType match {
      case Posix =>
        LocalRepository(createUniqueLocalLocation)
      case S3 =>
        S3Repository(createUniqueS3Location, conf.s3TmpDirectory)
      case Hdfs =>
        HdfsRepository(createUniqueHdfsLocation)
    }
    runWithRepository(repo)(f)
  }

  def withHdfsRepository[A](f: HdfsRepository => ResultTIO[A]): ResultTIO[A] = {
    runWithRepository(HdfsRepository(createUniqueHdfsLocation))(f)
  }

  def withCluster[A](f: Cluster => ResultTIO[A]): ResultTIO[A] =
    runWithCluster(Cluster(createUniqueIvoryLocation))(f)

  def runWithRepository[A, R <: Repository](repository: R)(f: R => ResultTIO[A]): ResultTIO[A] =
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


  def createUniqueIvoryLocation = createUniqueHdfsLocation

  def createUniqueLocalLocation: LocalIvoryLocation = {
    val path = createUniquePath.path
    LocalIvoryLocation(LocalLocation(path))
  }

  def createUniqueS3Location: S3IvoryLocation = {
    val path = createUniquePath.asRelative.path
    S3IvoryLocation(S3Location(testBucket, path), conf.s3Client)
  }

  def createUniqueHdfsLocation: HdfsIvoryLocation = {
    val path = createUniquePath.path
    HdfsIvoryLocation(HdfsLocation(path), conf.configuration, conf.scoobiConfiguration, conf.codec)
  }

  def createLocationFile(location: IvoryLocation): ResultTIO[Unit] =
    saveLocationFile(location, "")

  def saveLocationFile(location: IvoryLocation, content: String): ResultTIO[Unit] =
    IvoryLocation.writeUtf8(location, content)

  def createLocationDir(location: IvoryLocation): ResultTIO[Unit] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path))                 => Directories.mkdirs(DirPath.unsafe(path))
    case s @ S3IvoryLocation(S3Location(bucket, key), s3Client)      => SawsS3.storeObject(S3Address(bucket, key+"/file"), new File(key+"/file")).executeT(s3Client).void
    case h @ HdfsIvoryLocation(HdfsLocation(p), configuration, _, _) => PoacherHdfs.mkdir(new Path(p)).void.run(configuration)
  }

  def s3TempPath: String =
    s3TempPathDir.path

  def s3TempPathDir: DirPath=
    DirPath("tests") </> tempUniquePath

  def tempUniquePath: DirPath =
    DirPath.unsafe(s"temporary-${UUID.randomUUID()}")
}
