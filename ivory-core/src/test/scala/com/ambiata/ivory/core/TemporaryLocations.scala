package com.ambiata.ivory.core

import java.util.UUID
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.ambiata.notion.core.TemporaryType._
import com.ambiata.poacher.hdfs.{Hdfs => PoacherHdfs}
import com.ambiata.saws.s3.S3Address
import com.ambiata.saws.s3.TemporaryS3._
import org.apache.hadoop.fs.Path

import scalaz.{Store =>_,_}, Scalaz._

trait TemporaryLocations {

  val conf = IvoryConfiguration.Empty

  def withIvoryLocationDir[A](temporaryType: TemporaryType)(f: IvoryLocation => ResultTIO[A]): ResultTIO[A] = {
    val location = createLocation(temporaryType)
    createLocationDir(location) >> runWithIvoryLocationDir(location)(f)
  }

  def createLocation(temporaryType: TemporaryType): IvoryLocation = {
    val uniquePath = createUniquePath.path
    temporaryType match {
      case Posix  => LocalIvoryLocation(LocalLocation(uniquePath))
      case S3     => S3IvoryLocation(S3Location(testBucket, s3TempPath), conf.s3Client)
      case Hdfs   => HdfsIvoryLocation(HdfsLocation(uniquePath), conf.configuration, conf.scoobiConfiguration, conf.codec)
    }
  }

  def withIvoryLocationFile[A](temporaryType: TemporaryType)(f: IvoryLocation => ResultTIO[A]): ResultTIO[A] =
    runWithIvoryLocationFile(createLocation(temporaryType))(f)

  def withCluster[A](f: Cluster => ResultTIO[A]): ResultTIO[A] =
    runWithCluster(Cluster.fromIvoryConfiguration(new Path(createUniquePath.path), conf, 1))(f)

  def runWithRepository[A, R <: Repository](repository: R)(f: R => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryRepository(repository).pure[ResultTIO])(tmp => f(tmp.repo))

  def runWithIvoryLocationFile[A](location: IvoryLocation)(f: IvoryLocation => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryLocationFile(location).pure[ResultTIO])(tmp => f(tmp.location))

  def runWithIvoryLocationDir[A](location: IvoryLocation)(f: IvoryLocation => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryLocationDir(location).pure[ResultTIO])(tmp => f(tmp.location))

  def runWithCluster[A](cluster: Cluster)(f: Cluster => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryCluster(cluster).pure[ResultTIO])(tmp => f(tmp.cluster))

  /** Please use this with care - we need to ensure we _always_ cleanup these files */
  def createUniquePath: DirPath =
    DirPath.unsafe(System.getProperty("java.io.tmpdir", "/tmp")) </> DirPath.unsafe(s"temporary-${UUID.randomUUID()}")

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
    case s @ S3IvoryLocation(S3Location(bucket, key), s3Client)      => (S3Address(bucket, key) / ".location").put("").executeT(s3Client).void
    case h @ HdfsIvoryLocation(HdfsLocation(p), configuration, _, _) => PoacherHdfs.mkdir(new Path(p)).void.run(configuration)
  }
}

object TemporaryLocations extends TemporaryLocations
