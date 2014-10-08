package com.ambiata.ivory.core

import java.io.File

import com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control.{ResultT, ResultTIO}
import com.ambiata.mundane.data.Lists
import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.saws.s3._
import com.nicta.scoobi.core.ScoobiConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodec

import scalaz._, Scalaz._
import scalaz.effect.IO

trait IvoryLocation {
  type SelfType <: IvoryLocation

  def location: Location
  def show: String

  def map(f: DirPath => DirPath): SelfType

  def </>(other: FilePath):      SelfType = map(_ </> other.toDirPath)
  def </>(other: DirPath):       SelfType = map(_ </> other)
  def </>(name: FileName):       SelfType = map(_ </> name)
}

case class HdfsIvoryLocation(location: HdfsLocation, configuration: Configuration, scoobiConfiguration: ScoobiConfiguration, codec: Option[CompressionCodec]) extends IvoryLocation {
  type SelfType = HdfsIvoryLocation

  def show: String = location.path

  def map(f: DirPath => DirPath): SelfType =
    copy(location = location.copy(path = f(location.dirPath).path))

  def toHdfs: String = location.path
  def toHdfsPath: Path = new Path(toHdfs)

  def dirPath  = location.dirPath
  def filePath = location.filePath
}

object HdfsIvoryLocation {
  def create(dir: DirPath, ivory: IvoryConfiguration): HdfsIvoryLocation =
    apply(HdfsLocation(dir.path), ivory)

  def apply(location: HdfsLocation, ivory: IvoryConfiguration): HdfsIvoryLocation =
    new HdfsIvoryLocation(location, ivory.configuration, ivory.scoobiConfiguration, ivory.codec)

  def fromUri(uri: String, ivory: IvoryConfiguration): ResultTIO[HdfsIvoryLocation] =
    IvoryLocation.fromUri(uri, ivory).flatMap {
      case h: HdfsIvoryLocation => ResultT.ok[IO, HdfsIvoryLocation](h)
      case l                    => ResultT.fail[IO, HdfsIvoryLocation](s"${l.show} is not an HDFS location")
    }
}

case class S3IvoryLocation(location: S3Location, @transient s3Client: AmazonS3Client) extends IvoryLocation {
  type SelfType = S3IvoryLocation

  def show: String = location.bucket+"/"+location.key

  def map(f: DirPath => DirPath): SelfType =
    copy(location = location.copy(key = f(DirPath.unsafe(location.key)).path))
}

object S3IvoryLocation {
  def apply(location: S3Location, ivory: IvoryConfiguration): S3IvoryLocation =
    new S3IvoryLocation(location, ivory.s3Client)
}

case class LocalIvoryLocation(location: LocalLocation) extends IvoryLocation {
  type SelfType = LocalIvoryLocation

  def show: String = location.path

  def dirPath  = location.dirPath
  def filePath = location.filePath

  def map(f: DirPath => DirPath): SelfType =
    copy(location = location.copy(path = f(location.dirPath).path))
}

object LocalIvoryLocation {
  def create(dir: DirPath): LocalIvoryLocation =
    LocalIvoryLocation(LocalLocation(dir.path))
}

object IvoryLocation {
  def deleteAll(location: IvoryLocation): ResultTIO[Unit] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path))            => Directories.delete(l.dirPath).void
    case s @ S3IvoryLocation(S3Location(bucket, key), s3Client) => S3.deleteAllx(S3Address(bucket, key)).executeT(s3Client)
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) => Hdfs.deleteAll(new Path(path)).run(conf)
  }

  def delete(location: IvoryLocation): ResultTIO[Unit] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path))            => Files.delete(l.filePath).void
    case s @ S3IvoryLocation(S3Location(bucket, key), s3Client) => S3.deleteObject(S3Address(bucket, key)).executeT(s3Client)
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) => Hdfs.delete(new Path(path)).run(conf)
  }

  def readLines(location: IvoryLocation): ResultTIO[List[String]] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path))            => Files.readLines(l.filePath).map(_.toList)
    case s @ S3IvoryLocation(S3Location(bucket, key), s3Client) => S3.readLines(S3Address(bucket, key)).executeT(s3Client)
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) =>
      Hdfs.isDirectory(new Path(path)).flatMap { isDirectory =>
        if (isDirectory)
          Hdfs.globFilesRecursively(new Path(path)).filterHidden
            .flatMap(_.traverseU(Hdfs.readLines)).map(_.toList.flatten)
        else
          Hdfs.readLines(new Path(path)).map(_.toList)
      }.run(conf)
  }

  def list(location: IvoryLocation): ResultTIO[List[IvoryLocation]] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path)) =>
      Directories.list(l.dirPath).map(fs => fs.map(f => l.copy(location = LocalLocation(f.path))))

    case s @ S3IvoryLocation(S3Location(bucket, key), s3Client) =>
      S3.listKeysx(S3Address(bucket, key)).executeT(s3Client).map(_.map { k =>
        s.copy(location = S3Location(bucket,  k))
      })

    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) =>
      Hdfs.globFilesRecursively(new Path(path)).run(conf).map(_.map { p =>
        h.copy(location = HdfsLocation(p.toString))
      })
  }

  def exists(location: IvoryLocation): ResultTIO[Boolean] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path)) =>
      Files.exists(FilePath.unsafe(path)).flatMap(e => if (e) ResultT.ok[IO, Boolean](e) else Directories.exists(l.dirPath))
    case s @ S3IvoryLocation(S3Location(bucket, key), s3Client) => S3.exists(S3Address(bucket, key)).executeT(s3Client)
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) => Hdfs.exists(new Path(path)).run(conf)
  }

  def writeUtf8Lines(location: IvoryLocation, lines: List[String]): ResultTIO[Unit] = 
    writeUtf8(location, Lists.prepareForFile(lines))
  
  def writeUtf8(location: IvoryLocation, string: String): ResultTIO[Unit] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path))            => Files.write(l.filePath, string)
    case s @ S3IvoryLocation(S3Location(bucket, key), s3Client) => S3.writeLines(S3Address(bucket, key), string.split("\n")).executeT(s3Client).void
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) => Hdfs.writeWith(new Path(path), out => Streams.write(out, string)).run(conf)
  }

  def isDirectory(location: IvoryLocation): ResultTIO[Boolean] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path))            => ResultT.safe[IO, Boolean](new File(path).isDirectory)
    case s @ S3IvoryLocation(S3Location(bucket, key), s3Client) => S3.listSummaryx(S3Address(bucket, key + "/")).map(_.nonEmpty).executeT(s3Client)
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) => Hdfs.isDirectory(new Path(path)).run(conf)
  }

  def fromKey(repository: Repository, key: Key): IvoryLocation =
    repository.toIvoryLocation(key)

  def fromUri(s: String, ivory: IvoryConfiguration): ResultTIO[IvoryLocation] =
    ResultT.fromDisjunctionString[IO, IvoryLocation](parseUri(s, ivory))

  def fromDirPath(dirPath: DirPath): LocalIvoryLocation =
    LocalIvoryLocation(LocalLocation(dirPath.path))

  def fromFilePath(filePath: FilePath): LocalIvoryLocation =
    LocalIvoryLocation(LocalLocation(filePath.path))

  def parseUri(s: String, ivory: IvoryConfiguration): String \/ IvoryLocation = try {
    val uri = new java.net.URI(s)
    uri.getScheme match {
      case null =>
        // TODO Should be LocalLocation but our own consumers aren't ready yet
        // https://github.com/ambiata/ivory/issues/87
        HdfsIvoryLocation(HdfsLocation(new File(uri.getPath).getAbsolutePath), ivory.configuration, ivory.scoobiConfiguration, ivory.codec).right

      case _ => Location.fromUri(s).map {
        case l: LocalLocation  => LocalIvoryLocation(l)
        case s: S3Location     => S3IvoryLocation(s, ivory.s3Client)
        case h: HdfsLocation   => HdfsIvoryLocation(h, ivory.configuration, ivory.scoobiConfiguration, ivory.codec)
      }
    }
  } catch {
    case e: java.net.URISyntaxException =>
      e.getMessage.left
  }
}
