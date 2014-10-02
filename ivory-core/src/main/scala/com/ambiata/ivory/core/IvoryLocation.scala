package com.ambiata.ivory.core

import java.io.File

import com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control.{ResultT, ResultTIO}
import com.ambiata.mundane.io._
import com.ambiata.mundane.store.Key
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.saws.s3.S3
import com.nicta.scoobi.core.ScoobiConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodec

import scalaz._, Scalaz._
import scalaz.effect.IO

trait IvoryLocation {
  type SelfType <: IvoryLocation

  def location: Location
  def show = location.path.path

  def map(f: DirPath => DirPath): SelfType

  def </>(other: FilePath):      SelfType = map(_ </> other.toDirPath)
  def </>(other: DirPath):       SelfType = map(_ </> other)
  def </>(name: FileName):       SelfType = map(_ </> name)
}

case class HdfsIvoryLocation(location: HdfsLocation, configuration: Configuration, scoobiConfiguration: ScoobiConfiguration, codec: Option[CompressionCodec]) extends IvoryLocation {
  type SelfType = HdfsIvoryLocation

  def map(f: DirPath => DirPath): SelfType =
    copy(location = location.copy(path = f(location.path)))

  def toHdfs: String = location.path.path
  def toHdfsPath: Path = new Path(toHdfs)
}

object HdfsIvoryLocation {
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

  def map(f: DirPath => DirPath): SelfType =
    copy(location = location.copy(path = f(location.path)))
}

object S3IvoryLocation {
  def apply(location: S3Location, ivory: IvoryConfiguration): S3IvoryLocation =
    new S3IvoryLocation(location, ivory.s3Client)
}

case class LocalIvoryLocation(location: LocalLocation) extends IvoryLocation {
  type SelfType = LocalIvoryLocation

  def map(f: DirPath => DirPath): SelfType =
    copy(location = location.copy(path = f(location.path)))
}

object IvoryLocation {
  def deleteAll(location: IvoryLocation): ResultTIO[Unit] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path))            => Directories.delete(path).void
    case s @ S3IvoryLocation(S3Location(path), s3Client)        => S3.deleteAll(path).executeT(s3Client)
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) => Hdfs.deleteAll(h.toHdfsPath).run(conf)
  }

  def delete(location: IvoryLocation): ResultTIO[Unit] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path))            => Files.delete(path.toFilePath).void
    case s @ S3IvoryLocation(S3Location(path), s3Client)        => S3.deleteObject(path.toFilePath).executeT(s3Client)
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) => Hdfs.delete(h.toHdfsPath).run(conf)
  }

  def readLines(location: IvoryLocation): ResultTIO[List[String]] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path))            => Files.readLines(path.toFilePath).map(_.toList)
    case s @ S3IvoryLocation(S3Location(path), s3Client)        => S3.readLines(path.toFilePath).executeT(s3Client)
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) =>
      Hdfs.isDirectory(h.toHdfsPath).flatMap { isDirectory =>
        if (isDirectory)
          Hdfs.globFilesRecursively(h.toHdfsPath).filterHidden
            .flatMap(_.traverseU(Hdfs.readLines)).map(_.toList.flatten)
        else
          Hdfs.readLines(h.toHdfsPath).map(_.toList)
      }.run(conf)
  }

  def list(location: IvoryLocation): ResultTIO[List[FilePath]] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path))            => Directories.list(path)
    case s @ S3IvoryLocation(S3Location(path), s3Client)        => S3.listKeys(path).map(_.map(FilePath.unsafe)).executeT(s3Client)
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) => Hdfs.globFilesRecursively(h.toHdfsPath).map(_.map(p => FilePath.unsafe(p.toString))).run(conf)
  }
  
  def exists(location: IvoryLocation): ResultTIO[Boolean] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path))            => Files.exists(path.toFilePath).flatMap(e => if (e) ResultT.ok[IO, Boolean](e) else Directories.exists(path))
    case s @ S3IvoryLocation(S3Location(path), s3Client)        => S3.exists(path.toFilePath).executeT(s3Client)
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) => Hdfs.exists(h.toHdfsPath).run(conf)
  }
  
  def writeUtf8Lines(location: IvoryLocation, lines: List[String]): ResultTIO[Unit] = 
    writeUtf8(location, lines.mkString("\n"))
  
  def writeUtf8(location: IvoryLocation, string: String): ResultTIO[Unit] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path))            => Files.write(path.toFilePath, string)
    case s @ S3IvoryLocation(S3Location(path), s3Client)        => S3.writeLines(path.toFilePath, string.split("\n")).executeT(s3Client).void
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) => Hdfs.writeWith(h.toHdfsPath, out => Streams.write(out, string)).run(conf)
  }
  
  def fromKey(repository: Repository, key: Key): IvoryLocation =
    repository.toIvoryLocation(key)

  def fromUri(s: String, ivory: IvoryConfiguration): ResultTIO[IvoryLocation] = 
    ResultT.fromDisjunctionString[IO, IvoryLocation](parseUri(s, ivory))

  def fromDirPath(dirPath: DirPath): LocalIvoryLocation =
    LocalIvoryLocation(LocalLocation(dirPath))

  def fromFilePath(filePath: FilePath): LocalIvoryLocation =
    LocalIvoryLocation(LocalLocation(filePath.toDirPath))

  def parseUri(s: String, ivory: IvoryConfiguration): String \/ IvoryLocation = try {
    val uri = new java.net.URI(s)
    uri.getScheme match {
      case null =>
        // TODO Should be LocalLocation but our own consumers aren't ready yet
        // https://github.com/ambiata/ivory/issues/87
        HdfsIvoryLocation(HdfsLocation(DirPath.unsafe(new File(uri.getPath).getAbsolutePath)), ivory.configuration, ivory.scoobiConfiguration, ivory.codec).right

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
