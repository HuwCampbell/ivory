package com.ambiata.ivory.core

import java.io.File

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control.{ResultT, RIO}
import com.ambiata.mundane.data.{Lists => L}
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

  /** This is far from ok, but is acting as a magnet for broken code that depends on this
      nonsense casting. This will be removed with s3 changes. */
  def asHdfsIvoryLocation: RIO[HdfsIvoryLocation] =
    this match {
      case h @ HdfsIvoryLocation(_, _, _, _) =>
        h.pure[RIO]
      case _ =>
        RIO.fail[HdfsIvoryLocation]("This ivory operation currently only supports hdfs locations.")
    }
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

  def fromUri(uri: String, ivory: IvoryConfiguration): RIO[HdfsIvoryLocation] =
    IvoryLocation.fromUri(uri, ivory).flatMap {
      case h: HdfsIvoryLocation => RIO.ok[HdfsIvoryLocation](h)
      case l                    => RIO.fail[HdfsIvoryLocation](s"${l.show} is not an HDFS location")
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
  def deleteAll(location: IvoryLocation): RIO[Unit] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path))            => Directories.delete(l.dirPath).void
    case s @ S3IvoryLocation(S3Location(bucket, key), s3Client) => S3Prefix(bucket, key).delete.execute(s3Client)
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) => Hdfs.deleteAll(new Path(path)).run(conf)
  }

  def delete(location: IvoryLocation): RIO[Unit] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path))            => Files.delete(l.filePath).void
    case s @ S3IvoryLocation(S3Location(bucket, key), s3Client) => S3Address(bucket, key).delete.execute(s3Client)
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) => Hdfs.delete(new Path(path)).run(conf)
  }

  def readUtf8(location: IvoryLocation): RIO[String] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path)) =>
      Files.read(l.filePath)
    case s @ S3IvoryLocation(S3Location(bucket, key), s3Client) =>
      S3Address(bucket, key).get.execute(s3Client)
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) =>
      Hdfs.readContentAsString(new Path(path)).run(conf)
  }

  def readLines(location: IvoryLocation): RIO[List[String]] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path)) =>
      Files.readLines(l.filePath).map(_.toList)
    case s @ S3IvoryLocation(S3Location(bucket, key), s3Client) =>
      S3Address(bucket, key).getLines.execute(s3Client)
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) =>
      Hdfs.isDirectory(new Path(path)).flatMap { isDirectory =>
        if (isDirectory)
          Hdfs.globFilesRecursively(new Path(path)).filterHidden
            .flatMap(_.traverseU(Hdfs.readLines)).map(_.toList.flatten)
        else
          Hdfs.readLines(new Path(path)).map(_.toList)
      }.run(conf)
  }

  def readUnsafe(location: IvoryLocation)(f: java.io.InputStream => RIO[Unit]): RIO[Unit] = {
    import scalaz.effect.Effect._
    location match {
      case l @ LocalIvoryLocation(LocalLocation(path)) =>
        RIO.using(l.filePath.toInputStream)(f)
      case s @ S3IvoryLocation(S3Location(bucket, key), s3Client) =>
        RIO.using(S3Address(bucket, key).getObject.map(_.getObjectContent).execute(s3Client))(f)
      case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) =>
        Hdfs.readWith(new Path(path), f).run(conf)
    }
  }

  def streamLinesUTF8[A](location: IvoryLocation, empty: => A)(f: (String, A) => A): RIO[A] = {
    RIO.io(empty).flatMap { s =>
      var state = s
      readUnsafe(location) { in => RIO.io {
        val reader = new java.io.BufferedReader(new java.io.InputStreamReader(in, "UTF-8"))
        var line = reader.readLine
        while (line != null) {
          state = f(line, state)
          line = reader.readLine
        }
      }}.as(state)
    }
  }

  def list(location: IvoryLocation): RIO[List[IvoryLocation]] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path)) =>
      Directories.list(l.dirPath).map(fs => fs.map(f => l.copy(location = LocalLocation(f.path))))

    case s @ S3IvoryLocation(S3Location(bucket, key), s3Client) =>
      S3Pattern(bucket, key).listKeys.execute(s3Client).map(_.map { k =>
        s.copy(location = S3Location(bucket,  k))
      })

    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) =>
      Hdfs.globFilesRecursively(new Path(path)).run(conf).map(_.map { p =>
        h.copy(location = HdfsLocation(p.toString))
      })
  }

  def exists(location: IvoryLocation): RIO[Boolean] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path))            =>
      Files.exists(FilePath.unsafe(path)).flatMap(e => if (e) RIO.ok[Boolean](e) else Directories.exists(l.dirPath))
    case s @ S3IvoryLocation(S3Location(bucket, key), s3Client) => S3Pattern(bucket, key).exists.execute(s3Client)
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) => Hdfs.exists(new Path(path)).run(conf)
  }

  def writeUtf8Lines(location: IvoryLocation, lines: List[String]): RIO[Unit] =
    writeUtf8(location, L.prepareForFile(lines))

  def writeUtf8(location: IvoryLocation, string: String): RIO[Unit] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path))            => Files.write(l.filePath, string)
    case s @ S3IvoryLocation(S3Location(bucket, key), s3Client) => S3Address(bucket, key).put(string).execute(s3Client).void
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) => Hdfs.writeWith(new Path(path), out => Streams.write(out, string)).run(conf)
  }

  def size(location: IvoryLocation): RIO[Bytes] = (location match {
    case l @ LocalIvoryLocation(LocalLocation(path)) =>
      Directories.list(l.dirPath).flatMap(_.traverseU(f => RIO.io { f.toFile.length.toLong }).map(_.sum))
    case s @ S3IvoryLocation(S3Location(bucket, key), s3Client) =>
      S3Pattern(bucket, key).size.execute(s3Client).map(_.getOrElse(0L))
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) =>
      Hdfs.globFilesRecursively(new Path(path)).flatMap(_.traverse(f => Hdfs.size(f).map(_.toBytes.value)).map(_.sum)).run(conf)
  }).map(Bytes.apply)

  def isDirectory(location: IvoryLocation): RIO[Boolean] = location match {
    case l @ LocalIvoryLocation(LocalLocation(path))            => RIO.safe[Boolean](new File(path).isDirectory)
    case s @ S3IvoryLocation(S3Location(bucket, key), s3Client) => S3Prefix(bucket, key + "/").listSummary.map(_.nonEmpty).execute(s3Client)
    case h @ HdfsIvoryLocation(HdfsLocation(path), conf, sc, _) => Hdfs.isDirectory(new Path(path)).run(conf)
  }

  def fromKey(repository: Repository, key: Key): IvoryLocation =
    repository.toIvoryLocation(key)

  def fromUri(s: String, ivory: IvoryConfiguration): RIO[IvoryLocation] =
    RIO.fromDisjunctionString[IvoryLocation](parseUri(s, ivory))

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

      case _ => Location.fromUri(s).map(l => fromLocation(l, ivory))
    }
  } catch {
    case e: java.net.URISyntaxException =>
      e.getMessage.left
  }

  def fromLocation(loc: Location, ivory: IvoryConfiguration): IvoryLocation = loc match {
    case l: LocalLocation =>
      LocalIvoryLocation(l)
    case s: S3Location =>
      S3IvoryLocation(s, ivory.s3Client)
    case h: HdfsLocation =>
      HdfsIvoryLocation(h, ivory.configuration, ivory.scoobiConfiguration, ivory.codec)
  }
}
