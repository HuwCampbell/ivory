package com.ambiata.ivory.core

import com.ambiata.mundane.control.{ResultT, ResultTIO}
import com.ambiata.mundane.io._
import com.ambiata.mundane.store.Key
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.saws.s3.S3
import org.apache.hadoop.fs.Path

import scalaz._, Scalaz._
import scalaz.effect.IO

case class IvoryLocation(location: Location, @transient ivory: IvoryConfiguration) {
  def configuration = ivory.configuration
  def s3Client = ivory.s3Client

  def path = location.path

  def map(f: DirPath => DirPath): IvoryLocation = copy(location = location match {
    case LocalLocation(path, _) => LocalLocation(f(path), location.uri)
    case S3Location(path, _)    => S3Location(f(path), location.uri)
    case HdfsLocation(path, _)  => HdfsLocation(f(path), location.uri)
  })

  def </>(other: IvoryLocation): IvoryLocation = map(_ </> other.path)
  def </>(other: FilePath): IvoryLocation = map(_ </> other.toDirPath)
  def </>(other: DirPath): IvoryLocation  = map(_ </> other)
  def </>(name: FileName): IvoryLocation  = map(_ </> name)

  def toHdfs: Path = new Path(path.path)
}

object IvoryLocation {
  def deleteAll(location: IvoryLocation): ResultTIO[Unit] = location.location match {
    case LocalLocation(path, _) => Directories.delete(path).void
    case S3Location(path, _)    => S3.deleteAll(path).executeT(location.s3Client)
    case HdfsLocation(path, _)  => Hdfs.deleteAll(location.toHdfs).run(location.configuration)
  }

  def delete(location: IvoryLocation): ResultTIO[Unit] = location.location match {
    case LocalLocation(path, _) => Files.delete(path.toFilePath).void
    case S3Location(path, _)    => S3.deleteObject(path.toFilePath).executeT(location.s3Client)
    case HdfsLocation(path, _)  => Hdfs.delete(location.toHdfs).run(location.configuration)
  }

  def readLines(location: IvoryLocation): ResultTIO[List[String]] = location.location match {
    case LocalLocation(path, _) => Files.readLines(path.toFilePath).map(_.toList)
    case S3Location(path, _)    => S3.readLines(path.toFilePath).executeT(location.s3Client)
    case HdfsLocation(path, _)  =>
      Hdfs.isDirectory(location.toHdfs).flatMap { isDirectory =>
        if (isDirectory)
          Hdfs.globFilesRecursively(location.toHdfs).filterHidden
            .flatMap(_.traverseU(Hdfs.readLines)).map(_.toList.flatten)
        else
          Hdfs.readLines(location.toHdfs).map(_.toList)
      }.run(location.configuration)
  }

  def list(location: IvoryLocation): ResultTIO[List[FilePath]] = location.location match {
    case LocalLocation(path, _) => Directories.list(path)
    case S3Location(path, _)    => S3.listKeys(path).map(_.map(FilePath.unsafe)).executeT(location.s3Client)
    case HdfsLocation(path, _)  => Hdfs.globFilesRecursively(location.toHdfs).map(_.map(p => FilePath.unsafe(p.toString))).run(location.configuration)
  }
  
  def exists(location: IvoryLocation): ResultTIO[Boolean] = location.location match {
    case LocalLocation(path, _) => Files.exists(path.toFilePath).flatMap(e => if (e) ResultT.ok[IO, Boolean](e) else Directories.exists(location.path))
    case S3Location(path, _)    => S3.exists(path.toFilePath).executeT(location.s3Client)
    case HdfsLocation(path, _)  => Hdfs.exists(location.toHdfs).run(location.configuration)
  }
  
  def writeUtf8Lines(location: IvoryLocation, lines: List[String]): ResultTIO[Unit] = 
    writeUtf8(location, lines.mkString("\n"))
  
  def writeUtf8(location: IvoryLocation, string: String): ResultTIO[Unit] = location match {
    case IvoryLocation(LocalLocation(s, _), _)    => Files.write(s.toFilePath, string)
    case IvoryLocation(S3Location(s, _)   , conf) => S3.writeLines(s.toFilePath, string.split("\n")).executeT(conf.s3Client).void
    case IvoryLocation(HdfsLocation(s, _) , conf) => Hdfs.writeWith(location.toHdfs, out => Streams.write(out, string)).run(conf.configuration)
  }
  
  def fromKey(repository: Repository, key: Key): IvoryLocation =
    repository.toIvoryLocation(key)

  def fromUri(s: String, ivory: IvoryConfiguration): ResultTIO[IvoryLocation] = 
    ResultT.fromDisjunctionString[IO, IvoryLocation](parseUri(s, ivory))

  def fromDirPath(dirPath: DirPath): IvoryLocation =
    IvoryLocation(LocalLocation(dirPath, new java.net.URI("file:/"+dirPath.path)), IvoryConfiguration.Empty)

  def fromFilePath(filePath: FilePath): IvoryLocation =
    IvoryLocation(LocalLocation(filePath.toDirPath, new java.net.URI("file:/"+filePath.path)), IvoryConfiguration.Empty)

  def parseUri(s: String, ivory: IvoryConfiguration): String \/ IvoryLocation = try {
    val uri = new java.net.URI(s)
    uri.getScheme match {
      case null =>
        // TODO Should be LocalLocation but our own consumers aren't ready yet
        // https://github.com/ambiata/ivory/issues/87
        IvoryLocation(HdfsLocation(DirPath.unsafe(uri.getPath), uri), ivory).right
      case _ => Location.fromUri(s).map(l => IvoryLocation(l, ivory))
    }
  } catch {
    case e: java.net.URISyntaxException =>
      e.getMessage.left
  }
}
