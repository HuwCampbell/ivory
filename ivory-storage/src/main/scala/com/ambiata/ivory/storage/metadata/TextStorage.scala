package com.ambiata.ivory.storage.metadata

import scalaz.{Value => _, _}, Scalaz._, \&/._, effect.IO
import org.apache.hadoop.fs.Path
import com.ambiata.ivory.alien.hdfs._, HdfsS3Action._
import com.ambiata.ivory.storage.store._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.saws.s3.S3

trait TextStorage[L, T] {

  def name: String
  def parseLine(i: Int, l: String): ValidationNel[String, L]
  def fromList(s: List[L]): T
  def toList(t: T): List[L]
  def toLine(l: L): String

  def fromStore[F[+_] : Monad](path: StorePathResultT[F]): ResultT[F, T] = for {
    exists <- path.run(_.exists)
    _      <- if (!exists) ResultT.fail[F, Unit](s"Path ${path.path} does not exist in ${path.store}!") else ResultT.ok[F, Unit](())
    lines  <- path.run(_.linesUtf8.read)
    t      <- ResultT.fromDisjunction[F, T](fromLines(lines).leftMap(\&/.This(_)))
  } yield t

  def fromHdfs(path: Path): Hdfs[T] =
    Hdfs.readWith(path, is => fromInputStream(is))

  def toHdfs(path: Path, t: T): Hdfs[Unit] =
    Hdfs.writeWith(path, os => Streams.write(os, delimitedString(t)))

  def fromS3(bucket: String, key: String, tmp: Path): HdfsS3Action[T] = for {
    file  <- HdfsS3Action.fromAction(S3.downloadFile(bucket, key, to = tmp.toString))
    t     <- HdfsS3Action.fromHdfs(fromHdfs(new Path(file.getPath)))
  } yield t

  def toS3(bucket: String, key: String, t: T, tmp: Path): HdfsS3Action[Unit] = for {
    _ <- HdfsS3Action.fromHdfs(toHdfs(tmp, t))
    _ <- HdfsS3.putPaths(bucket, key, tmp, glob = "*")
    _ <- HdfsS3Action.fromHdfs(Hdfs.filesystem.map(fs => fs.delete(tmp, true)))
  } yield ()

  def fromInputStream(is: java.io.InputStream): ResultTIO[T] = for {
    content <- Streams.read(is)
    r <- ResultT.fromDisjunction[IO, T](fromLines(content.lines.toList).leftMap(This(_)))
  } yield r

  def fromString(s: String): ValidationNel[String, T] =
    fromLinesAll(s.lines.toList)

  def fromLines(lines: List[String]): String \/ T = {
    fromLinesAll(lines).leftMap(_.toList.mkString(",")).disjunction
  }

  def fromLinesAll(lines: List[String]): ValidationNel[String, T] = {
    val numbered = lines.zipWithIndex.map({ case (l, n) => (l, n + 1) })
    numbered.traverseU({ case (l, n) => parseLine(n, l).leftMap(_.map(s"Line $n: " +))}).map(fromList)
  }

  def fromFile(path: String): ResultTIO[T] = {
    val file = new java.io.File(path)
    for {
      raw <- Files.read(file.getAbsolutePath.toFilePath)
      fs  <- ResultT.fromDisjunction[IO, T](fromLines(raw.lines.toList).leftMap(err => This(s"Error reading $name from file '$path': $err")))
    } yield fs
  }

  def delimitedString(t: T): String = {
    toList(t).map(toLine).mkString("\n")
  }
}
