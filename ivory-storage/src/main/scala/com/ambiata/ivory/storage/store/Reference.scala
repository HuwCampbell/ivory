package com.ambiata.ivory.storage.store

import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store._
import com.nicta.scoobi.Scoobi._

import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.poacher.hdfs.HdfsStore
import com.ambiata.saws.s3.S3Store
import com.ambiata.saws.core.Clients

import scalaz.{Store => _, _}, Scalaz._, effect._, \&/._

/**
 * Represents a relative path within a repository
 */
case class Reference[F[_]](store: Store[F], path: FilePath) {

  def run[A](f: Store[F] => FilePath => A): A =
    f(store)(path)

  def </>(path2: FilePath): Reference[F] =
    copy(path = path </> path2)

  def </>(path2: String): Reference[F] =
    copy(path = path </> path2)
}

object Reference {

  val defaultS3TmpDirectory: FilePath =
    ".s3repository".toFilePath

  def hdfsPath(ref: ReferenceIO): ResultTIO[Path] = ref match {
    case Reference(HdfsStore(_, root), p) => ResultT.ok[IO, Path]((root </> p).toHdfs)
    case _                                => ResultT.fail[IO, Path](s"Given reference '${ref}' is not HDFS")
  }

  def fromUriResultTIO(s: String, conf: ScoobiConfiguration): ResultTIO[ReferenceIO] =
    ResultT.fromDisjunction[IO, ReferenceIO](fromUri(s, conf).leftMap(This.apply))

  def fromUri(s: String, conf: ScoobiConfiguration): String \/ ReferenceIO = {
    val (root, file) = s.lastIndexOf('/') match {
      case -1 => (s, "/")
      case i => (s.substring(0, i), s.substring(i))
    }
    storeFromUri(root, conf).map(s => Reference(s, file.toFilePath))
  }

  def storeFromUri(s: String, conf: ScoobiConfiguration): String \/ Store[ResultTIO] =
    location(s).map(_ match {
      case HdfsLocation(path)       => HdfsStore(conf, path.toFilePath)
      case LocalLocation(path)      => PosixStore(path.toFilePath)
      case S3Location(bucket, path) => S3Store(bucket, path.toFilePath, Clients.s3, defaultS3TmpDirectory)
    })

  def location(s: String): String \/ Location = try {
    val uri = new java.net.URI(s)
    uri.getScheme match {
      case null =>
        // TODO Should be LocalLocation but our own consumers aren't ready yet
        // https://github.com/ambiata/ivory/issues/87
        HdfsLocation(uri.getPath).right
      case _ => Location.fromUri(s)
    }
  } catch {
    case e: java.net.URISyntaxException =>
      e.getMessage.left
  }
}
