package com.ambiata.ivory.core

import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store._
import com.ambiata.poacher.hdfs.HdfsStore
import com.ambiata.saws.s3.S3Store
import com.nicta.scoobi.core.ScoobiConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

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

  def readLines(implicit F: Monad[F]): F[List[String]] = for {
    paths <- store.filter(path, p => { val sp = (FilePath.root </> p).relativeTo(path); !sp.path.startsWith("_") && !sp.path.startsWith(".") })
    all   <- paths.traverseU(store.linesUtf8.read).map(_.flatten)
  } yield all
}

object Reference {

  def hdfsPath(ref: ReferenceIO): ResultTIO[Path] = ref match {
    case Reference(HdfsStore(_, root), p) => ResultT.ok[IO, Path]((root </> p).toHdfs)
    case _                                => ResultT.fail[IO, Path](s"Given reference '${ref}' is not HDFS")
  }

  def fromUriResultTIO(uri: String, configuration: Configuration): ResultTIO[ReferenceIO] =
    fromUriResultTIO(uri, IvoryConfiguration.fromConfiguration(configuration))

  def fromUriResultTIO(uri: String, scoobiConfiguration: ScoobiConfiguration): ResultTIO[ReferenceIO] =
    fromUriResultTIO(uri, IvoryConfiguration.fromScoobiConfiguration(scoobiConfiguration))

  def fromUriFilePathResultTIO(uri: FilePath, repositoryConfiguration: IvoryConfiguration): ResultTIO[ReferenceIO] =
    fromUriResultTIO(uri.path, repositoryConfiguration)

  def fromUriResultTIO(uri: String, repositoryConfiguration: IvoryConfiguration): ResultTIO[ReferenceIO] =
    ResultT.fromDisjunction[IO, ReferenceIO](fromUri(uri, repositoryConfiguration).leftMap(This.apply))

  def fromUri(uri: String, repositoryConfiguration: IvoryConfiguration): String \/ ReferenceIO = {
    val (root, file) = uri.lastIndexOf('/') match {
      case -1 => (uri, "/")
      case i  => (uri.substring(0, i), uri.substring(i))
    }
    storeFromUri(root, repositoryConfiguration).map(s => Reference(s, file.toFilePath))
  }

  def storeFromUri(uri: String, repositoryConfiguration: IvoryConfiguration): String \/ Store[ResultTIO] =
    location(uri).map(_ match {
      case HdfsLocation(path)       => HdfsStore(repositoryConfiguration.configuration, path.toFilePath)
      case LocalLocation(path)      => PosixStore(path.toFilePath)
      case S3Location(bucket, path) => S3Store(bucket, path.toFilePath, repositoryConfiguration.s3Client, repositoryConfiguration.s3TmpDirectory)
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
