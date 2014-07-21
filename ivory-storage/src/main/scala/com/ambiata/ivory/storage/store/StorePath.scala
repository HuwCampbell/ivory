package com.ambiata.ivory.storage.store

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store._
import com.nicta.scoobi.Scoobi._
import scalaz.{Store => _, _}, effect._

/**
 * Represents a relative path within a repository
 */
case class StorePath[F[_]](store: Store[F], path: FilePath) {

  def run[A](f: Store[F] => FilePath => A): A =
    f(store)(path)

  def </>(path2: FilePath): StorePath[F] =
    copy(path = path </> path2)
}

object StorePath {

  val defaultS3TmpDirectory: FilePath =
    ".s3repository".toFilePath

  def fromUri(s: String, conf: ScoobiConfiguration): String \/ StorePathIO = {
    val (root, file) = s.lastIndexOf('/') match {
      case -1 => (s, "/")
      case i => (s.substring(0, i), s.substring(i))
    }
    StoreUtil.fromUri(root, conf).map(new StorePath(_, FilePath(file)))
  }

  def fromUriResult(s: String, conf: ScoobiConfiguration): ResultTIO[StorePathIO] =
    ResultT.fromDisjunction[IO, StorePathIO](StorePath.fromUri(s, conf).leftMap(\&/.This(_)))
}
