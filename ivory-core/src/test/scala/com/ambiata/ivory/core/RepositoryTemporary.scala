package com.ambiata.ivory.core

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.ivory.core.{IvoryConfigurationTemporary => ICT}
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.notion.core.{TemporaryType => T, _}
import com.ambiata.notion.core.Arbitraries._
import com.ambiata.poacher.hdfs._
import com.ambiata.saws.s3._

import org.apache.hadoop.conf.Configuration
import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._


case class RepositoryTemporary(t: T, path: String) {
  def repository: RIO[Repository] = t match {
    case (T.Posix) =>
      local
    case (T.S3) =>
      s3
    case (T.Hdfs) =>
      hdfs
  }

  def hdfs: RIO[Repository] = for {
    c <- IvoryConfigurationTemporary.random.conf
    r <- HdfsTemporary(path).path.map(p =>
        HdfsRepository(HdfsLocation(p.toString), c)).run(c.configuration)
  } yield r

  def s3: RIO[Repository] = for {
    c <- IvoryConfigurationTemporary.random.conf
    r <- S3Temporary(path).prefix.map(p =>
      S3Repository(S3Location(p.bucket, p.prefix), c)).run(c.s3Client).map(_._2)
  } yield r

  def local: RIO[Repository] =
    LocalTemporary(path).directory.map(d =>
      LocalRepository(LocalLocation(d.path)))
}

object RepositoryTemporary {
  implicit def RepositoryTemporaryArbitrary: Arbitrary[RepositoryTemporary] =
    Arbitrary(arbitrary[T].map(RepositoryTemporary(_, java.util.UUID.randomUUID().toString)))

  /** Deprecated callbacks. Use `RepositoryTemporary.repository` */
  def withRepository[A](t: T)(f: Repository => RIO[A]): RIO[A] =
    RepositoryTemporary(t, java.util.UUID.randomUUID().toString).repository >>= (f(_))

  def withHdfsRepository[A](f: HdfsRepository => RIO[A]): RIO[A] = for {
    t <- RepositoryTemporary(T.Hdfs, java.util.UUID.randomUUID().toString).repository
    h <- t.asHdfsRepository
    r <- f(h)
  } yield r

}
