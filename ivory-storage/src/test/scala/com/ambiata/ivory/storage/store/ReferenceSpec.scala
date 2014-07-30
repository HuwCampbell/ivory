package com.ambiata.ivory.storage.store

import com.ambiata.poacher.hdfs.HdfsStore
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store.PosixStore
import com.ambiata.saws.s3.S3Store
import com.nicta.scoobi.Scoobi._
import org.specs2._

class ReferenceSpec extends Specification { def is = s2"""

Reference
---------

  Can parse local URIs                            $local
  Can parse local URIs with no file               $localShort
  Can parse hdfs URIs                             $hdfs
  Can parse s3 URIs                               $s3

"""
  lazy val Conf = ScoobiConfiguration()

  def local =
    Reference.fromUri("file:///some/path", Conf).toEither must beRight (new Reference(
      PosixStore(FilePath.root </> "some"), FilePath("/path")
    ))

  def localShort =
    Reference.fromUri("file:///some/", Conf).toEither must beRight (new Reference(
      PosixStore(FilePath.root </> "some"), FilePath("/")
    ))

  def hdfs =
    Reference.fromUri("hdfs:///some/path", Conf).toEither must beRight(new Reference(
      HdfsStore(Conf, "/some".toFilePath), FilePath("/path")
    ))

  def s3 =
    Reference.fromUri("s3://bucket/key", Conf).toEither must beRight((s: ReferenceIO) => s must beLike({
      case Reference(S3Store(_, _, client, _), _) =>
        s must_== Reference(S3Store("bucket", "".toFilePath, client, Repository.defaultS3TmpDirectory), FilePath("/key"))
    }))

  def default =
    Reference.fromUri("/some/path", Conf).toEither must beRight (Reference(
      HdfsStore(Conf, "/some".toFilePath), FilePath("/path")
    ))

  def fragment =
    Reference.fromUri("some/path", Conf).toEither must beRight (Reference(
      HdfsStore(Conf, "some".toFilePath), FilePath("/path")
    ))
}
