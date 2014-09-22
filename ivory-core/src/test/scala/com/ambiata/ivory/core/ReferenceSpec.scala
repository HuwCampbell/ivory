package com.ambiata.ivory.core

import com.ambiata.mundane.io._
import com.ambiata.mundane.store.PosixStore
import com.ambiata.poacher.hdfs.HdfsStore
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
  lazy val conf = IvoryConfiguration.fromScoobiConfiguration(ScoobiConfiguration())

  def local =
    Reference.parseUri("file:///some/path", conf).toEither must beRight (new Reference(
      PosixStore(DirPath.Root </> "some" </> "path"), DirPath.Empty
    ))

  def localShort =
    Reference.parseUri("file:///some/", conf).toEither must beRight (new Reference(
      PosixStore(DirPath.Root </> "some"), DirPath.Empty
    ))

  def hdfs =
    Reference.parseUri("hdfs:///some/path", conf).toEither must beRight(new Reference(
      HdfsStore(conf.configuration, DirPath.Root </> "some" </> "path"), DirPath.Empty
    ))

  def s3 =
    Reference.parseUri("s3://bucket/key", conf).toEither must beRight((s: ReferenceIO) => s must beLike({
      case Reference(S3Store(_, _, client, _), _) =>
        s must_== Reference(S3Store("bucket", DirPath("key"), client, IvoryConfiguration.defaultS3TmpDirectory), DirPath.Empty)
    }))

  def default =
    Reference.parseUri("/some/path", conf).toEither must beRight (Reference(
      HdfsStore(conf.configuration, DirPath("some")), DirPath("path")
    ))

  def fragment =
    Reference.parseUri("some/path", conf).toEither must beRight (Reference(
      HdfsStore(conf.configuration, DirPath("some")), DirPath("path")
    ))
}
