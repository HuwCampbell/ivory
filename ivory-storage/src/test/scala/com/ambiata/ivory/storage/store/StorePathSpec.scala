package com.ambiata.ivory.storage.store

import com.ambiata.ivory.alien.hdfs.HdfsStore
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store.PosixStore
import com.ambiata.saws.s3.S3Store
import com.nicta.scoobi.Scoobi._
import org.specs2._

class StorePathSpec extends Specification { def is = s2"""

StorePath
---------

  Can parse local URIs                            $local
  Can parse local URIs with no file               $localShort
  Can parse hdfs URIs                             $hdfs
  Can parse s3 URIs                               $s3

"""
  lazy val Conf = ScoobiConfiguration()

  def local =
    StorePath.fromUri("file:///some/path", Conf).toEither must beRight (new StorePath(
      PosixStore(FilePath.root </> "some"), FilePath("/path")
    ))

  def localShort =
    StorePath.fromUri("file:///some/", Conf).toEither must beRight (new StorePath(
      PosixStore(FilePath.root </> "some"), FilePath("/")
    ))

  def hdfs =
    StorePath.fromUri("hdfs:///some/path", Conf).toEither must beRight(new StorePath(
      HdfsStore(Conf, "/some".toFilePath), FilePath("/path")
    ))

  def s3 =
    StorePath.fromUri("s3://bucket/key", Conf).toEither must beRight((s: StorePathIO) => s must beLike({
      case StorePath(S3Store(_, _, client, _), _) =>
        s must_== StorePath(S3Store("bucket", "".toFilePath, client, Repository.defaultS3TmpDirectory), FilePath("/key"))
    }))
}
