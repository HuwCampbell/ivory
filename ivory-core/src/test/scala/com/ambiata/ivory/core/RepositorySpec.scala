package com.ambiata.ivory.core

import com.ambiata.mundane.io._
import com.nicta.scoobi.Scoobi._
import org.specs2._

class RepositorySpec extends Specification with ScalaCheck { def is = s2"""

Repository Known Answer Tests
-----------------------------

  Can parse hdfs URIs                             $hdfs
  Can parse s3 URIs                               $s3
  Can parse local URIs                            $local
  Can parse relative URIs                         $relative
  Can parse default local URIs                    $dfault
  Can parse default relative local URIs           $fragment

"""
  lazy val conf = IvoryConfiguration.fromScoobiConfiguration(ScoobiConfiguration())

  def hdfs =
    Repository.fromUri("hdfs:///some/path", conf).toEither must beRight((r: Repository) => r must beLike({
      case HdfsRepository(_, _) =>
        r must_== HdfsRepository(DirPath.Root </> "some" </> "path", conf)
    }))

  def s3 =
    Repository.fromUri("s3://bucket/key", conf).toEither must beRight((r: Repository) => r must beLike({
      case repository: S3Repository =>
        r must_== S3Repository("bucket", DirPath.Empty </> "key", conf)
    }))

  def local =
    Repository.fromUri("file:///some/path", conf).toEither must beRight(LocalRepository(DirPath.Root </> "some" </> "path"))

  def relative =
    Repository.fromUri("file:some/path", conf).toEither must beRight(LocalRepository(DirPath.Empty </> "some" </> "path"))

  def dfault =
    Repository.fromUri("/some/path", conf).toEither must beRight((r: Repository) => r must beLike({
      case HdfsRepository(_, _) =>
        r must_== HdfsRepository(DirPath.Root </> "some" </> "path", conf)
    }))

  def fragment =
    Repository.fromUri("some/path", conf).toEither must beRight((r: Repository) => r must beLike({
      case HdfsRepository(_, _) =>
        r must_== HdfsRepository("some" </> "path", conf)
    }))

}
