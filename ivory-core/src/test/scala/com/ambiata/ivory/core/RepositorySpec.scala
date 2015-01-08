package com.ambiata.ivory.core

import java.io.File
import java.net.URI

import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.nicta.scoobi.Scoobi._
import org.specs2._
import org.specs2.matcher.DisjunctionMatchers

class RepositorySpec extends Specification with ScalaCheck with DisjunctionMatchers { def is = s2"""

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
  lazy val flags = IvoryFlags.default

  def hdfs =
    Repository.parseUri("hdfs:///some/path", conf, flags) must
      be_\/-(HdfsRepository(HdfsLocation("/some/path"), conf, flags))

  def s3 =
    Repository.parseUri("s3://bucket/key", conf, flags) must
      be_\/-(S3Repository(S3Location("bucket", "key"), conf, flags))

  def local =
    Repository.parseUri("file:///some/path", conf, flags) must
      be_\/-(LocalRepository(LocalLocation("/some/path"), flags))

  def relative =
    Repository.parseUri("file:some/path", conf, flags) must
      be_\/-(LocalRepository(LocalLocation("some/path"), flags))

  def dfault =
    Repository.parseUri("/some/path", conf, flags) must
      be_\/-(HdfsRepository(HdfsLocation("/some/path"), conf, flags))

  def fragment =
    Repository.parseUri("some/path", conf, flags) must
      be_\/-(HdfsRepository(HdfsIvoryLocation(HdfsLocation((DirPath.unsafe(new File(".").getAbsolutePath).dirname </> "some" </> "path").path), conf.configuration, conf.scoobiConfiguration, conf.codec), flags))

}
