package com.ambiata.ivory.core

import com.ambiata.mundane.io.{Location => _, _}
import org.specs2._

class LocationSpec extends Specification { def is = s2"""

Location Known Answer Tests
-----------------------------

  Can parse hdfs URIs                             $hdfs
  Can parse s3 URIs                               $s3
  Can parse local URIs                            $local
  Can parse relative URIs                         $relative
  Can parse default local URIs                    $dfault
  Can parse default relative local URIs           $fragment

"""
  def hdfs =
    Location.fromUri("hdfs:///some/path").toEither must beRight(HdfsLocation("/some/path"))

  def s3 =
    Location.fromUri("s3://bucket/key").toEither must beRight(S3Location("bucket", "key"))

  def local =
    Location.fromUri("file:///some/path").toEither must beRight(LocalLocation("/some/path"))

  def relative =
    Location.fromUri("file:some/path").toEither must beRight(LocalLocation("some/path"))

  def dfault =
    Location.fromUri("/some/path").toEither must beRight(HdfsLocation("/some/path"))

  def fragment =
    Location.fromUri("some/path").toEither must beRight(HdfsLocation("some/path"))

}
