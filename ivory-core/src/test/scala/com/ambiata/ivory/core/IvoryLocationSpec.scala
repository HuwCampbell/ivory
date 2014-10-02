package com.ambiata.ivory.core

import java.net.URI

import com.ambiata.mundane.io._
import org.specs2._

class IvoryLocationSpec extends Specification { def is = s2"""

IvoryLocation
-------------

  Can parse local URIs                            $local
  Can parse local URIs with no file               $localShort
  Can parse hdfs URIs                             $hdfs
  Can parse s3 URIs                               $s3

"""

  def local =
    IvoryLocation.parseUri("file:///some/path", ivory).toEither must
      beRight((_: IvoryLocation).location must_== LocalLocation(DirPath.Root </> "some" </> "path"))
    
  def localShort =
    IvoryLocation.parseUri("file:///some/", ivory).toEither must
      beRight((_: IvoryLocation).location must_== LocalLocation(DirPath.Root </> "some"))

  def hdfs =
    IvoryLocation.parseUri("hdfs:///some/path", ivory).toEither must
      beRight((_: IvoryLocation).location must_== HdfsLocation(DirPath.Root </> "some" </> "path"))

  def s3 =
    IvoryLocation.parseUri("s3://bucket/key", ivory).toEither must
      beRight((_: IvoryLocation).location must_== S3Location(DirPath.Root </> "bucket" </> "key"))
  
  val ivory = IvoryConfiguration.Empty
}
