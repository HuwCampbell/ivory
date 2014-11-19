package com.ambiata.ivory.core

import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.ambiata.notion.core.TemporaryType._
import org.specs2._
import TemporaryLocations._
import org.specs2.execute.AsResult
import org.specs2.matcher.{MustMatchers, ThrownExpectations}
import org.specs2.specification.FixtureExample
import com.ambiata.mundane.testing.ResultTIOMatcher._
import scalaz._, Scalaz._

class IvoryLocationSpec extends Specification with ForeachTemporaryType with ThrownExpectations { def is = s2"""

IvoryLocation
-------------

  Can parse local URIs                            $local
  Can parse local URIs with no file               $localShort
  Can parse hdfs URIs                             $hdfs
  Can parse s3 URIs                               $s3

 The IvoryLocation object provides functions to read/write/query locations on different systems ${section("aws")}

   isDirectory    $isDirectory
   deleteAll      $deleteAll
   delete         $delete
   readLines      $readLines
   list           $list
   exists         $exists
"""
  def local =
    IvoryLocation.parseUri("file:///some/path", ivory).toEither must
      beRight((_: IvoryLocation).location must_== LocalLocation("/some/path"))

  def localShort =
    IvoryLocation.parseUri("file:///some/", ivory).toEither must
      beRight((_: IvoryLocation).location must_== LocalLocation("/some/"))

  def hdfs =
    IvoryLocation.parseUri("hdfs:///some/path", ivory).toEither must
      beRight((_: IvoryLocation).location must_== HdfsLocation("/some/path"))

  def s3 =
    IvoryLocation.parseUri("s3://bucket/key", ivory).toEither must
      beRight((_: IvoryLocation).location must_== S3Location("bucket", "key"))

  def isDirectory = { temporaryType: TemporaryType =>
    "This location is a directory on "+temporaryType ==> {
      withIvoryLocationDir(temporaryType) { location =>
        IvoryLocation.writeUtf8(location </> "file", "") >>
        IvoryLocation.isDirectory(location)
      } must beOkValue(true)
    }

    "The location is a file on "+temporaryType ==> {
      withIvoryLocationFile(temporaryType) { location =>
        IvoryLocation.writeUtf8(location, "") >>
        IvoryLocation.isDirectory(location)
      } must beOkValue(false)
    }
  }

  def deleteAll = { temporaryType: TemporaryType =>
    "All files are deleted "+temporaryType ==> {
      withIvoryLocationDir(temporaryType) { location =>
        IvoryLocation.writeUtf8(location </> "file1", "") >>
        IvoryLocation.writeUtf8(location </> "file2", "") >>
        IvoryLocation.deleteAll(location) >>
        IvoryLocation.list(location)
      } must beOkValue(Nil)
    }
  }

  def delete = { temporaryType: TemporaryType =>
    "The file is deleted on "+temporaryType ==> {
      withIvoryLocationFile(temporaryType) { location =>
        IvoryLocation.writeUtf8(location, "") >>
          IvoryLocation.delete(location) >>
          IvoryLocation.exists(location)
      } must beOkValue(false)
    }
  }

  def readLines = { temporaryType: TemporaryType =>
    val lines = List("one", "two")
    withIvoryLocationFile(temporaryType) { location =>
      IvoryLocation.writeUtf8Lines(location, lines) >>
      IvoryLocation.readLines(location)
    } must beOkValue(lines)
  }


  def list = { temporaryType: TemporaryType =>
    "All files are listed on "+temporaryType ==> {
      withIvoryLocationDir(temporaryType) { location =>
        IvoryLocation.writeUtf8(location </> "file1", "") >>
          IvoryLocation.writeUtf8(location </> "file2", "") >>
          IvoryLocation.list(location).map(ls => (ls.map(_.show.split("/").last)).filter(!_.startsWith(".location")))
      } must beOkLike { case (ls1) => ls1.toSet must_== Set("file1", "file2") }
    }
  }

  def exists = { temporaryType: TemporaryType =>
    "There is a file on "+temporaryType ==> {
      withIvoryLocationFile(temporaryType) { location =>
        IvoryLocation.writeUtf8(location, "") >>
          IvoryLocation.exists(location)
      } must beOkValue(true)
    }
  }

  val ivory = IvoryConfiguration.Empty


}

trait ForeachTemporaryType extends FixtureExample[TemporaryType] with MustMatchers {
  def fixture[R : AsResult](f: TemporaryType => R) =
    Seq(Posix, Hdfs, S3).toStream.map(t => AsResult(f(t))).reduceLeft(_ and _)
}
