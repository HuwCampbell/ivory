package com.ambiata.ivory.core

import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.ambiata.notion.core.TemporaryType._
import org.specs2._
import TemporaryLocations._
import org.specs2.execute.AsResult
import org.specs2.matcher.{MustMatchers, ThrownExpectations}
import org.specs2.specification.FixtureExample
import com.ambiata.mundane.testing.RIOMatcher._
import scalaz._, Scalaz._
import TemporaryIvoryConfiguration._

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
   readWrite      $readWrite
   readLines      $readLines
   streamLines    $streamLines
   list           $list
   exists         $exists

"""
  def local =
    withConfX(ivory =>
      IvoryLocation.parseUri("file:///some/path", ivory)) must
        beOkLike(_.map(_.location) ==== LocalLocation("/some/path").right)

  def localShort =
    withConfX(ivory =>
      IvoryLocation.parseUri("file:///some/", ivory)) must
        beOkLike(_.map(_.location) ==== LocalLocation("/some/").right)

  def hdfs =
    withConfX(ivory =>
      IvoryLocation.parseUri("hdfs:///some/path", ivory)) must
        beOkLike(_.map(_.location) ==== HdfsLocation("/some/path").right)

  def s3 =
    withConfX(ivory =>
      IvoryLocation.parseUri("s3://bucket/key", ivory)) must
        beOkLike(_.map(_.location) ==== S3Location("bucket", "key").right)

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

  def readWrite = { temporaryType: TemporaryType =>
    // Sigh, this can be fixed with rejigging the dependencies, but IvoryLocation should go very soon
    val contents = "a\nb \u9999\n"
    withIvoryLocationFile(temporaryType) { location =>
      IvoryLocation.writeUtf8(location, contents) >>
        IvoryLocation.readUtf8(location)
    } must beOkValue(contents)
  }

  def readLines = { temporaryType: TemporaryType =>
    val lines = List("one", "two")
    withIvoryLocationFile(temporaryType) { location =>
      IvoryLocation.writeUtf8Lines(location, lines) >>
      IvoryLocation.readLines(location)
    } must beOkValue(lines)
  }

  def streamLines = { temporaryType: TemporaryType =>
    val lines = List("one", "two", "three")
    withIvoryLocationFile(temporaryType) { location =>
      IvoryLocation.writeUtf8Lines(location, lines) >>
        IvoryLocation.streamLinesUTF8(location, List[String]())(_ :: _).map(_.reverse)
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

}

trait ForeachTemporaryType extends FixtureExample[TemporaryType] with MustMatchers {
  def fixture[R : AsResult](f: TemporaryType => R) =
    Seq(Posix, Hdfs, S3).toStream.map(t => AsResult(f(t))).reduceLeft(_ and _)
}
