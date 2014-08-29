package com.ambiata.ivory.core

import org.specs2._
import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.mundane.io._

import scalaz._, Scalaz._

class PartitionSpec extends Specification with ScalaCheck { def is = s2"""

Partition Tests
----------

Parsing a Partition from a file:
  Succeeds with a valid path                          $file
  Fails when the path is the partition dir            $fileIsDir
  Fails when the path contains a malformed date       $fileMalformedDate
  Fails when the namespace is missing                 $fileMissingNamespace

Parsing a Partition from a dir:
  Succeeds for a dir path                             $dir
  Fails when the path is a file                       $dirIsFile
  Fails when the path isn't complete                  $dirIncomplete
  Fails when the path contains a malformed date       $dirMalformedDate
  Fails when the namespace is missing                 $dirMissingNamespace

Can create a Partition path as a:
  DirPath                 $dirPath
  String                  $stringPath

Can filter Partitions:
  Between two dates       $between
  Before a date           $before
  After a date            $after

"""

  case class MalformedDateString(date: String)
  implicit def MalformedDateStringArbitrary: Arbitrary[MalformedDateString] =
    Arbitrary(for {
      date <- arbitrary[Date]
      str  <- Gen.oneOf("bad/%02d/%02d".format(date.month, date.day)
                      , "%4d/bad/%02d".format(date.year, date.day)
                      , "%4d/%02d/bad".format(date.year, date.month))
    } yield MalformedDateString(str))

  def file = prop((partition: Partition) => {
    val fp = "a" </> "b" </> "c" </> partition.path <|> "file"
    Partition.parseFile(fp) ==== partition.success[String]
  })

  def fileIsDir =
    Partition.parseFile("root" </> "ns" </> "2104" </> "08" <|> "11").toOption ==== None

  def fileMalformedDate = prop((malformed: MalformedDateString) =>
    Partition.parseFile("root" </> "ns" </> DirPath.unsafe(malformed.date) <|> "file").toOption ==== None)

  def fileMissingNamespace =
    Partition.parseFile("2014" </> "08" </> "11" <|> "file").toOption ==== None

  def dir = prop((partition: Partition) => {
    val dp = "d" </> "e" </> partition.path
    Partition.parseDir(dp) ==== partition.success[String]
  })

  def dirIsFile = prop((partition: Partition) => {
    val dp = "d" </> "e" </> partition.path </> "file"
    Partition.parseDir(dp).toOption ==== None
  })

  def dirIncomplete =
    Partition.parseDir("root" </> "ns" </> "2104" </> "08").toOption ==== None

  def dirMalformedDate = prop((malformed: MalformedDateString) =>
    Partition.parseFile("root" </> "ns" </> FilePath.unsafe(malformed.date)).toOption ==== None)

  def dirMissingNamespace =
    Partition.parseDir("2014" </> "08" </> "11").toOption ==== None

  def dirPath = prop((p: Partition) =>
    Partition.dirPath(p.namespace, p.date) ==== p.path)

  def stringPath = prop((p: Partition) =>
    Partition.stringPath(p.namespace.name, p.date) ==== p.path.path)

  def between = prop((partitions: Partitions, dates: UniqueDates) => {
    val ps = partitions.partitions
    val expected = ps.filter(p => p.date.isAfterOrEqual(dates.earlier) && p.date.isBeforeOrEqual(dates.later))
    Partitions.pathsBetween(ps, dates.earlier, dates.later) must_== expected
  })

  def before = prop((partitions: Partitions, date: Date) => {
    val ps = partitions.partitions
    Partitions.pathsBeforeOrEqual(ps, date) must_== ps.filter(_.date.isBeforeOrEqual(date))
  })

  def after = prop((partitions: Partitions, date: Date) => {
    val ps = partitions.partitions
    Partitions.pathsAfterOrEqual(ps, date) must_== ps.filter(_.date.isAfterOrEqual(date))
  })
}
