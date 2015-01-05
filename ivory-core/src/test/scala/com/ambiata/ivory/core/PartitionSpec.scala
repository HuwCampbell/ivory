package com.ambiata.ivory.core

import org.specs2._
import org.scalacheck._, Arbitrary._
import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import scalaz._, Scalaz._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

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
  DirPath                 $key
  String                  $stringPath

Compress a Partition as intervals:
  All input partitions appear in the output intervals $intervals
  When indexed by name, both sides of the range should have the same namespace $indexed

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
    val fp = "a" </> "b" </> "c" </> toDirPath(partition.key) <|> "file"
    Partition.parseFile(fp) ==== partition.success[String]
  })

  def fileIsDir =
    Partition.parseFile("root" </> "ns" </> "2104" </> "08" <|> "11").toOption ==== None

  def fileMalformedDate = prop((malformed: MalformedDateString) =>
    Partition.parseFile("root" </> "ns" </> DirPath.unsafe(malformed.date) <|> "file").toOption ==== None)

  def fileMissingNamespace =
    Partition.parseFile("2014" </> "08" </> "11" <|> "file").toOption ==== None

  def dir = prop((partition: Partition) => {
    val dp = "d" </> "e" </> toDirPath(partition.key)
    Partition.parseDir(dp) ==== partition.success[String]
  })

  def dirIsFile = prop((partition: Partition) => {
    val dp = "d" </> "e" </> toDirPath(partition.key) </> "file"
    Partition.parseDir(dp).toOption ==== None
  })

  def dirIncomplete =
    Partition.parseDir("root" </> "ns" </> "2104" </> "08").toOption ==== None

  def dirMalformedDate = prop((malformed: MalformedDateString) =>
    Partition.parseFile("root" </> "ns" </> FilePath.unsafe(malformed.date)).toOption ==== None)

  def dirMissingNamespace =
    Partition.parseDir("2014" </> "08" </> "11").toOption ==== None

  def key = prop((p: Partition) =>
    Partition.key(p.namespace, p.date) ==== p.key)

  def stringPath = prop((p: Partition) =>
    Partition.stringPath(p.namespace.name, p.date) ==== p.key.name)

  def intervals = prop((ps: List[Partition]) => {
    val result = Partition.intervals(ps)
    ps.forall(p => result.exists({
      case (min, max) =>
        min.namespace == p.namespace &&
          max.namespace == p.namespace &&
          min.date >= p.date &&
          max.date <= p.date
    })) })


  def indexed = prop((ps: List[Partition]) => {
    val result = Partition.intervalsByNamespace(ps)
    result.forall({
      case (n, ps) =>
        ps.toList.forall({
          case (from, to) =>
            from.namespace == n && to.namespace == n
        })
    })
  })

  def toDirPath(key: Key) =
    DirPath.unsafe(key.name)
}
