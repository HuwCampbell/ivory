package com.ambiata.ivory.core

import java.io.File

import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.mundane.io.FilePath
import com.ambiata.mundane.parse.ListParser

import scalaz.Scalaz._
import scalaz._

case class Partition(namespace: String, date: Date) {
  def path: FilePath =
    FilePath(Partition.stringPath(namespace, date))

  def order(other: Partition): Ordering =
    (namespace ?|? other.namespace) match {
      case Ordering.EQ => date ?|? other.date
      case o           => o
    }
}

object Partition {
  implicit def PartitionOrder: Order[Partition] =
    Order.order(_ order _)

  implicit def PartitionOrdering =
    PartitionOrder.toScalaOrdering

  def parseDir(dir: FilePath): Validation[String, Partition] =
    listParser.flatMap(p => ListParser.consumeRest.map(_ => p)).run(dir.components.reverse)

  def parseFile(file: FilePath): Validation[String, Partition] = for {
    parent    <- file.parent.toSuccess(s"Expecting parent in path '${file}', but got none")
    partition <- parseDir(parent)
  } yield partition

  def listParser: ListParser[Partition] = {
    import com.ambiata.mundane.parse.ListParser._
    // TODO remove once we denend on a version of mundane which contains this combinator
    def byte: ListParser[Byte] = for {
      s         <- string
      position  <- getPosition
      result    <- value(s.parseByte.leftMap(_ => s"""not a byte: '$s'"""))
    } yield result

    for {
      d    <- byte
      m    <- byte
      y    <- short
      date <- Date.create(y, m, d) match {
        case None       => ListParser((position, _) => (position, s"""not a valid date ($y-$m-$d)""").failure)
        case Some(date) => date.point[ListParser]
      }
      ns   <- string.nonempty
    } yield Partition(ns, date)
  }

  def stringPath(ns: String, date: Date): String =
    ns + "/" + "%4d/%02d/%02d".format(date.year, date.month, date.day)
}

case class Partitions(partitions: List[Partition]) {
  def sorted: Partitions =
    Partitions(partitions.sorted)

  def isEmpty: Boolean =
    partitions.isEmpty
}

object Partitions {

  /** Filter paths before or equal to a given date */
  def pathsBeforeOrEqual(partitions: List[Partition], to: Date): List[Partition] =
    partitions.filter(_.date.isBeforeOrEqual(to))

  /** Filter paths after or equal to a given date */
  def pathsAfterOrEqual(partitions: List[Partition], from: Date): List[Partition] =
    partitions.filter(_.date.isAfterOrEqual(from))

  /** Filter paths between two dates (inclusive) */
  def pathsBetween(partitions: List[Partition], from: Date, to: Date): List[Partition] =
    pathsBeforeOrEqual(pathsAfterOrEqual(partitions, from), to)
}
