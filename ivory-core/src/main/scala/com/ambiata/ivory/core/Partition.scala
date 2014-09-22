package com.ambiata.ivory.core

import com.ambiata.mundane.io._
import com.ambiata.mundane.store._
import com.ambiata.mundane.parse.ListParser

import scalaz.Scalaz._
import scalaz._

case class Partition(namespace: Name, date: Date) {
  def key: Key =
    Partition.key(namespace, date)

  def order(other: Partition): Ordering =
    namespace ?|? other.namespace match {
      case Ordering.EQ => date ?|? other.date
      case o           => o
    }
}

object Partition {
  implicit def PartitionOrder: Order[Partition] =
    Order.order(_ order _)

  implicit def PartitionOrdering =
    PartitionOrder.toScalaOrdering

  def parseDir(dir: DirPath): Validation[String, Partition] =
    listParser.flatMap(p => ListParser.consumeRest.as(p)).run(dir.components.reverse)

  def parseFile(file: String): Validation[String, Partition] =
    parseFile(FilePath.unsafe(file))

  def parseKey(key: Key): Validation[String, Partition] =
    parseFile(FilePath.unsafe(key.name))

  def parseFile(file: FilePath): Validation[String, Partition] = for {
    parent    <- file.dirname.success[String]
    partition <- parseDir(parent)
  } yield partition

  def listParser: ListParser[Partition] = {
    import com.ambiata.mundane.parse.ListParser._
    for {
      d    <- byte
      m    <- byte
      y    <- short
      date <- Date.create(y, m, d) match {
        case None       => ListParser((position, _) => (position, s"""not a valid date ($y-$m-$d)""").failure)
        case Some(date) => date.point[ListParser]
      }
      ns   <- Name.listParser
    } yield Partition(ns, date)
  }

  def stringPath(namespace: String, date: Date): String =
    namespace + "/" + "%4d/%02d/%02d".format(date.year, date.month, date.day)

  def key(namespace: Name, date: Date): Key =
    namespace.asKeyName / Key.unsafe("%4d/%02d/%02d".format(date.year, date.month, date.day))
}

case class Partitions(partitions: List[Partition]) {
  def sorted: Partitions =
    Partitions(partitions.sorted)

  def isEmpty: Boolean =
    partitions.isEmpty

  def show = partitions.map(_.key.name).mkString("\n", "\n", "\n")

  def filter(f: Partition => Boolean): Partitions =
    Partitions(partitions.filter(f))
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
