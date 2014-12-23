package com.ambiata.ivory.core

import argonaut._, Argonaut._

import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.ambiata.mundane.parse.ListParser

import scalaz.Scalaz._
import scalaz.\&/.This
import scalaz._
import scalaz.effect.IO

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

  implicit def PartitionOrdering: scala.Ordering[Partition] =
    PartitionOrder.toScalaOrdering

  def intervalsByNamespace(ps: List[Partition]): Map[Name, NonEmptyList[(Partition, Partition)]] =
    intervals(ps).groupBy1(_._1.namespace)

  def intervals(ps: List[Partition]): List[(Partition, Partition)] =
    ps.sorted.foldLeft[List[(Partition, Partition)]](Nil)({
      case (Nil, el) =>
        List(el -> el)
      case ((min, max) :: t, el) =>
        if (el.namespace == max.namespace && Date.fromLocalDate(max.date.localDate.plusDays(1)) == el.date)
          (min, el) :: t
        else
          (el, el) :: (min, max) :: t
    }).reverse

  def parseDir(dir: DirPath): Validation[String, Partition] =
    listParser.flatMap(p => ListParser.consumeRest.as(p)).run(dir.components.reverse)

  def parseFile(file: String): Validation[String, Partition] =
    parseFile(FilePath.unsafe(file))

  def parseNamespaceDateKey(key: Key): Validation[String, Partition] =
    parseDir(DirPath.unsafe(key.name))

  def parseFile(file: FilePath): Validation[String, Partition] =
    parseDir(file.dirname)

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

  implicit def PartitionCodecJson: CodecJson[Partition] =
    casecodec2(Partition.apply, Partition.unapply)("name", "date")
}
