package com.ambiata.ivory.core

import com.ambiata.mundane.parse.ListParser
import com.ambiata.mundane.io.FilePath
import scalaz._, Scalaz._

case class Factset(id: FactsetId, format: FactsetFormat, partitions: List[Sized[Partition]]) {
  def show =
    s"""
       |Factset: $id
       |Partitions:
       |  ${partitions.mkString("\n", "\n", "\n")}
     """.stripMargin

  def filterByPartition(pred: Partition => Boolean): Factset =
    Factset(id, format, partitions.filter(p => pred(p.value)))

  def filterByDate(pred: Date => Boolean): Factset =
    filterByPartition(p => pred(p.date))

  def bytes: Bytes =
    partitions.foldMap(_.bytes)
}

object Factset {
  implicit def FactsetEqual: Equal[Factset] =
    Equal.equalA

  def parseFile(file: FilePath): Validation[String, (FactsetId, Partition)] =
    pathListParser.run(file.dirname.components.reverse)

  def pathListParser: ListParser[(FactsetId, Partition)] = for {
    partition <- Partition.listParser
    factset   <- FactsetId.listParser
    _         <- ListParser.consumeRest
  } yield (factset, partition)
}
