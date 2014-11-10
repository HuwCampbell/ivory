package com.ambiata.ivory.core

import com.ambiata.mundane.parse.ListParser
import com.ambiata.mundane.io.FilePath
import scalaz._

case class Factset(id: FactsetId, partitions: List[Partition]) {
  def show =
    s"""
       |Factset: $id
       |Partitions:
       |  ${partitions.mkString("\n", "\n", "\n")}
     """.stripMargin

  def filterByPartition(pred: Partition => Boolean): Factset =
    Factset(id, partitions.filter(pred))

  def filterByDate(pred: Date => Boolean): Factset =
    filterByPartition(p => pred(p.date))
}

object Factset {

  def parseFile(file: FilePath): Validation[String, (FactsetId, Partition)] =
    pathListParser.run(file.dirname.components.reverse)

  def pathListParser: ListParser[(FactsetId, Partition)] = for {
    partition <- Partition.listParser
    factset   <- FactsetId.listParser
    _         <- ListParser.consumeRest
  } yield (factset, partition)

}
