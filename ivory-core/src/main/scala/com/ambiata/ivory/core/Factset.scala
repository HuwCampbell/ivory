package com.ambiata.ivory.core

import com.ambiata.mundane.parse.ListParser
import com.ambiata.mundane.io.FilePath
import scalaz._, Scalaz._

case class Factset(id: FactsetId, partitions: Partitions) {
  def show =
    s"""
       |Factset: $id
       |Partitions:
       |  ${partitions.show}
     """.stripMargin

  def filter(f: Partition => Boolean): Factset =
    copy(partitions = partitions.filter(f))
}

object Factset {

  def parseFile(file: FilePath): Validation[String, (FactsetId, Partition)] = for {
    parent <- file.dirname.success[String]
    res    <- pathListParser.run(parent.components.reverse)
  } yield res

  def pathListParser: ListParser[(FactsetId, Partition)] = for {
    partition <- Partition.listParser
    factset   <- FactsetId.listParser
    _         <- ListParser.consumeRest
  } yield (factset, partition)

}
