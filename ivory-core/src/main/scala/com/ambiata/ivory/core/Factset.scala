package com.ambiata.ivory.core

import IvorySyntax._
import com.ambiata.mundane.parse.ListParser
import com.ambiata.mundane.io.FilePath
import scalaz._, Scalaz._

case class Factset(id: FactsetId, partitions: Partitions)

object Factset {

  def parseFile(file: FilePath): Validation[String, (FactsetId, Partition)] = for {
    parent <- file.parent.toSuccess(s"Expecting parent in path '${file}', but got none")
    res    <- pathListParser.run(parent.components.reverse)
  } yield res

  def pathListParser: ListParser[(FactsetId, Partition)] = for {
    partition <- Partition.listParser
    factset   <- FactsetId.listParser
    _         <- ListParser.consumeRest
  } yield (factset, partition)
}
