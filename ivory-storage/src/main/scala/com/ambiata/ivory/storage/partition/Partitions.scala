package com.ambiata.ivory.storage.partition

import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import com.ambiata.ivory.core._
import scalaz._, Scalaz._, \&/._, effect.IO

object Partitions {
  def getFromFactset(repository: Repository, factset: FactsetId): ResultTIO[List[Partition]] =
    for {
      keys       <- repository.store.list(Repository.factset(factset)).map(_.map(_.dropRight(1).drop(2)).filter(_ != Key.Root).distinct)
      partitions <- keys.traverseU(key => ResultT.fromDisjunction[IO, Partition](Partition.parseNamespaceDateKey(key).disjunction.leftMap(This.apply)))
    } yield partitions.sorted
}
