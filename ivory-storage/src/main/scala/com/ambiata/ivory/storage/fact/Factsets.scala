package com.ambiata.ivory.storage
package fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._

import scalaz._, Scalaz._, effect._

object Factsets {

  def listIds(repository: Repository): ResultTIO[List[FactsetId]] = for {
    names <- repository.store.listHeads(Repository.factsets).map(_.filterHidden.map(_.name))
    fids  <- names.traverseU(n => ResultT.fromOption[IO, FactsetId](FactsetId.parse(n), s"Can not parse factset id '$n'"))
  } yield fids

  def latestId(repository: Repository): ResultTIO[Option[FactsetId]] =
    listIds(repository).map(_.sorted.lastOption)

  // TODO handle locking
  def allocateFactsetIdI(repository: Repository): IvoryTIO[FactsetId] =
    IvoryT.fromResultTIO { allocateFactsetId(repository) }

  def allocateFactsetId(repository: Repository): ResultTIO[FactsetId] = for {
    nextOpt <- latestId(repository).map(_.map(_.next).getOrElse(Some(FactsetId.initial)))
    next    <- ResultT.fromOption[IO, FactsetId](nextOpt, s"No more Factset Ids left!")
    _       <- repository.store.bytes.write(Repository.factsets / next.asKeyName / ".allocated", scodec.bits.ByteVector.empty)
  } yield next

  def factsets(repository: Repository): ResultTIO[List[Factset]] = for {
    ids      <- listIds(repository)
    factsets <- ids.traverse(id => factset(repository, id))
  } yield factsets.sortBy(_.id)

  def factset(repository: Repository, id: FactsetId): ResultTIO[Factset] =
    Partitions.getFromFactset(repository, id).map(partitions => Factset(id, partitions.sorted))
}
