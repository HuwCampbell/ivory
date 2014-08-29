package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import scalaz._, Scalaz._, effect._, \&/._

object Factsets {

  def listIds(repository: Repository): ResultTIO[List[FactsetId]] = for {
    names <- ReferenceStore.listDirs(repository.factsets).map(_.filterHidden.map(_.basename.name))
    fids  <- names.traverseU(n => ResultT.fromOption[IO, FactsetId](FactsetId.parse(n), s"Can not parse factset id '$n'"))
  } yield fids

  def latestId(repository: Repository): ResultTIO[Option[FactsetId]] =
    listIds(repository).map(_.sorted.lastOption)

  // TODO handle locking
  def allocateFactsetId(repository: Repository): ResultTIO[FactsetId] = for {
    nextOpt <- latestId(repository).map(_.map(_.next).getOrElse(Some(FactsetId.initial)))
    next    <- ResultT.fromOption[IO, FactsetId](nextOpt, s"No more Factset Ids left!")
    _       <- ReferenceStore.writeBytes(repository.factsets </> next.asFileName </> ".allocated", scodec.bits.ByteVector.empty)
  } yield next

  def factsets(repository: Repository): ResultTIO[List[Factset]] = for {
    ids      <- listIds(repository)
    factsets <- ids.traverse(id => factset(repository, id))
  } yield factsets.sortBy(_.id)

  def factset(repository: Repository, id: FactsetId): ResultTIO[Factset] = for {
    files      <- ReferenceStore.list(repository.factset(id)).map(_.filterHidden)
    partitions <- ResultT.fromDisjunction[IO, List[Partition]](files.traverseU(Partition.parseFile).disjunction.leftMap(This.apply))
  } yield Factset(id, Partitions(partitions.sorted))
}
