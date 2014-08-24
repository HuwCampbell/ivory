package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.data.StoreDataUtil
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.repository._

import com.ambiata.mundane.control._
import com.ambiata.mundane.io.FilePath

import scalaz._, Scalaz._, effect._, \&/._

object Factsets {

  def listIds(repo: Repository): ResultTIO[List[FactsetId]] = for {
    names <- StoreDataUtil.listDir(repo.toStore, Repository.factsets).map(_.map(_.basename.path))
    fids  <- names.traverseU(n => ResultT.fromOption[IO, FactsetId](FactsetId.parse(n), s"Can not parse factset id '${n}'"))
  } yield fids

  def latestId(repo: Repository): ResultTIO[Option[FactsetId]] =
    listIds(repo).map(_.sorted.lastOption)

  // TODO handle locking
  def allocateId: IvoryTIO[FactsetId] = IvoryT.fromResultT(repo => for {
    nextOpt <- latestId(repo).map(_.map(_.next).getOrElse(Some(FactsetId.initial)))
    next    <- ResultT.fromOption[IO, FactsetId](nextOpt, s"No more Factset Ids left!")
    _       <- repo.toStore.bytes.write(Repository.factsets </> FilePath(next.render) </> ".allocated", scodec.bits.ByteVector.empty)
  } yield next)

  def factsets(repo: Repository): ResultTIO[List[Factset]] = for {
    ids      <- listIds(repo)
    factsets <- ids.traverse(id => factset(repo, id))
  } yield factsets.sortBy(_.id)

  def factset(repo: Repository, id: FactsetId): ResultTIO[Factset] = for {
    files      <- repo.toStore.list(Repository.factset(id)).map(_.filterHidden)
    partitions <- ResultT.fromDisjunction[IO, List[Partition]](files.traverseU(Partition.parseFile).disjunction.leftMap(This.apply))
  } yield Factset(id, Partitions(partitions.sorted))
}
