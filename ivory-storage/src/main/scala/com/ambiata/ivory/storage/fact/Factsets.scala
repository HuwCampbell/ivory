package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.data.StoreDataUtil
import com.ambiata.ivory.storage.repository._

import com.ambiata.mundane.control._
import com.ambiata.mundane.io.FilePath

import scalaz._, Scalaz._, effect._

object Factsets {

  def listIds(repo: Repository): ResultTIO[List[FactsetId]] = for {
    names <- StoreDataUtil.listDir(repo.toStore, Repository.factsets).map(_.map(_.basename.path))
    fids  <- names.traverseU(n => ResultT.fromOption[IO, FactsetId](FactsetId.parse(n), s"Can not parse factset id '${n}'"))
  } yield fids

  def latestId(repo: Repository): ResultTIO[Option[FactsetId]] =
    listIds(repo).map(_.sorted.lastOption)

  // TODO handle locking
  def allocateId(repo: Repository): ResultTIO[FactsetId] = for {
    nextOpt <- latestId(repo).map(_.map(_.next).getOrElse(Some(FactsetId.initial)))
    next    <- ResultT.fromOption[IO, FactsetId](nextOpt, s"No more Factset Ids left!")
    _       <- repo.toStore.bytes.write(Repository.factsets </> FilePath(next.render) </> ".allocated", scodec.bits.ByteVector.empty)
  } yield next
}
