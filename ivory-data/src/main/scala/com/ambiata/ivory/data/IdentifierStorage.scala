package com.ambiata.ivory.data

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store._
import scalaz.{Store => _, _}, effect._
import scodec.bits.ByteVector

object IdentifierStorage {

  // NOTE: The FilePath must be _relative_ to the Store
  type StoreKeeper[A] = (Store[ResultTIO], FilePath) => ResultTIO[A]
  type IdentityPath = (Identifier, FilePath)

  def get: StoreKeeper[Option[IdentityPath]] = { (store, dir) =>
    StoreDataUtil.listDirWithoutDotFiles(store, dir)
      .map(_.flatMap(x => Identifier.parse(x.basename.path)).sorted.lastOption)
      .map(_.map(i => i -> (dir </> i.render)))
  }

  def getOrFail: StoreKeeper[IdentityPath] =
    (store, dir) => get(store, dir)
      .flatMap(_.fold(ResultT.fail[IO, IdentityPath](s"No identifiers found in $dir"))(ResultT.ok))

  def write(path: FilePath, value: ByteVector): StoreKeeper[IdentityPath] = { (store, dir) =>
    val uuid = java.util.UUID.randomUUID.toString
    val from = FilePath("tmp") </> uuid
    def next = for {
      i <- get(store, dir).map(_.map(_._1).flatMap(_.next).getOrElse(Identifier.initial))
      newPath = dir </> i.render
      // TODO This is currently not threadsafe - need to deal with concurrent moves!
      _ <- store.move(from, newPath </> path)
    } yield (i, newPath)
    for {
      _ <- store.bytes.write(from, value)
      i <- next
    } yield i
  }
}
