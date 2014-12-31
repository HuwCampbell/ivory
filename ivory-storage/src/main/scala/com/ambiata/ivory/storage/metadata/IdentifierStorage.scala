package com.ambiata.ivory.storage
package metadata

import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import scodec.bits.ByteVector

import scalaz._, Scalaz._

object IdentifierStorage {

  def get(repository: Repository, key: Key): RIO[Option[Identifier]] = {
    repository.store.listHeads(key)
      .map(_.flatMap(x => Identifier.parse(x.name)).sorted.lastOption)
  }

  def getOrFail(repository: Repository, key: Key): RIO[Identifier] =
    get(repository, key)
      .flatMap(_.fold(RIO.fail[Identifier](s"No identifiers found in $key"))(RIO.ok))

  /**
   * Write the identifier value to a temporary file
   * Then move it to its final location
   *
   *  key / new identifier / keyName
   */
  def write(repository: Repository, key: Key, keyName: KeyName, value: ByteVector): RIO[Identifier] = {
    // TODO This is currently not threadsafe - need to deal with concurrent moves!
    def writeToNextIdentifierFile(temporary: Key): RIO[Identifier] = for {
      current <- get(repository, key)
      next    =  current.flatMap(_.next).getOrElse(Identifier.initial)
      newKey  =  key / next.asKeyName
      _       <- repository.store.move(temporary, newKey / keyName)
    } yield next

    for {
      temporary <- Repository.tmpDir(repository)
      _         <- repository.store.bytes.write(temporary, value)
      id        <- writeToNextIdentifierFile(temporary)
    } yield id
  }

  def latestId(repository: Repository, key: Key): RIO[Option[Identifier]] =
    listIds(repository, key).map(_.lastOption)

  def nextIdOrFail(repository: Repository, key: Key): RIO[Identifier] =
    latestId(repository, key).map(_.map(_.next).getOrElse(Some(Identifier.initial))) >>= (id =>
      RIO.fromOption[Identifier](id, s"No more identifiers left at ${key.name}!"))

  def listIds(repository: Repository, key: Key): RIO[List[Identifier]] = for {
    keys <- repository.store.listHeads(key).map(_.filterHidden)
    ids  <- {
      keys.traverseU(key => RIO.fromOption[Identifier](Identifier.parse(key.name),
        s"""Can not parse id '${key.name}'"""))
    }
  } yield ids.sorted
}
