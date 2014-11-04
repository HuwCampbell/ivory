package com.ambiata.ivory.storage
package metadata

import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import scodec.bits.ByteVector

import scalaz.effect._

object IdentifierStorage { outer =>

  def get(repository: Repository, key: Key): ResultTIO[Option[Identifier]] = {
    repository.store.listHeads(key)
      .map(_.flatMap(x => Identifier.parse(x.name)).sorted.lastOption)
  }

  def getOrFail(repository: Repository, key: Key): ResultTIO[Identifier] =
    get(repository, key)
      .flatMap(_.fold(ResultT.fail[IO, Identifier](s"No identifiers found in $key"))(ResultT.ok))

  /**
   * Write the identifier value to a temporary file
   * Then move it to its final location
   *
   *  key / new identifier / keyName
   */
  def write(repository: Repository, key: Key, keyName: KeyName, value: ByteVector): ResultTIO[Identifier] = {
    // TODO This is currently not threadsafe - need to deal with concurrent moves!
    def writeToNextIdentifierFile(temporary: Key): ResultTIO[Identifier] = for {
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
}
