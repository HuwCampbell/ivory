package com.ambiata.ivory.storage
package metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.data.Identifier
import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import scodec.bits.ByteVector
import java.util.UUID._

import scalaz.effect._
import scalaz.{Store => _, _}, Scalaz._

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
    val temporary = "tmp" / KeyName.unsafe(randomUUID.toString)
    // TODO This is currently not threadsafe - need to deal with concurrent moves!
    def writeToNextIdentifierFile: ResultTIO[Identifier] = for {
      current <- get(repository, key)
      next    =  current.flatMap(_.next).getOrElse(Identifier.initial)
      newKey  =  key / next.asKeyName
      _       <- repository.store.move(temporary, newKey / keyName)
    } yield next

    repository.store.bytes.write(temporary, value) >>
    writeToNextIdentifierFile
  }

}
