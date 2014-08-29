package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.data.Identifier
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import scodec.bits.ByteVector
import java.util.UUID._

import scalaz.effect._
import scalaz.{Store => _, _}, Scalaz._

object IdentifierStorage {

  def get(ref: Reference[ResultTIO]): ResultTIO[Option[Identifier]] = {
    ReferenceStore.listDirs(ref)
      .map(_.filterHidden)
      .map(_.flatMap(x => Identifier.parse(x.basename.name)).sorted.lastOption)
  }

  def getOrFail(ref: Reference[ResultTIO]): ResultTIO[Identifier] =
    get(ref)
      .flatMap(_.fold(ResultT.fail[IO, Identifier](s"No identifiers found in $ref"))(ResultT.ok))

  /**
   * Write the identifier value to a temporary file under the path:
   * Then move it to its final location
   *
   *  ref.path </> new identifier </> fileName
   */
  def write(ref: Reference[ResultTIO], fileName: FileName, value: ByteVector): ResultTIO[Identifier] = {
    val temporary = "tmp" <|> FileName.unsafe(randomUUID.toString)
    // TODO This is currently not threadsafe - need to deal with concurrent moves!
    def writeToNextIdentifierFile: ResultTIO[Identifier] = for {
      current <- get(ref)
      next    =  current.flatMap(_.next).getOrElse(Identifier.initial)
      newPath =  ref.path </> next.asFileName
      _       <- ref.store.move(temporary, newPath <|> fileName)
    } yield next

    ref.store.bytes.write(temporary, value) >>
    writeToNextIdentifierFile
  }
}
