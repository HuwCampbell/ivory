package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.DictionaryThriftConversion._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.storage.version._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import scodec.bits.ByteVector

import scalaz.Scalaz._
import scalaz.\&/.This
import scalaz.effect._

case class DictionaryThriftStorage(repository: Repository) {

  val DATA: FileName = "data"
  val dictionaries = repository.dictionaries

  def load: ResultTIO[Dictionary] =
    loadOption.flatMap(ResultT.fromOption(_, s"No dictionaries found"))

  def loadOption: ResultTIO[Option[Dictionary]] =
    loadWithId.map(_.map(_._2))

  def loadMigrate: ResultTIO[Option[(DictionaryId, Dictionary)]] =
    loadWithId.flatMap(_.traverse[ResultTIO, (DictionaryId, Dictionary)] {
      case (Some(id), dict) => ResultT.ok(id -> dict)
      case (_, dict)        => store(dict).map(_ -> dict)
    })

  private def loadWithId: ResultTIO[Option[(Option[DictionaryId], Dictionary)]] =
    IdentifierStorage.get(dictionaries).flatMap {
      case Some(id) => loadFromId(DictionaryId(id)).map(_.map(some(DictionaryId(id)) ->))
      case None     => loadDates.map(_.map(none ->))
    }

  private def loadDates: ResultTIO[Option[Dictionary]] =
    for {
      allDictionaries            <- ReferenceStore.listFiles(dictionaries)
      lastDateDictionaryDirectory = allDictionaries.filter(_.basename.name.matches("""\d{4}-\d{2}-\d{2}"""))
                                                   .sortBy(_.basename.name).reverse.headOption
      result                     <-
        lastDateDictionaryDirectory match {
          case None       => ResultT.ok[IO, Option[Dictionary]](None)
          case Some(last) => DictionaryTextStorage.fromFile(dictionaries </> last.basename)
        }
    } yield result

  def loadFromId(id: DictionaryId): ResultTIO[Option[Dictionary]] =
    loadFromPath(dictionaries </> id.asFileName </> DATA)

  def loadFromPath(reference: Reference[ResultTIO]): ResultTIO[Option[Dictionary]] =
    ReferenceStore.readBytes(reference).flatMap { bytes =>
      ResultT.fromDisjunction(dictionaryFromThrift(ThriftSerialiser().fromBytes1(() => new ThriftDictionary(), bytes.toArray)).leftMap(This.apply))
    }.map(some) ||| ResultT.ok(none)

  def store(dictionary: Dictionary): ResultTIO[DictionaryId] = for {
    bytes <- ResultT.safe[IO, Array[Byte]](ThriftSerialiser().toBytes(dictionaryToThrift(dictionary)))
    id    <- IdentifierStorage.write(dictionaries, DATA, ByteVector(bytes))
    _     <- Version.write(dictionaries </> id, Version(DictionaryVersionOne.toString))
  } yield DictionaryId(id)
}
