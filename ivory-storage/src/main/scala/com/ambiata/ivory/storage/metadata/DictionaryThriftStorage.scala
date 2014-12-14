package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.DictionaryThriftConversion._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.storage.version.{Version => StringVersion}
import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import com.ambiata.poacher.mr.ThriftSerialiser
import scodec.bits.ByteVector

import scalaz.Scalaz._
import scalaz.\&/.This
import scalaz.effect._

case class DictionaryThriftStorage(repository: Repository) {

  val DATA = KeyName.unsafe("data")
  val dictionaries = Repository.dictionaries

  def load: RIO[Dictionary] =
    loadOption.flatMap(ResultT.fromOption(_, s"No dictionaries found"))

  def loadOption: RIO[Option[Dictionary]] =
    loadWithId.map(_.map(_._2))

  def loadMigrate: RIO[Option[(DictionaryId, Dictionary)]] =
    loadWithId.flatMap(_.traverse[RIO, (DictionaryId, Dictionary)] {
      case (Some(id), dict) => ResultT.ok(id -> dict)
      case (_, dict)        => store(dict).map(_ -> dict)
    })

  def loadWithId: RIO[Option[(Option[DictionaryId], Dictionary)]] =
    IdentifierStorage.get(repository, dictionaries).flatMap {
      case Some(id) =>
        loadFromId(DictionaryId(id)).map(_.map(some(DictionaryId(id)) ->))
      case None =>
        loadDates.map(_.map(none ->))
    }

  def loadDates: RIO[Option[Dictionary]] =
    for {
      allDictionaries    <- repository.store.listHeads(dictionaries)
      lastDateDictionary = allDictionaries.filter(_.name.matches("""\d{4}-\d{2}-\d{2}"""))
                                          .sortBy(_.name).reverse.headOption
      result                     <-
        lastDateDictionary match {
          case None       => ResultT.ok[IO, Option[Dictionary]](None)
          case Some(last) => DictionaryTextStorage.fromKeyStore(repository, dictionaries / last).map(Some.apply)
        }
    } yield result

  def loadFromId(id: DictionaryId): RIO[Option[Dictionary]] =
    loadFromPath(dictionaries / id.asKeyName / DATA)

  def loadFromPath(key: Key): RIO[Option[Dictionary]] =
    repository.store.bytes.read(key).flatMap { bytes =>
      ResultT.fromDisjunction(dictionaryFromThrift(ThriftSerialiser().fromBytes1(() => new ThriftDictionary(), bytes.toArray)).leftMap(This.apply))
    }.map(some) ||| ResultT.ok(none)

  def store(dictionary: Dictionary): RIO[DictionaryId] = for {
    bytes <- ResultT.safe[IO, Array[Byte]](ThriftSerialiser().toBytes(dictionaryToThrift(dictionary)))
    id    <- IdentifierStorage.write(repository, dictionaries, DATA, ByteVector(bytes))
    _     <- StringVersion.write(repository, dictionaries / id.asKeyName, StringVersion(DictionaryVersionOne.toString))
  } yield DictionaryId(id)
}
