package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.DictionaryThriftConversion._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.data._
import com.ambiata.ivory.storage.version._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import scodec.bits.ByteVector

import scalaz.Scalaz._
import scalaz.\&/.This
import scalaz.effect._

case class DictionaryThriftStorage(repository: Repository) {

  val DATA = "data".toFilePath
  val store = repository.toStore
  val dictDir = Repository.dictionaries

  def load: ResultTIO[Dictionary] =
    loadOption.flatMap(ResultT.fromOption(_, s"No dictionaries found"))

  def loadOption: ResultTIO[Option[Dictionary]] =
    loadWithId.map(_.map(_._2))

  def loadMigrate: ResultTIO[Option[(DictionaryId, Dictionary)]] =
    loadWithId.flatMap(_.traverse[ResultTIO, (DictionaryId, Dictionary)] {
      case (Some(identifier), dict) => ResultT.ok(DictionaryId(identifier) -> dict)
      case (_, dict)                => store(dict).map(_ -> dict)
    })

  private def loadWithId: ResultTIO[Option[(Option[Identifier], Dictionary)]] =
    IdentifierStorage.get(store, dictDir).flatMap {
      case Some(path) => loadFromId(DictionaryId(path._1)).map(_.map(some(path._1) ->))
      case None       => loadDates.map(_.map(none ->))
    }

  private def loadDates: ResultTIO[Option[Dictionary]] =
    StoreDataUtil.listDir(store, dictDir)
      .map(_.filter(_.basename.path.matches("""\d{4}-\d{2}-\d{2}""")).sortBy(_.basename.path).reverse.headOption)
      .flatMap(_.traverse[ResultTIO, Dictionary] { dictDir => for {
        dictPaths <- store.list(dictDir)
        dicts     <- dictPaths.traverseU(path => DictionaryTextStorage.fromStore(Reference(store, path)))
      } yield dicts.foldLeft(Dictionary.empty)(_ append _)
    })

  def loadFromId(identifier: DictionaryId): ResultTIO[Option[Dictionary]] =
    loadFromPath(dictDir </> identifier.render </> DATA)

  def loadFromPath(dictPath: FilePath): ResultTIO[Option[Dictionary]] =
    store.bytes.read(dictPath).flatMap {
      bytes => ResultT.fromDisjunction(dictionaryFromThrift(ThriftSerialiser().fromBytes1(() => new ThriftDictionary(), bytes.toArray)).leftMap(This.apply))
    }.map(some) ||| ResultT.ok(none)

  def store(dictionary: Dictionary): ResultTIO[DictionaryId] = for {
    bytes <- ResultT.safe[IO, Array[Byte]](ThriftSerialiser().toBytes(dictionaryToThrift(dictionary)))
    i     <- IdentifierStorage.write(DATA, ByteVector(bytes))(store, dictDir)
    _     <- Version.write(Reference(store, i._2), Version(DictionaryVersionOne.toString))
  } yield DictionaryId(i._1)
}
