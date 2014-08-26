package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import DictionaryThriftConversion._
import com.ambiata.ivory.data._
import com.ambiata.ivory.storage._, repository._, store._, version._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import scalaz.\&/.This
import scalaz._, Scalaz._, effect._
import scodec.bits.ByteVector

case class DictionaryThriftStorage(repository: Repository) {

  val DATA = "data".toFilePath
  val store = repository.toStore
  val dictDir = Repository.dictionaries

  def load: ResultTIO[Dictionary] =
    loadOption.flatMap(ResultT.fromOption(_, s"No dictionaries found"))

  def loadOption: ResultTIO[Option[Dictionary]] =
    loadWithId.map(_.map(_._2))

  def loadMigrate: ResultTIO[Option[(Identifier, Dictionary)]] =
    loadWithId.flatMap(_.traverse[ResultTIO, (Identifier, Dictionary)] {
      case (Some(identifier), dict) => ResultT.ok(identifier -> dict)
      case (_, dict)                => store(dict).map(_._1 -> dict)
    })

  private def loadWithId: ResultTIO[Option[(Option[Identifier], Dictionary)]] =
    IdentifierStorage.get(store, dictDir).flatMap {
      case Some(path) => loadFromId(path._1).map(_.map(some(path._1) ->))
      case None       => loadDates.map(_.map(none ->))
    }

  private def loadDates: ResultTIO[Option[Dictionary]] =
    StoreDataUtil.listDir(store, dictDir)
      .map(_.filter(_.basename.path.matches("""\d{4}-\d{2}-\d{2}""")).sortBy(_.basename.path).reverse.headOption)
      .flatMap(_.traverse[ResultTIO, Dictionary] { dictDir => for {
        dictPaths <- store.list(dictDir)
        dicts     <- dictPaths.traverseU(path => DictionaryTextStorage.fromStore(Reference(store, path)))
      } yield dicts.foldLeft(Dictionary(Nil))(_ append _)
    })

  def loadFromId(identifier: Identifier): ResultTIO[Option[Dictionary]] =
    loadFromPath(dictDir </> identifier.render </> DATA)

  def loadFromPath(dictPath: FilePath): ResultTIO[Option[Dictionary]] =
    store.bytes.read(dictPath).flatMap {
      bytes => ResultT.fromDisjunction(dictionaryFromThrift(ThriftSerialiser().fromBytes1(() => new ThriftDictionary(), bytes.toArray)).leftMap(This.apply))
    }.map(some) ||| ResultT.ok(none)

  def store(dictionary: Dictionary): ResultTIO[(Identifier, FilePath)] = for {
    bytes <- ResultT.safe[IO, Array[Byte]](ThriftSerialiser().toBytes(dictionaryToThrift(dictionary)))
    i     <- IdentifierStorage.write(DATA, ByteVector(bytes))(store, dictDir)
    _     <- Version.write(Reference(store, i._2), Version(DictionaryVersionOne.toString))
  } yield i
}
