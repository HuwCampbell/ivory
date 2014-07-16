package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._, DictionaryThriftConversion.dictionary._
import com.ambiata.ivory.data._
import com.ambiata.ivory.storage._, repository._, store._, version._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import scalaz._, Scalaz._, effect._
import scodec.bits.ByteVector

case class DictionaryThriftStorage(repository: Repository) {

  val DATA = "data".toFilePath
  val store = repository.toStore
  val dictDir = Repository.dictionaries

  def load: ResultTIO[Dictionary] =
    loadWithId.map(_._2)

  def loadMigrate: ResultTIO[(Identifier, Dictionary)] =
    loadWithId.flatMap {
      case (Some(identifier), dict) => ResultT.ok(identifier -> dict)
      case (_, dict)                => store(dict).map(_._1 -> dict)
    }

  private def loadWithId: ResultTIO[(Option[Identifier], Dictionary)] =
    IdentifierStorage.getOrFail(store, dictDir).flatMap(path => loadFromId(path._1).map(Some(path._1) ->)) |||
      loadDates.map(none ->) |||
      ResultT.fail(s"No dictionaries found in $dictDir")

  private def loadDates: ResultTIO[Dictionary] = for {
    dictDir   <- StoreDataUtil.listDir(store, dictDir).map(_.filter(_.basename.path.matches("""\d{4}-\d{2}-\d{2}""")).sortBy(_.basename.path).reverse.headOption)
    dictPaths <- dictDir.cata(store.list, ResultT.fail[IO, List[FilePath]](s"No date-based dictionaries found in $dictDir"))
    dicts     <- dictPaths.traverseU(path => DictionaryTextStorage.dictionaryFromHdfs(StorePath(store, path)))
  } yield dicts.foldLeft(Dictionary(Map()))(_ append _)

  def loadFromRef(path: FilePath): ResultTIO[Dictionary] = for {
    idS  <- store.utf8.read(path)
    id   <- ResultT.fromOption[IO, Identifier](Identifier.parse(idS), s"No dictionary ref found at $path")
    dict <- loadFromId(id)
  } yield dict

  def loadFromId(identifier: Identifier): ResultTIO[Dictionary] =
    loadFromPath(dictDir </> identifier.render </> DATA)

  def loadFromPath(dictPath: FilePath): ResultTIO[Dictionary] = for {
    bytes <- store.bytes.read(dictPath)
    dict = ThriftSerialiser().fromBytes1(() => new ThriftDictionary(), bytes.toArray)
  } yield from(dict)

  def store(dictionary: Dictionary): ResultTIO[(Identifier, FilePath)] = for {
    bytes <- ResultT.safe[IO, Array[Byte]](ThriftSerialiser().toBytes(to(dictionary)))
    i <- IdentifierStorage.write(DATA, ByteVector(bytes))(store, dictDir)
    _ <- Version.write(StorePath(store, i._2), Version(DictionaryVersionOne.toString))
  } yield i
}
