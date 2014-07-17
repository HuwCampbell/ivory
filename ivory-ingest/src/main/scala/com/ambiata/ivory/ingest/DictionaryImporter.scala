package com.ambiata.ivory.ingest

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import scalaz._, Scalaz._, effect._

// FIX move to com.ambiata.ivory.ingest.internal
object DictionaryImporter {

  import DictionaryImportValidate._

  def fromPath(repository: Repository, source: StorePathIO, importOpts: ImportOpts): ResultTIO[(DictValidation[Unit], Option[FilePath])] =
    DictionaryTextStorage.dictionaryFromHdfs(source).flatMap(fromDictionary(repository, _, importOpts))

  def fromDictionary(repository: Repository, dictionary: Dictionary, importOpts: ImportOpts): ResultTIO[(DictValidation[Unit], Option[FilePath])] = {
    val storage = DictionaryThriftStorage(repository)
    for {
      oldDictionary <- storage.loadOption.map(_.getOrElse(Dictionary(Map())))
      newDictionary = importOpts.ty match {
        case Update => oldDictionary.append(dictionary)
        case Override => dictionary
      }
      validation = validate(oldDictionary, newDictionary)
      doImport = validation.isSuccess || importOpts.force
      path <- if (doImport) storage.store(newDictionary).map(_._2).map(some) else None.pure[ResultTIO]
    } yield validation -> path
  }

  sealed trait ImportType
  case object Update extends ImportType
  case object Override extends ImportType

  case class ImportOpts(ty: ImportType, force: Boolean)
}
