package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._
import com.ambiata.ivory.data.Identifier
import com.ambiata.ivory.storage.metadata._
import com.ambiata.mundane.control._
import scalaz._, Scalaz._
import scalaz.effect.IO

object DictionaryImporter {

  import com.ambiata.ivory.operation.ingestion.DictionaryImportValidate._

  def importFromPath(repository: Repository, source: ReferenceIO, importOpts: ImportOpts): ResultTIO[(DictValidation[Unit], Option[DictionaryId])] =
    DictionaryTextStorageV2.fromFileIO(source).flatMap(fromDictionary(repository, _, importOpts))

  def fromDictionary(repository: Repository, dictionary: Dictionary, importOpts: ImportOpts): ResultTIO[(DictValidation[Unit], Option[DictionaryId])] = {
    val storage = DictionaryThriftStorage(repository)
    for {
      oldDictionary <- storage.loadOption.map(_.getOrElse(Dictionary.empty))
      newDictionary = importOpts.ty match {
        case Update => oldDictionary.append(dictionary)
        case Override => dictionary
      }
      validation = validate(oldDictionary, newDictionary) |+| validateSelf(newDictionary)
      doImport = validation.isSuccess || importOpts.force
      dictIdIden <- if (doImport)
        for {
          dictId <- storage.store(newDictionary)
          _      <- Metadata.incrementCommitDictionary(repository, dictId)
        } yield dictId.some
      else None.pure[ResultTIO]
    } yield validation -> dictIdIden
  }

  sealed trait ImportType
  case object Update extends ImportType
  case object Override extends ImportType

  case class ImportOpts(ty: ImportType, force: Boolean)
}
