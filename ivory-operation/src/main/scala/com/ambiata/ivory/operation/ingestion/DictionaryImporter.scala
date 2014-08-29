package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._
import com.ambiata.ivory.data.Identifier
import com.ambiata.ivory.storage.metadata._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import scalaz._, Scalaz._, effect._

object DictionaryImporter {

  import com.ambiata.ivory.operation.ingestion.DictionaryImportValidate._

  def fromPath(repository: Repository, source: ReferenceIO, importOpts: ImportOpts): ResultTIO[(DictValidation[Unit], Option[FilePath])] =
    DictionaryTextStorageV2.fromStore(source).flatMap(fromDictionary(repository, _, importOpts))

  def fromDictionary(repository: Repository, dictionary: Dictionary, importOpts: ImportOpts): ResultTIO[(DictValidation[Unit], Option[FilePath])] = {
    val storage = DictionaryThriftStorage(repository)
    for {
      oldDictionary <- storage.loadOption.map(_.getOrElse(Dictionary.empty))
      newDictionary = importOpts.ty match {
        case Update => oldDictionary.append(dictionary)
        case Override => dictionary
      }
      validation = validate(oldDictionary, newDictionary) |+| validateSelf(newDictionary)
      doImport = validation.isSuccess || importOpts.force
      path <- if (doImport) storage.store(newDictionary).map(_._2).map(some) else None.pure[ResultTIO]
      
      // Update Commit
      latestFeatureStoreId <- Metadata.latestFeatureStoreId(repository)
      _ <- path.cata(
        (x: FilePath) => (for {
          x <- ResultT.fromOption[IO, Identifier](Identifier.parse(x.basename.path), s"Failed to parse new-ly created dictionary id '$x'")
          y <- latestFeatureStoreId.cata(
             (x: FeatureStoreId) => x.pure[ResultTIO]
            ,{
              println("no feature store present, creating an empty one")
              FeatureStoreTextStorage.increment(repository, FactsetId.initial).map(_.id): ResultTIO[FeatureStoreId]
            })
          } yield CommitTextStorage.increment(repository, Commit(DictionaryId(x), y)))
        , ().pure[ResultTIO])
    } yield validation -> path
  }

  sealed trait ImportType
  case object Update extends ImportType
  case object Override extends ImportType

  case class ImportOpts(ty: ImportType, force: Boolean)
}
