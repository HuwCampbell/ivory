package com.ambiata.ivory.ingest

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import scalaz._

// FIX move to com.ambiata.ivory.ingest.internal
object DictionaryImporter {

  def fromPath(repository: Repository, source: StorePathIO): ResultTIO[FilePath] =
    DictionaryTextStorage.DictionaryTextLoader(source).load.flatMap(fromDictionary(repository, _))

  def fromDictionary(repository: Repository, dictionary: Dictionary): ResultTIO[FilePath] =
    DictionaryThriftStorage(repository).store(dictionary).map(_._2)
}
