package com.ambiata.ivory.storage.metadata

import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.repository._

object Metadata {

  /* Store */
  def storeFromIvory(repo: Repository, name: String): ResultTIO[FeatureStore] =
    FeatureStoreTextStorage.fromName(repo, name)

  def storeToIvory(repo: Repository, store: FeatureStore, name: String): ResultTIO[Unit] =
    FeatureStoreTextStorage.toName(repo, name, store)

  /* Dictionary */
  def dictionaryFromIvory(repo: Repository): ResultTIO[Dictionary] =
    DictionaryThriftStorage(repo).load

  def dictionaryToIvory(repo: Repository, dictionary: Dictionary): ResultTIO[Unit] =
    DictionaryThriftStorage(repo).store(dictionary).map(_ => ())
}
