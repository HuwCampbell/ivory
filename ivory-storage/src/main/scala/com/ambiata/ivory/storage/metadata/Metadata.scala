package com.ambiata.ivory.storage.metadata

import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._

object Metadata {

  /* Store */
  def storeFromIvory(repo: Repository, id: FeatureStoreId): ResultTIO[FeatureStore] =
    FeatureStoreTextStorage.fromId(repo, id)

  def storeToIvory(repo: Repository, store: FeatureStore, id: FeatureStoreId): ResultTIO[Unit] =
    FeatureStoreTextStorage.toId(repo, id, store)

  def latestStoreId(repo: Repository): ResultTIO[Option[FeatureStoreId]] =
    FeatureStoreTextStorage.latestId(repo)

  def listStoreIds(repo: Repository): ResultTIO[List[FeatureStoreId]] =
    FeatureStoreTextStorage.listIds(repo)

  /* Dictionary */
  def dictionaryFromIvory(repo: Repository): ResultTIO[Dictionary] =
    DictionaryThriftStorage(repo).load

  def dictionaryToIvory(repo: Repository, dictionary: Dictionary): ResultTIO[Unit] =
    DictionaryThriftStorage(repo).store(dictionary).map(_ => ())
}
