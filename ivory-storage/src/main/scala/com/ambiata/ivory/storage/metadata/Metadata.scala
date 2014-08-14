package com.ambiata.ivory.storage.metadata

import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._

object Metadata {

  /** Feature Store */
  def storeFromIvory(repo: Repository, id: FeatureStoreId): ResultTIO[FeatureStore] =
    FeatureStoreTextStorage.fromId(repo, id)

  def storeToIvory(repo: Repository, featureStore: FeatureStore): ResultTIO[Unit] =
    FeatureStoreTextStorage.toId(repo, featureStore)

  /**
   * This will read the latest FeatureStore, add the given FactsetId to it then persist
   * back to the repository with a new FeatureStoreId
   */
  def incrementStore(repo: Repository, factset: FactsetId): ResultTIO[FeatureStore] =
    FeatureStoreTextStorage.increment(repo, factset)

  def latestStoreId(repo: Repository): ResultTIO[Option[FeatureStoreId]] =
    FeatureStoreTextStorage.latestId(repo)

  def listStoreIds(repo: Repository): ResultTIO[List[FeatureStoreId]] =
    FeatureStoreTextStorage.listIds(repo)

  /** Dictionary */
  def dictionaryFromIvory(repo: Repository): ResultTIO[Dictionary] =
    DictionaryThriftStorage(repo).load

  def dictionaryToIvory(repo: Repository, dictionary: Dictionary): ResultTIO[Unit] =
    DictionaryThriftStorage(repo).store(dictionary).map(_ => ())
}
