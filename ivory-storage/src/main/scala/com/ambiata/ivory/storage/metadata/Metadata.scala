package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control.IvoryT._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.control._

object Metadata {

  /** Feature Store */
  def featureStoreFromIvory(repo: Repository, id: FeatureStoreId): ResultTIO[FeatureStore] =
    FeatureStoreTextStorage.fromId(repo, id)

  def featureStoreFromIvoryT(id: FeatureStoreId): IvoryTIO[FeatureStore] =
    fromResultT(featureStoreFromIvory(_, id))

  def featureStoreToIvory(repo: Repository, featureStore: FeatureStore): ResultTIO[Unit] =
    FeatureStoreTextStorage.toId(repo, featureStore)

  /**
   * This will read the latest FeatureStore, add the given FactsetId to it then persist
   * back to the repository with a new FeatureStoreId
   */
  def incrementFeatureStore(factset: FactsetId): IvoryTIO[FeatureStore] =
    FeatureStoreTextStorage.increment(factset)

  def latestFeatureStoreId(repo: Repository): ResultTIO[Option[FeatureStoreId]] =
    FeatureStoreTextStorage.latestId(repo)

  def latestFeatureStoreIdT: IvoryTIO[Option[FeatureStoreId]] =
    fromResultT(latestFeatureStoreId)

  def listFeatureStoreIds(repo: Repository): ResultTIO[List[FeatureStoreId]] =
    FeatureStoreTextStorage.listIds(repo)

  /** Dictionary */
  def dictionaryFromIvory(repo: Repository): ResultTIO[Dictionary] =
    DictionaryThriftStorage(repo).load

  def dictionaryFromIvoryT: IvoryTIO[Dictionary] =
    fromResultT(dictionaryFromIvory)

  def dictionaryToIvory(repo: Repository, dictionary: Dictionary): ResultTIO[Unit] =
    DictionaryThriftStorage(repo).store(dictionary).map(_ => ())

  def dictionaryToIvoryT(dictionary: Dictionary): IvoryTIO[Unit] =
    fromResultT(dictionaryToIvory(_, dictionary))

}
