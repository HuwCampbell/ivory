package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control.RepositoryT._
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control._
import scalaz.effect.IO
import scalaz._, Scalaz._

object Metadata {

  /** Feature Store */
  def featureStoreFromIvory(repo: Repository, id: FeatureStoreId): RIO[FeatureStore] =
    FeatureStoreTextStorage.fromId(repo, id)

  def featureStoreFromRepositoryT(id: FeatureStoreId): RepositoryTIO[FeatureStore] =
    fromResultT(featureStoreFromIvory(_, id))

  def featureStoreToIvory(repo: Repository, featureStore: FeatureStore): RIO[Unit] =
    FeatureStoreTextStorage.toId(repo, featureStore)

  /**
   * This will read the latest FeatureStore, add the given FactsetId to it then persist
   * back to the repository with a new FeatureStoreId
   */
  def incrementFeatureStore(factset: List[FactsetId]): RepositoryTIO[FeatureStoreId] = RepositoryT.fromResultTIO(repo =>
    FeatureStoreTextStorage.increment(repo, factset))

  def latestFeatureStoreId(repo: Repository): RIO[Option[FeatureStoreId]] =
    FeatureStoreTextStorage.latestId(repo)

  def latestFeatureStoreIdT: RepositoryTIO[Option[FeatureStoreId]] =
    RepositoryT.fromResultTIO(latestFeatureStoreId)

  /** @return the latest store or fail if there is none */
  def latestFeatureStoreOrFail(repository: Repository): RIO[FeatureStore] =
    latestFeatureStoreIdOrFail(repository).flatMap(id => featureStoreFromIvory(repository, id))

  /** @return the latest store id or fail if there is none */
  def latestFeatureStoreIdOrFail(repository: Repository): RIO[FeatureStoreId] =
    latestFeatureStoreId(repository).flatMap { latest =>
      ResultT.fromOption[IO, FeatureStoreId](latest, s"no store found for this repository ${repository}")
    }

  def listFeatureStoreIds(repo: Repository): RIO[List[FeatureStoreId]] =
    FeatureStoreTextStorage.listIds(repo)

  /** Dictionary */
  def dictionaryFromIvory(repo: Repository, dictionaryId: DictionaryId): RIO[Dictionary] =
    DictionaryThriftStorage(repo).loadFromId(dictionaryId) >>=
      (dictionary => ResultT.fromOption(dictionary, s"Dictionary not found in Ivory '$dictionaryId'"))

  def latestDictionaryFromIvory(repo: Repository): RIO[Dictionary] =
    DictionaryThriftStorage(repo).load

  def latestDictionaryIdFromIvory(repo: Repository): RIO[DictionaryId] = for {
    latestDict         <- DictionaryThriftStorage(repo).loadMigrate.map(_.map(_._1))
    latestDictionaryId <- ResultT.fromOption[IO, DictionaryId](latestDict, "Could not load a dictionary")
  } yield latestDictionaryId

  def latestDictionaryFromRepositoryT: RepositoryTIO[Dictionary] =
    fromResultT(latestDictionaryFromIvory)

  def latestDictionaryIdFromRepositoryT: RepositoryTIO[DictionaryId] =
    fromResultT(latestDictionaryIdFromIvory)

  def dictionaryToIvory(repo: Repository, dictionary: Dictionary): RIO[Unit] =
    DictionaryThriftStorage(repo).store(dictionary).void

  def dictionaryToRepositoryT(dictionary: Dictionary): RepositoryTIO[Unit] =
    fromResultT(dictionaryToIvory(_, dictionary))

  def dictionaryLoadMigrate(repo: Repository): RIO[Option[(DictionaryId, Dictionary)]] =
    DictionaryThriftStorage(repo).loadMigrate

  /** Commit */
  def listCommitIds(repo: Repository): RIO[List[CommitId]] =
    CommitTextStorage.listIds(repo)

  def listCommitIdsT(repo: Repository): RepositoryTIO[List[CommitId]] =
    fromResultT(listCommitIds(_))

  def latestCommitId(repo: Repository): RIO[Option[CommitId]] =
    CommitTextStorage.latestId(repo)

  def findOrCreateLatestCommitId(repo: Repository): RIO[CommitId] = for {
      store <- latestFeatureStoreOrFail(repo)
      dictionaryId <- latestDictionaryIdFromIvory(repo)
      commitId <- CommitTextStorage.findOrCreateLatestId(repo, dictionaryId, store.id)
  } yield commitId

  def latestCommitIdT(repo: Repository): RepositoryTIO[Option[CommitId]] =
    fromResultT(latestCommitId(_))

  def commitFromIvory(repo: Repository, commitId: CommitId): RIO[Commit] =
    CommitTextStorage.fromId(repo, commitId) >>=
      (commit => ResultT.fromOption(commit, s"Commit not found in Ivory '$commitId'"))

  def incrementCommit(repo: Repository, dictionaryId: DictionaryId, featureStoreId: FeatureStoreId,
                      configId: RepositoryConfigId): RIO[CommitId] =
    CommitTextStorage.increment(repo, Commit(dictionaryId, featureStoreId, Some(configId)))

  def incrementCommitDictionary(repo: Repository, dictionaryId: DictionaryId): RIO[CommitId] = for {
    // Don't fail if no feature store exists - create a blank one
    storeId  <- Metadata.latestFeatureStoreId(repo).flatMap(_.cata(_.point[RIO], FeatureStoreTextStorage.increment(repo, Nil)))
    repoRead <- RepositoryRead.fromRepository(repo)
    configId <- latestConfigurationId.run(repoRead)
    commitId <- CommitTextStorage.increment(repo, Commit(dictionaryId, storeId, configId))
  } yield commitId

  def incrementCommitFeatureStore(repo: Repository, featureStoreId: FeatureStoreId): RIO[CommitId] = for {
    latestDictionaryId <- latestDictionaryIdFromIvory(repo)
    repoRead           <- RepositoryRead.fromRepository(repo)
    configId           <- latestConfigurationId.run(repoRead)
    commitId           <- CommitTextStorage.increment(repo, Commit(latestDictionaryId, featureStoreId, configId))
  } yield commitId

  def incrementCommitFeatureStoreT(featureStoreId: FeatureStoreId): RepositoryTIO[CommitId] =
    RepositoryT.fromResultTIO(repo => incrementCommitFeatureStore(repo, featureStoreId))

  def latestConfigurationId: RepositoryTIO[Option[RepositoryConfigId]] =
    RepositoryConfigTextStorage.latestId

  def configuration: RepositoryTIO[RepositoryConfig] =
    RepositoryConfigTextStorage.load
}
