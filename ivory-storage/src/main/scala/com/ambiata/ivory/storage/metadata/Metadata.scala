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

  def findFactsets(repo: Repository): RIO[List[FactsetId]] = for {
    ids <- FeatureStoreTextStorage.listIds(repo)
    fs  <- ids.traverseU(id => FeatureStoreTextStorage.storeIdsFromId(repo, id))
  } yield fs.flatten.map(_.value).distinct

  def featureStoreFromRepositoryT(id: FeatureStoreId): RepositoryTIO[FeatureStore] =
    fromRIO(featureStoreFromIvory(_, id))

  def featureStoreToIvory(repo: Repository, featureStore: FeatureStore): RIO[Unit] =
    FeatureStoreTextStorage.toId(repo, featureStore)

  /**
   * This will read the latest FeatureStore, add the given FactsetId to it then persist
   * back to the repository with a new FeatureStoreId
   */
  def incrementFeatureStore(factset: List[FactsetId]): RepositoryTIO[FeatureStoreId] = RepositoryT.fromRIO(repo =>
    FeatureStoreTextStorage.increment(repo, factset))

  def latestFeatureStoreId(repo: Repository): RIO[Option[FeatureStoreId]] =
    FeatureStoreTextStorage.latestId(repo)

  def latestFeatureStoreIdT: RepositoryTIO[Option[FeatureStoreId]] =
    RepositoryT.fromRIO(latestFeatureStoreId)

  /** @return the latest store or fail if there is none */
  def latestFeatureStoreOrFail(repository: Repository): RIO[FeatureStore] =
    latestFeatureStoreIdOrFail(repository).flatMap(id => featureStoreFromIvory(repository, id))

  /** @return the latest store id or fail if there is none */
  def latestFeatureStoreIdOrFail(repository: Repository): RIO[FeatureStoreId] =
    latestFeatureStoreId(repository).flatMap { latest =>
      RIO.fromOption[FeatureStoreId](latest, s"no store found for this repository ${repository}")
    }

  def listFeatureStoreIds(repo: Repository): RIO[List[FeatureStoreId]] =
    FeatureStoreTextStorage.listIds(repo)

  /** Dictionary */
  def dictionaryFromIvory(repo: Repository, dictionaryId: DictionaryId): RIO[Dictionary] =
    DictionaryThriftStorage(repo).loadFromId(dictionaryId) >>=
      (dictionary => RIO.fromOption(dictionary, s"Dictionary not found in Ivory '$dictionaryId'"))

  def latestDictionaryFromIvory(repo: Repository): RIO[Dictionary] =
    DictionaryThriftStorage(repo).load

  def latestDictionaryIdFromIvory(repo: Repository): RIO[DictionaryId] = for {
    latestDict         <- DictionaryThriftStorage(repo).loadMigrate.map(_.map(_._1))
    latestDictionaryId <- RIO.fromOption[DictionaryId](latestDict, "Could not load a dictionary")
  } yield latestDictionaryId

  def latestDictionaryFromRepositoryT: RepositoryTIO[Dictionary] =
    fromRIO(latestDictionaryFromIvory)

  def latestDictionaryIdFromRepositoryT: RepositoryTIO[DictionaryId] =
    fromRIO(latestDictionaryIdFromIvory)

  def dictionaryToIvory(repo: Repository, dictionary: Dictionary): RIO[Unit] =
    DictionaryThriftStorage(repo).store(dictionary).void

  def dictionaryToRepositoryT(dictionary: Dictionary): RepositoryTIO[Unit] =
    fromRIO(dictionaryToIvory(_, dictionary))

  def dictionaryLoadMigrate(repo: Repository): RIO[Option[(DictionaryId, Dictionary)]] =
    DictionaryThriftStorage(repo).loadMigrate

  /** Commit */
  def listCommitIds(repo: Repository): RIO[List[CommitId]] =
    CommitStorage.listIds(repo)

  def listCommitIdsT: RepositoryTIO[List[CommitId]] =
    fromRIO(listCommitIds(_))

  def latestCommitId(repo: Repository): RIO[Option[CommitId]] =
    CommitStorage.latestId(repo)

  def findOrCreateLatestCommitId(repo: Repository): RIO[CommitId] = for {
      store <- latestFeatureStoreOrFail(repo)
      dictionaryId <- latestDictionaryIdFromIvory(repo)
      commitId <- CommitStorage.findOrCreateLatestId(repo, dictionaryId, store.id)
  } yield commitId

  def latestCommitIdT(repo: Repository): RepositoryTIO[Option[CommitId]] =
    fromRIO(latestCommitId(_))

  def commitFromIvory(repo: Repository, commitId: CommitId): RIO[CommitMetadata] =
    CommitStorage.fromId(repo, commitId) >>=
      (commit => RIO.fromOption(commit, s"Commit not found in Ivory '$commitId'"))

  def incrementCommit(repo: Repository, dictionaryId: DictionaryId, featureStoreId: FeatureStoreId,
                      configId: RepositoryConfigId): RIO[CommitId] =
    CommitStorage.increment(repo, CommitMetadata(dictionaryId, featureStoreId, Some(configId)))

  def incrementCommitDictionary(repo: Repository, dictionaryId: DictionaryId): RIO[CommitId] =
    RepositoryT.runWithRepo(repo, incrementCommitOpts(Some(dictionaryId), None, None))

  def incrementCommitFeatureStore(repo: Repository, featureStoreId: FeatureStoreId): RIO[CommitId] =
    RepositoryT.runWithRepo(repo, incrementCommitFeatureStoreT(featureStoreId))

  def incrementCommitFeatureStoreT(featureStoreId: FeatureStoreId): RepositoryTIO[CommitId] =
    incrementCommitOpts(None, Some(featureStoreId), None)

  def incrementCommitRepositoryConfig(configId: RepositoryConfigId): RepositoryTIO[CommitId] =
    incrementCommitOpts(None, None, Some(configId))

  def incrementCommitOpts(dictionaryIdOpt: Option[DictionaryId],
                          storeIdOpt: Option[FeatureStoreId],
                          configIdOpt: Option[RepositoryConfigId]): RepositoryTIO[CommitId] = for {
    dictionaryId <- dictionaryIdOpt.cata(_.pure[RepositoryTIO], latestDictionaryIdFromRepositoryT)
    // Don't fail if no feature store exists - create a blank one
    storeId      <- storeIdOpt.cata(_.pure[RepositoryTIO],
      Metadata.latestFeatureStoreIdT.flatMap(_.cata(_.point[RepositoryTIO], RepositoryT.fromRIO(repo => FeatureStoreTextStorage.increment(repo, Nil)))))
    configId     <- configIdOpt.cata(_.some.pure[RepositoryTIO], latestConfigurationId)
    commitId     <- RepositoryT.fromRIO(repo => CommitStorage.increment(repo, CommitMetadata(dictionaryId, storeId, configId)))
  } yield commitId

  def latestConfigurationId: RepositoryTIO[Option[RepositoryConfigId]] =
    RepositoryConfigTextStorage.latestId

  def configuration: RepositoryTIO[RepositoryConfig] =
    RepositoryConfigTextStorage.load
}
