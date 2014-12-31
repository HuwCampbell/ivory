package com.ambiata.ivory.storage
package metadata

import com.ambiata.ivory.storage.control.{RepositoryT, RepositoryRead}
import com.ambiata.mundane.parse.ListParser

import scalaz._, Scalaz._

import com.ambiata.mundane.control._
import com.ambiata.ivory.core._

// 3 lines.
// The first line is dictionaryId: DictionaryId
// The second line is featureStoreId: FeatureStoreId
// The third line is an optional repositoryConfigId: RepositoryConfigId
object CommitStorage {
  def head(repository: Repository): RIO[Commit] =
    latestIdOrFail(repository).flatMap(byIdOrFail(repository, _))

  def byId(repository: Repository, id: CommitId): RIO[Option[Commit]] = for {
    exists <- repository.store.exists(Repository.commitById(id))
    x <- if (exists) for {
             lines <- repository.store.linesUtf8.read(Repository.commitById(id))
             metadata <- RIO.fromDisjunctionString(fromLines.run(lines).disjunction)
             commit <- hydrate(repository, metadata)
           } yield commit.some
         else
           none[Commit].pure[RIO]
  } yield x

  def byIdOrFail(repository: Repository, id: CommitId): RIO[Commit] =
    byId(repository, id).flatMap({
      case Some(commit) =>
        commit.pure[RIO]
      case None =>
        RIO.fail(s"Ivory invariant violated, no commit id $id.")
    })

  def hydrate(repository: Repository, metadata: CommitMetadata): RIO[Commit] = for {
    dictionary <- Metadata.dictionaryFromIvory(repository, metadata.dictionaryId)
    read <- RepositoryRead.fromRepository(repository)
    config <- metadata.configId.traverseU(id => RepositoryConfigTextStorage.loadById(id).run(read).map(id -> _))
    store <- FeatureStoreTextStorage.fromId(repository, metadata.featureStoreId)
  } yield Commit(metadata.dictionaryId, dictionary, store, config)

  def increment(repository: Repository, c: CommitMetadata): RIO[CommitId] = for {
    latest      <- latestId(repository)
    nextId      <- RIO.fromOption[CommitId](latest.map(_.next).getOrElse(Some(CommitId.initial)),
      s"""The number of possible commits ids for this Ivory repository has exceeded the limit (which is quite large).
         |
         |${Crash.raiseIssue}
      """.stripMargin)
    _           <- storeCommitToId(repository, nextId, c)
  } yield nextId

  def fromId(repository: Repository, id: CommitId): RIO[Option[CommitMetadata]] = for {
    commitId <- listIds(repository).map(_.find(_ === id))
    commit   <- commitId.cata(x => for {
        lns  <- repository.store.linesUtf8.read(Repository.commitById(x))
        cmt  <- RIO.fromDisjunctionString[CommitMetadata](fromLines.run(lns).disjunction)
    } yield Some(cmt)
      , none.pure[RIO])
  } yield commit

  def storeCommitToId(repository: Repository, id: CommitId, commit: CommitMetadata): RIO[Unit] =
    repository.store.linesUtf8.write(Repository.commitById(id), toLines(commit))

  def toLines(commit: CommitMetadata): List[String] =
    List(commit.dictionaryId.render, commit.featureStoreId.render) ++ commit.configId.map(_.render)

  def fromLines: ListParser[CommitMetadata] = for {
    did   <- Identifier.listParser.map(DictionaryId.apply)
    fsid  <- Identifier.listParser.map(FeatureStoreId.apply)
    cid   <- ListParser.stringOpt.map(_.flatMap(RepositoryConfigId.parse))
  } yield CommitMetadata(did, fsid, cid)

  /**
   * looks for the latest commit id, if there are no commits, it pushes one and returns
   * the id for it.  Be aware that its a potential write operation.
   */
  def findOrCreateLatestId(repo: Repository, dictionaryId: DictionaryId, featureStoreId: FeatureStoreId): RIO[CommitId] = for {
    oCommitId <- latestId(repo)
    commitId <- oCommitId match {
      case Some(x)  => x.pure[RIO]
      case None     =>
        RepositoryT.runWithRepo(repo, RepositoryConfigTextStorage.latestId) >>= (rcid =>
          increment(repo, CommitMetadata(dictionaryId, featureStoreId, rcid)))
    }
  } yield commitId

  def latestId(repository: Repository): RIO[Option[CommitId]] =
    IdentifierStorage.latestId(repository, Repository.commits).map(_.map(CommitId.apply))

  def latestIdOrFail(repository: Repository): RIO[CommitId] =
    latestId(repository).flatMap({
      case Some(id) =>
        id.pure[RIO]
      case None =>
        RIO.fail("Ivory invariant violated, no repository commit.")
    })

  def listIds(repo: Repository): RIO[List[CommitId]] =
    IdentifierStorage.listIds(repo, Repository.commits).map(_.map(CommitId.apply))
}
