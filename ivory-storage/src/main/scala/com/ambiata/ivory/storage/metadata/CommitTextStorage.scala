package com.ambiata.ivory.storage
package metadata

import com.ambiata.ivory.storage.control.RepositoryRead
import com.ambiata.mundane.parse.ListParser

import scalaz._, Scalaz._, effect._

import com.ambiata.mundane.control._
import com.ambiata.ivory.core._

// 3 lines.
// The first line is dictionaryId: DictionaryId
// The second line is featureStoreId: FeatureStoreId
// The third line is an optional repositoryConfigId: RepositoryConfigId
object CommitTextStorage {

  def increment(repository: Repository, c: Commit): RIO[CommitId] = for {
    latest      <- latestId(repository)
    nextId      <- ResultT.fromOption[IO, CommitId](latest.map(_.next).getOrElse(Some(CommitId.initial)), "Ran out of Commit ids!")
    _           <- storeCommitToId(repository, nextId, c)
  } yield nextId

  def fromId(repository: Repository, id: CommitId): RIO[Option[Commit]] = for {
    commitId <- listIds(repository).map(_.find(_ === id))
    commit   <- commitId.cata(x => for {
        lns  <- repository.store.linesUtf8.read(Repository.commitById(x))
        cmt  <- ResultT.fromDisjunctionString[IO, Commit](fromLines.run(lns).disjunction)
    } yield Some(cmt)
      , none.pure[RIO])
  } yield commit

  def storeCommitToId(repository: Repository, id: CommitId, commit: Commit): RIO[Unit] =
    repository.store.linesUtf8.write(Repository.commitById(id), toLines(commit))

  def toLines(commit: Commit): List[String] =
    List(commit.dictionaryId.render, commit.featureStoreId.render) ++ commit.configId.map(_.render)

  def fromLines: ListParser[Commit] = for {
    did   <- Identifier.listParser.map(DictionaryId.apply)
    fsid  <- Identifier.listParser.map(FeatureStoreId.apply)
    cid   <- ListParser.stringOpt.map(_.flatMap(RepositoryConfigId.parse))
  } yield Commit(did, fsid, cid)

  /**
   * looks for the latest commit id, if there are no commits, it pushes one and returns
   * the id for it.  Be aware that its a potential write operation.
   **/
  def findOrCreateLatestId(repo: Repository, dictionaryId: DictionaryId, featureStoreId: FeatureStoreId): RIO[CommitId] = for {
    oCommitId <- latestId(repo)
    commitId <- oCommitId match {
      case Some(x)  => x.pure[RIO]
      case None     =>
        RepositoryRead.fromRepository(repo) >>= (read =>
        RepositoryConfigTextStorage.latestId.run(read) >>= (rcid =>
          increment(repo, Commit(dictionaryId, featureStoreId, rcid))))
    }
  } yield commitId

  def latestId(repo: Repository): RIO[Option[CommitId]] =
    IdentifierStorage.latestId(repo, Repository.commits).map(_.map(CommitId.apply))

  def listIds(repo: Repository): RIO[List[CommitId]] =
    IdentifierStorage.listIds(repo, Repository.commits).map(_.map(CommitId.apply))
}
