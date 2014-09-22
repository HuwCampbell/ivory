package com.ambiata.ivory.storage
package metadata

import scalaz._, Scalaz._, effect._

import com.ambiata.mundane.control._
import com.ambiata.ivory.data._
import com.ambiata.ivory.core._

// 2 lines.
// The first line is dictionaryId: DictionaryId
// The second line is featureStoreId: FeatureStoreId
object CommitTextStorage extends TextStorage[DictionaryId \/ FeatureStoreId, Commit] {

  val name = "commit"

  def increment(repository: Repository, c: Commit): ResultTIO[CommitId] = for {
    latest      <- latestId(repository)
    nextId      <- ResultT.fromOption[IO, CommitId](latest.map(_.next).getOrElse(Some(CommitId.initial)), "Ran out of Commit ids!")
    _           <- storeCommitToId(repository, nextId, c)
  } yield nextId

  def fromId(repository: Repository, id: CommitId): ResultTIO[Option[Commit]] = for {
    commitId <- listIds(repository).map(_.find(_ === id))
    commit   <- commitId.cata(x =>
        fromKeyStore(repository, Repository.commitById(x)).map(_.some)
      , none.pure[ResultTIO])
  } yield commit

  def storeCommitToId(repository: Repository, id: CommitId, commit: Commit): ResultTIO[Unit] =
    toKeyStore(repository, Repository.commitById(id), commit)

  def fromList(entries: List[DictionaryId \/ FeatureStoreId]): ValidationNel[String, Commit] =
    entries match {
      case -\/(dict) :: \/-(featurestore) :: Nil =>
        Validation.success(Commit(dict, featurestore))
      case _ => Validation.failure(NonEmptyList("malformed commit metadata, not 2 lines long"))
    }

  def parseLine(i: Int, l: String): ValidationNel[String, DictionaryId \/ FeatureStoreId] =
    if (i == 1) {
      Identifier.parse(l).map(x => -\/(DictionaryId(x)))
        .cata(Validation.success, Validation.failure(NonEmptyList("malformed dictionary id")))
    } else if (i == 2) {
      OldIdentifier.parse(l).map(x => \/-(FeatureStoreId(x)))
        .cata(Validation.success, Validation.failure(NonEmptyList("malformed feature store id")))
    } else {
      Validation.failure(NonEmptyList(s"commit text storage parse error on line ${i}: $l"))
    }

  def toLine(id: DictionaryId \/ FeatureStoreId): String = id match {
    case -\/(l) => l.id.render
    case \/-(r) => r.id.render
  }

  def toList(commit: Commit): List[DictionaryId \/ FeatureStoreId] = commit match {
    case Commit(dict, feat) => List(-\/(dict), \/-(feat))
  }

  def listIds(repo: Repository): ResultTIO[List[CommitId]] = for {
    keys <- repo.store.list(Repository.commits).map(_.filterHidden)
    ids  <- keys.traverseU(key => ResultT.fromOption[IO, CommitId](CommitId.parse(key.name),
                                                      s"Can not parse Commit id '$key'"))
  } yield ids

  def latestId(repo: Repository): ResultTIO[Option[CommitId]] =
    listIds(repo).map(_.sorted.lastOption)

}
