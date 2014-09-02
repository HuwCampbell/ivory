package com.ambiata.ivory.storage.metadata

import scalaz._, Scalaz._, effect._

import com.ambiata.mundane.control._

import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.ivory.data._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._

// 2 lines.
// The first line is dictionaryId: DictionaryId
// The second line is featureStoreId: FeatureStoreId
object CommitTextStorage extends TextStorage[DictionaryId \/ FeatureStoreId, Commit] {

  val name = "commit"

  def increment(repo: Repository, c: Commit): ResultTIO[CommitId] = for {
    latest      <- latestId(repo)
    nextId      <- ResultT.fromOption[IO, CommitId](latest.map(_.next).getOrElse(Some(CommitId.initial)), "Ran out of Commit ids!")
    _           <- storeCommitToId(repo, nextId, c)
  } yield nextId

  def fromId(repo: Repository, id: CommitId): ResultTIO[Option[Commit]] = for {
    commitId <- listIds(repo).map(_.find(_ === id))
    commit <- commitId.cata(x =>
        fromStore(repo.toReference(Repository.commitById(x))).map(_.some)
      , none.pure[ResultTIO])
  } yield commit

  def storeCommitToId(repository: Repository, id: CommitId, commit: Commit): ResultTIO[Unit] =
    storeCommitToReference(repository.toReference(Repository.commitById(id)), commit)

  def storeCommitToReference(ref: ReferenceIO, commit: Commit): ResultTIO[Unit] =
    toStore(ref, commit)

  def fromList(entries: List[DictionaryId \/ FeatureStoreId]): ValidationNel[String, Commit] =
    entries match {
      case -\/(dict) :: \/-(featurestore) :: Nil =>
        Validation.success(Commit(dict, featurestore.some))
      case -\/(dict) :: Nil =>
        Validation.success(Commit(dict, none))
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

  def toLine(l: DictionaryId \/ FeatureStoreId): String = l match {
    case -\/(l) => l.id.render
    case \/-(r) => r.id.render
  }

  def toList(t: Commit): List[DictionaryId \/ FeatureStoreId] = t match {
      case Commit(dict, None) => List(-\/(dict))
      case Commit(dict, Some(feat)) => List(-\/(dict), \/-(feat))
    }

  def listIds(repo: Repository): ResultTIO[List[CommitId]] = for {
    paths <- repo.toStore.list(Repository.commits).map(_.filterHidden)
    ids   <- paths.traverseU(p =>
               ResultT.fromOption[IO, CommitId](CommitId.parse(p.basename.path),
                                                      s"Can not parse Commit id '${p}'"))
  } yield ids

  def latestId(repo: Repository): ResultTIO[Option[CommitId]] =
    listIds(repo).map(_.sorted.lastOption)

}
