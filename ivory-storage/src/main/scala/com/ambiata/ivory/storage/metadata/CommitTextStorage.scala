package com.ambiata.ivory.storage.metadata

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
    commit <- commitId.cata(x =>
        fromFileStoreIO(repository.commitById(x)).map(_.some)
      , none.pure[ResultTIO])
  } yield commit

  def storeCommitToId(repository: Repository, id: CommitId, commit: Commit): ResultTIO[Unit] =
    storeCommitToReference(repository.commitById(id), commit)

  def storeCommitToReference(ref: ReferenceIO, commit: Commit): ResultTIO[Unit] =
    toFileStoreIO(ref, commit)

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

  def toLine(l: DictionaryId \/ FeatureStoreId): String = l match {
    case -\/(l) => l.id.render
    case \/-(r) => r.id.render
  }

  def toList(t: Commit): List[DictionaryId \/ FeatureStoreId] = t match {
      case Commit(dict, feat) => List(-\/(dict), \/-(feat))
    }

  def listIds(repo: Repository): ResultTIO[List[CommitId]] = for {
    paths <- ReferenceStore.list(repo.commits).map(_.filterHidden)
    ids   <- paths.traverseU(p =>
               ResultT.fromOption[IO, CommitId](CommitId.parse(p.basename.name),
                                                      s"Can not parse Commit id '${p}'"))
  } yield ids

  def latestId(repo: Repository): ResultTIO[Option[CommitId]] =
    listIds(repo).map(_.sorted.lastOption)

}
