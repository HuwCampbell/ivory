package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.Arbitraries._
import com.ambiata.mundane.control._

import org.specs2._
import scalaz._, Scalaz._
import org.scalacheck._, Arbitrary._
import com.ambiata.mundane.testing.RIOMatcher._

class CommitStorageSpec extends Specification with ScalaCheck { def is = s2"""

  Parse a list of strings into a Commit                $stringsCommit
  Read a Commit from a Repository                      $readCommit
  Write a Commit to a Repository                       $writeCommit
  Can list all Commit Ids in a Repository              $listCommitIds
  Can get latest CommitId from a Repository            $latestCommitId
                                                       """
  import CommitStorage._

  def stringsCommit = prop { commit: CommitMetadata =>
    fromLines.run(toLines(commit)).toEither must beRight(commit)
  }

  def readCommit = prop((local: LocalTemporary, commit: CommitMetadata, commitId: CommitId) => for {
    d <- local.directory
    r = LocalRepository.create(d)
    _ <- storeCommitToId(r, commitId, commit)
    z <- fromId(r, commitId)
  } yield z ==== commit.some)

  def writeCommit = prop((local: LocalTemporary, commit: CommitMetadata, commitId: CommitId) => for {
    d <- local.directory
    r = LocalRepository.create(d)
    _ <- storeCommitToId(r, commitId, commit)
    z <- r.store.linesUtf8.read(Repository.commitById(commitId))
  } yield z ==== toLines(commit))

  def listCommitIds = prop((local: LocalTemporary, ids: CommitIds) => for {
    d <- local.directory
    r = LocalRepository.create(d)
    _ <- writeCommitIds(r, ids.ids)
    z <- Metadata.listCommitIds(r)
    l = z.length
  } yield z.toSet -> l ==== ids.ids.toSet -> ids.ids.length)

  def latestCommitId = prop((local: LocalTemporary, ids: CommitIds) => for {
    d <- local.directory
    r = LocalRepository.create(d)
    _ <- writeCommitIds(r, ids.ids)
    z <- Metadata.latestCommitId(r)
  } yield z ==== ids.ids.sortBy(_.id).lastOption)

  def writeCommitIds(repo: Repository, ids: List[CommitId]): RIO[Unit] =
    ids.traverse(id => repo.store.linesUtf8.write(Repository.commits / id.asKeyName, List(""))).void
}
