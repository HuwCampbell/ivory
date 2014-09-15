package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.mundane.io._
import com.ambiata.mundane.control._

import org.specs2._
import scalaz._, Scalaz._
import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.mundane.testing.ResultTIOMatcher._

class CommitTextStorageSpec extends Specification with ScalaCheck { def is = s2"""

  Parse a list of strings into a Commit                $stringsCommit
  Read a Commit from a Repository                      $readCommit
  Write a Commit to a Repository                       $writeCommit
  Can list all Commit Ids in a Repository              $listCommitIds
  Can get latest CommitId from a Repository            $latestCommitId
                                                       """
  import CommitTextStorage._

  def stringsCommit = prop { commit: Commit =>
    fromLines(toList(commit).map(toLine)) must_== commit.right
  }

  def readCommit = prop { (commit: Commit, commitId: CommitId) =>
    Temporary.using { dir =>
      val base = LocalLocation(dir.path)
      val repo = LocalRepository(base.path)

      storeCommitToId(repo, commitId, commit) >>
      fromId(repo, commitId)
    } must beOkValue(Some(commit))
  }

  def writeCommit = prop { (commit: Commit, commitId: CommitId) =>
    Temporary.using { dir =>
      val base = LocalLocation(dir.path)
      val repo = LocalRepository(base.path)
      storeCommitToId(repo, commitId, commit) >>
      repo.toStore.utf8.read(Repository.commitById(commitId))
    } must beOkLike(_ must_== delimitedString(commit))
  }

  def listCommitIds = prop { ids: SmallCommitIdList =>
    Temporary.using { dir =>
      val base = LocalLocation(dir.path)
      val repo = LocalRepository(base.path)
      writeCommitIds(repo, ids.ids) >>
      Metadata.listCommitIds(repo)
    } must beOkValue(ids.ids)
  }

  def latestCommitId = prop { ids: SmallCommitIdList =>
    Temporary.using { dir =>
      val base = LocalLocation(dir.path)
      val repo = LocalRepository(base.path)
      writeCommitIds(repo, ids.ids) >>
      Metadata.latestCommitId(repo)
    } must beOkValue(ids.ids.sortBy(_.id).lastOption)
  }

  def writeCommitIds(repo: Repository, ids: List[CommitId]): ResultTIO[Unit] =
    ids.traverse(id => writeFile(repo, Repository.commits </> FilePath(id.render), List(""))).void

  def writeFile(repo: Repository, file: FilePath, lines: List[String]): ResultTIO[Unit] =
    repo.toStore.linesUtf8.write(file, lines)
}
