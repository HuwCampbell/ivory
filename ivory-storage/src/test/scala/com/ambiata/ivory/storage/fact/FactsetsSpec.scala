package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.TestConfigurations._
import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.mundane.control._

import org.specs2._
import org.specs2.execute.{AsResult, Result}

import scalaz._, Scalaz._

class FactsetsSpec extends Specification with ScalaCheck { def is = s2"""

  Can get latest factset id                      $latest
  Can allocate a new factset id                  $allocate
  Can list all factsets                          $factsets
  Can read a single factset                      $factset
                                                 """

  def latest = prop { ids: FactsetIdList =>
    withRepository { repo =>
      (for {
        _ <- ids.ids.traverseU(id => allocatePath(repo, Repository.factset(id)))
        l <- Factsets.latestId(repo)
      } yield l) must beOkLike(_ must_== ids.ids.sorted.lastOption)
    }
  }

  def allocate = prop { factsetId: FactsetId =>
    withRepository { repo =>
      val nextId = factsetId.next.get

      val res = for {
        _ <- allocatePath(repo, Repository.factset(factsetId))
        n <- Factsets.allocateFactsetId(repo)
        e <- repo.store.existsPrefix(Repository.factset(nextId))
      } yield (e, n)

      res must beOkLike { case (e, n) => (e, n) must_== ((true, nextId)) }
    }
  }

  def factsets = prop { factsets: FactsetList =>
    withRepository { repo =>
      val expected = factsets.factsets.map(fs => fs.copy(partitions = fs.partitions.sorted)).sortBy(_.id)

      (factsets.factsets.traverseU(fs =>
        fs.partitions.partitions.traverseU(p => writeDataFile(repo, Repository.factset(fs.id) / p.key))
      ) must beOk) and
        (Factsets.factsets(repo) must beOkLike(_ must containTheSameElementsAs(expected)))
    }
  }

  def factset = prop { factset: Factset =>
    withRepository { repo =>
      val expected = factset.copy(partitions = factset.partitions.sorted)

      (factset.partitions.partitions.traverseU(p => writeDataFile(repo, Repository.factset(factset.id) / p.key)) must beOk) and
        (Factsets.factset(repo, factset.id) must beOkValue(expected))
    }
  }

  def withRepository[R : AsResult](f: Repository => R): Result =
    TemporaryDirPath.withDirPath { dir =>
      for {
        repository <- Repository.fromUri((dir </> "repo").path, IvoryConfiguration.fromScoobiConfiguration(scoobiConfiguration))
      } yield AsResult(f(repository))
    } must beOkLike(r => r.isSuccess aka r.message must beTrue)

  def allocatePath(repository: Repository, key: Key): ResultTIO[Unit] =
    writeEmptyFile(repository, key / ".allocated")

  def writeDataFile(repository: Repository, key: Key): ResultTIO[Unit] =
    writeEmptyFile(repository, key / "data")

  def writeEmptyFile(repository: Repository, key: Key): ResultTIO[Unit] =
    repository.store.utf8.write(key, "")
}
