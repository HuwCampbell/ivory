package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.TestConfigurations._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.mundane.control._

import org.specs2._
import org.specs2.execute.{AsResult, Result}

import scalaz._, Scalaz._

object FactsetsSpec extends Specification with ScalaCheck { def is = s2"""

  Can get latest factset id                      $latest
  Can allocate a new factset id                  $allocate
  Can list all factsets                          $factsets
  Can read a single factset                      $factset
                                                 """

  def latest = prop { ids: FactsetIdList =>
    withRepository { repo =>
      (for {
        _ <- ids.ids.traverseU(id => allocatePath(repo.factset(id)))
        l <- Factsets.latestId(repo)
      } yield l) must beOkLike(_ must_== ids.ids.sorted.lastOption)
    }
  }

  def allocate = prop { factsetId: FactsetId => 
    withRepository { repo =>
      val expected = factsetId.next.map((true, _))

      val res = for {
        _ <- allocatePath(repo.factset(factsetId))
        n <- Factsets.allocateFactsetId(repo)
        e <- ReferenceStore.exists(repo.factset(factsetId.next.get))
      } yield (e, n)

      expected.map(e => res must beOkLike(_ must_== e)).getOrElse(res.run.unsafePerformIO.toOption must beNone)
    }
  }

  def factsets = prop { factsets: FactsetList => 
    withRepository { repo =>
      val expected = factsets.factsets.map(fs => fs.copy(partitions = fs.partitions.sorted)).sortBy(_.id)

      (factsets.factsets.traverseU(fs =>
        fs.partitions.partitions.traverseU(p => writeDataFile(repo.factset(fs.id) </> p.path))
      ) must beOk) and
        (Factsets.factsets(repo) must beOkLike(_ must containTheSameElementsAs(expected)))
    }
  }

  def factset = prop { factset: Factset =>
    withRepository { repo =>
      val expected = factset.copy(partitions = factset.partitions.sorted)

      (factset.partitions.partitions.traverseU(p => writeDataFile(repo.factset(factset.id) </> p.path)) must beOk) and
        (Factsets.factset(repo, factset.id) must beOkValue(expected))
    }
  }

  def withRepository[R : AsResult](f: Repository => R): Result =
    Temporary.using { dir =>
      for {
        repository <- Repository.fromUriResultTIO((dir </> "repo").path, RepositoryConfiguration(scoobiConfiguration))
      } yield AsResult(f(repository))
    } must beOkLike(r => r.isSuccess aka r.message must beTrue)

  def allocatePath(ref: ReferenceIO): ResultTIO[Unit] =
    writeEmptyFile(ref </> ".allocated")

  def writeDataFile(ref: ReferenceIO): ResultTIO[Unit] =
    writeEmptyFile(ref </> "data")

  def writeEmptyFile(ref: ReferenceIO): ResultTIO[Unit] =
    ReferenceStore.writeUtf8(ref, "")
}
