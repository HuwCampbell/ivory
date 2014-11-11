package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.ambiata.mundane.control._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.notion.core._

import org.specs2._

import scalaz._, Scalaz._

class FactsetsSpec extends Specification with ScalaCheck { def is = s2"""

  Can get latest factset id                      $latest    ${tag("store")}
  Can allocate a new factset id                  $allocate  ${tag("store")}
  Can list all factsets                          $factsets  ${tag("store")}
  Can read a single factset                      $factset   ${tag("store")}
                                                 """

  def latest = prop { ids: FactsetIds =>
    RepositoryBuilder.using { repo =>
      for {
        _ <- ids.ids.traverseU(id => allocatePath(repo, Repository.factset(id)))
        l <- Factsets.latestId(repo)
      } yield l
    } must beOkLike(_ must_== ids.ids.sorted.lastOption)
  }.set(minTestsOk = 10)

  def allocate = prop { factsetId: FactsetId =>
    val nextId = factsetId.next.get
    RepositoryBuilder.using { repo =>

      for {
        _ <- allocatePath(repo, Repository.factset(factsetId))
        n <- Factsets.allocateFactsetId(repo)
        e <- repo.store.existsPrefix(Repository.factset(nextId))
      } yield (e, n)
    } must beOkLike { case (e, n) => (e, n) must_== ((true, nextId)) }
  }.set(minTestsOk = 10)

  def factsets = prop { factsets: Factsets =>
    val expected = factsets.factsets.map(fs => fs.copy(partitions = fs.partitions.sorted)).sortBy(_.id)
    RepositoryBuilder.using { repo =>

      factsets.factsets.traverseU(fs =>
        fs.partitions.traverseU(p => writeDataFile(repo, Repository.factset(fs.id) / p.key))
      ) >> Factsets.factsets(repo)
    } must beOkLike(_ must containTheSameElementsAs(expected))
  }.set(minTestsOk = 10)

  def factset = prop { factset: Factset =>
    val expected = factset.copy(partitions = factset.partitions.sorted)
    RepositoryBuilder.using { repo =>

      factset.partitions.traverseU(p => writeDataFile(repo, Repository.factset(factset.id) / p.key)) >>
        Factsets.factset(repo, factset.id)
    } must beOkValue(expected)
  }.set(minTestsOk = 10)

  def allocatePath(repository: Repository, key: Key): ResultTIO[Unit] =
    writeEmptyFile(repository, key / ".allocated")

  def writeDataFile(repository: Repository, key: Key): ResultTIO[Unit] =
    writeEmptyFile(repository, key / "data")

  def writeEmptyFile(repository: Repository, key: Key): ResultTIO[Unit] =
    repository.store.utf8.write(key, "")
}
