package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.ScalaCheckManagedProperties
import com.ambiata.mundane.io.TemporaryDirPath
import com.ambiata.notion.core._
import com.ambiata.mundane.control._

import org.specs2._
import scalaz._, Scalaz._
import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.mundane.testing.ResultTIOMatcher._

class FeatureStoreTextStorageSpec extends Specification with ScalaCheck with ScalaCheckManagedProperties { def is = s2"""

  Parse a list of strings into a FeatureStore          $stringsFeatureStore
  Read a FeatureStore from a Repository                $readFeatureStore
  Write a FeatureStore to a Repository                 $writeFeatureStore
  Can list all FeatureStore Ids in a Repository        $listFeatureStorIds
  Can get latest FeatureStoreId from a Repository      $latestFeatureStoreIs
                                                       """
  import FeatureStoreTextStorage._

  def stringsFeatureStore = prop { fstore: FeatureStore =>
    fromLines(toList(fstore.factsetIds).map(toLine)) must_== fstore.factsetIds.right
  }

  def readFeatureStore = managed { temp: TemporaryDirPath => fstore: FeatureStore =>
    val expected = fstore.copy(factsets = fstore.factsets.map(_.map(fs => fs.copy(partitions = fs.partitions.sorted))))
    val repo = LocalRepository.create(temp.dir)

    writeFeatureStore(repo, fstore) >>
    fromId(repo, fstore.id) must beOkValue(expected)
  }

  def writeFeatureStore = managed { temp: TemporaryDirPath => fstore: FeatureStore =>
    val repo = LocalRepository.create(temp.dir)
    toId(repo, fstore) >>
      repo.store.utf8.read(Repository.featureStoreById(fstore.id)) must
      beOkLike(_ must_== delimitedString(fstore.factsetIds))
  }

  def listFeatureStorIds = managed { temp: TemporaryDirPath => ids: FeatureStoreIds =>
    val repo = LocalRepository.create(temp.dir)
    writeFeatureStoreIds(repo, ids.ids) >>
    Metadata.listFeatureStoreIds(repo).map(_.toSet) must beOkValue(ids.ids.toSet)
  }

  def latestFeatureStoreIs = managed { temp: TemporaryDirPath => ids: FeatureStoreIds =>
    val repo = LocalRepository.create(temp.dir)
    writeFeatureStoreIds(repo, ids.ids) >>
    Metadata.latestFeatureStoreId(repo) must beOkValue(ids.ids.sortBy(_.id).lastOption)
  }

  def writeFeatureStoreIds(repo: Repository, ids: List[FeatureStoreId]): RIO[Unit] =
    ids.traverse(id => writeLines(repo, Repository.featureStores / id.asKeyName, List(""))).void

  /* Write out the feature store and factsets within it */
  def writeFeatureStore(repo: Repository, fstore: FeatureStore): RIO[Unit] = for {
    _ <- writeLines(repo, Repository.featureStoreById(fstore.id), fstore.factsetIds.map(_.value.render))
    _ <- fstore.factsets.map(_.value).traverseU(factset => factset.partitions.traverseU(partition =>
           writeLines(repo, Repository.factset(factset.id) / partition.key / "data", List(""))
         )).map(_.flatten)
  } yield ()

  def writeLines(repository: Repository, key: Key, lines: List[String]): RIO[Unit] =
    repository.store.linesUtf8.write(key, lines)
}
