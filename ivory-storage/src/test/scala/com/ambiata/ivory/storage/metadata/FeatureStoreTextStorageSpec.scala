package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.manifest._
import com.ambiata.notion.core._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.Arbitraries._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.notion.core._

import org.specs2._
import scalaz._, Scalaz._
import org.scalacheck._, Arbitrary._

class FeatureStoreTextStorageSpec extends Specification with ScalaCheck { def is = s2"""

  Parse a list of strings into a FeatureStore          $stringsFeatureStore
  Read a FeatureStore from a Repository                $readFeatureStore
  Write a FeatureStore to a Repository                 $writeFeatureStore
  Can list all FeatureStore Ids in a Repository        $listFeatureStoreIds
  Can get latest FeatureStoreId from a Repository      $latestFeatureStoreIds
                                                       """
  import FeatureStoreTextStorage._

  def stringsFeatureStore = prop { fstore: FeatureStore =>
    fromLines(toList(fstore.factsetIds).map(toLine)) ==== fstore.factsetIds.right
  }

  def readFeatureStore = prop((tmp: LocalTemporary, fstore: FeatureStore) => for {
    d <- tmp.directory
    r = LocalRepository.create(d)
    _ <- writeFeatureStore(r, fstore)
    z <- fromId(r, fstore.id)
  } yield z ==== fstore.copy(factsets = fstore.factsets.map(_.map(fs => fs.copy(partitions = fs.partitions.sorted)))))

  def writeFeatureStore = prop((tmp: LocalTemporary, fstore: FeatureStore) => for {
    d <- tmp.directory
    r = LocalRepository.create(d)
    _ <- toId(r, fstore)
    z <- r.store.utf8.read(Repository.featureStoreById(fstore.id))
  } yield z ==== delimitedString(fstore.factsetIds))

  def listFeatureStoreIds = prop((tmp: LocalTemporary, ids: FeatureStoreIds) => for {
    d <- tmp.directory
    r = LocalRepository.create(d)
    _ <- writeFeatureStoreIds(r, ids.ids)
    z <- Metadata.listFeatureStoreIds(r)
  } yield z.sorted ==== ids.ids.sorted)

  def latestFeatureStoreIds = prop((tmp: LocalTemporary, ids: FeatureStoreIds) => for {
    d <- tmp.directory
    r = LocalRepository.create(d)
    _ <- writeFeatureStoreIds(r, ids.ids)
    z <- Metadata.latestFeatureStoreId(r)
  } yield z ==== ids.ids.sortBy(_.id).lastOption)

  def writeFeatureStoreIds(repo: Repository, ids: List[FeatureStoreId]): RIO[Unit] =
    ids.traverse(id => writeLines(repo, Repository.featureStores / id.asKeyName, List(""))).void

  /* Write out the feature store and factsets within it */
  def writeFeatureStore(repo: Repository, fstore: FeatureStore): RIO[Unit] = for {
    _ <- writeLines(repo, Repository.featureStoreById(fstore.id), fstore.factsetIds.map(_.value.render))
    _ <- fstore.factsets.map(_.value).traverseU(factset => factset.partitions.traverseU(partition =>
           writeLines(repo, Repository.factset(factset.id) / partition.value.key / "data", List(""))) >>
             FactsetManifest.io(repo, factset.id).write(FactsetManifest.create(factset.id, FactsetFormat.V2, factset.partitions)))
  } yield ()

  def writeLines(repository: Repository, key: Key, lines: List[String]): RIO[Unit] =
    repository.store.linesUtf8.write(key, lines)

}
