package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.data.OldIdentifier
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.io._
import com.ambiata.mundane.control._

import org.specs2._
import com.nicta.scoobi.testing.TempFiles
import scalaz._, Scalaz._
import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.mundane.testing.ResultTIOMatcher._

class FeatureStoreTextStorageSpec extends Specification with ScalaCheck { def is = s2"""

  Parse a list of strings into a FeatureStore          $stringsFeatureStore
  Read a FeatureStore from a Repository                $readFeatureStore
  Write a FeatureStore to a Repository                 $writeFeatureStore
  Can list all FeatureStore Ids in a Repository        $listFeatureStorIds
  Can get latest FeatureStoreId from a Repository      $latestFeatureStoreIs
                                                       """
  import FeatureStoreTextStorage._

  def stringsFeatureStore = prop((fstore: FeatureStore) => {
    fromLines(toList(fstore.factsetIds).map(toLine)) must_== fstore.factsetIds.right
  })

  def readFeatureStore = prop((fstore: FeatureStore) => {
    val base = LocalLocation(TempFiles.createTempDir("FeatureStoreTextStorage.e2").getPath)
    val repo = LocalRepository(base.path)
    val expected = fstore.copy(factsets = fstore.factsets.map(_.map(fs => fs.copy(partitions = fs.partitions.sorted))))
    (writeFeatureStore(repo, fstore) must beOk) and (fromId(repo, fstore.id) must beOkLike(_ must_== expected))
  })

  def writeFeatureStore = prop((fstore: FeatureStore) =>  {
    val base = LocalLocation(TempFiles.createTempDir("FeatureStoreTextStorage.e3").getPath)
    val repo = LocalRepository(base.path)
    (toId(repo, fstore) must beOk) and
    (repo.toStore.utf8.read(Repository.featureStoreById(fstore.id)) must beOkLike(_ must_== delimitedString(fstore.factsetIds) + "\n"))
  })

  def listFeatureStorIds = prop((ids: SmallFeatureStoreIdList) => {
    val base = LocalLocation(TempFiles.createTempDir("FeatureStoreTextStorage.e4").getPath)
    val repo = LocalRepository(base.path)
    (writeFeatureStoreIds(repo, ids.ids) must beOk) and
    (Metadata.listFeatureStoreIds(repo) must beOkLike(_ must_== ids.ids))
  })

  def latestFeatureStoreIs = prop((ids: SmallFeatureStoreIdList) => {
    val base = LocalLocation(TempFiles.createTempDir("FeatureStoreTextStorage.e5").getPath)
    val repo = LocalRepository(base.path)
    (writeFeatureStoreIds(repo, ids.ids) must beOk) and
    (Metadata.latestFeatureStoreId(repo) must beOkLike(_ must_== ids.ids.sortBy(_.id).lastOption))
  })

  def writeFeatureStoreIds(repo: Repository, ids: List[FeatureStoreId]): ResultTIO[Unit] =
    ids.traverse(id => writeFile(repo, Repository.featureStores </> FilePath(id.render), List(""))).void

  /* Write out the feature store and factsets within it */
  def writeFeatureStore(repo: Repository, fstore: FeatureStore): ResultTIO[Unit] = for {
    _ <- writeFile(repo, Repository.featureStoreById(fstore.id), fstore.factsetIds.map(_.value.render))
    _ <- fstore.factsets.map(_.value).traverseU(factset => factset.partitions.partitions.traverseU(partition =>
           writeFile(repo, Repository.factset(factset.id) </> partition.path </> FilePath("data"), List(""))
         )).map(_.flatten)
  } yield ()

  def writeFile(repo: Repository, file: FilePath, lines: List[String]): ResultTIO[Unit] =
    repo.toStore.linesUtf8.write(file, lines)
}
