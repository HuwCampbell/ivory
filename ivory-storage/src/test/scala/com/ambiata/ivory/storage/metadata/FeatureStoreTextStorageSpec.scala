package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.data.OldIdentifier
import com.ambiata.ivory.data.IvoryDataLiterals._
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

  Parse a list of strings into a FeatureStore          $e1
  Read a FeatureStore from a Repository                $e2
  Write a FeatureStore to a Repository                 $e3
  Can list all FeatureStore Ids in a Repository        $e4
  Can get latest FeatureStoreId from a Repository      $e5
                                                       """
  import FeatureStoreTextStorage._

  val initalStore = FeatureStoreId(oi"00000")

  def e1 = prop((fstore: FeatureStore) => {
    fromLines(toList(fstore).map(toLine)) must_== fstore.right
  })

  def e2 = prop((fstore: FeatureStore) => {
    val base = LocalLocation(TempFiles.createTempDir("FeatureStoreTextStorage.e2").getPath)
    val repo = LocalRepository(base.path)
    (repo.toStore.utf8.write(Repository.storeById(initalStore), delimitedString(fstore)) must beOk) and
    (fromId(repo, initalStore) must beOkLike(_ must_== fstore))
  })

  def e3 = prop((fstore: FeatureStore) =>  {
    val base = LocalLocation(TempFiles.createTempDir("FeatureStoreTextStorage.e3").getPath)
    val repo = LocalRepository(base.path)
    (toId(repo, initalStore, fstore) must beOk) and
    (repo.toStore.utf8.read(Repository.storeById(initalStore)) must beOkLike(_ must_== delimitedString(fstore) + "\n"))
  })

  def e4 = prop((ids: SmallFeatureStoreIdList) => {
    val base = LocalLocation(TempFiles.createTempDir("FeatureStoreTextStorage.e4").getPath)
    val repo = LocalRepository(base.path)
    (writeFeatureStores(repo, ids.ids) must beOk) and
    (Metadata.listStoreIds(repo) must beOkLike(_ must_== ids.ids))
  })

  def e5 = prop((ids: SmallFeatureStoreIdList) => {
    val base = LocalLocation(TempFiles.createTempDir("FeatureStoreTextStorage.e5").getPath)
    val repo = LocalRepository(base.path)
    (writeFeatureStores(repo, ids.ids) must beOk) and
    (Metadata.latestStoreId(repo) must beOkLike(_ must_== ids.ids.sortBy(_.id).lastOption))
  })

  def writeFeatureStores(repo: Repository, ids: List[FeatureStoreId]): ResultTIO[Unit] =
    ids.traverse(id => repo.toStore.utf8.write(Repository.stores </> FilePath(id.render), "")).void
}
