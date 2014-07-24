package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.io._

import org.specs2._
import com.nicta.scoobi.testing.TempFiles
import scalaz._, Scalaz._
import org.scalacheck._, Arbitrary._, Arbitraries._
import com.ambiata.mundane.testing.ResultTIOMatcher._

class FeatureStoreTextStorageSpec extends Specification with ScalaCheck { def is = s2"""

  Parse a list of strings into a FeatureStore          $e1
  Read a FeatureStore from a Repository                $e2
  Write a FeatureStore to a Repository                 $e3
                                                       """
  import FeatureStoreTextStorage._

  def e1 = prop((fstore: FeatureStore) => {
    fromLines(toList(fstore).map(toLine)) must_== fstore.right
  })

  def e2 = prop((fstore: FeatureStore) => {
    val base = LocalLocation(TempFiles.createTempDir("FeatureStoreTextStorage.e2").getPath)
    val repo = LocalRepository(base.path)
    (repo.toStore.utf8.write(Repository.storeByName("00000"), delimitedString(fstore)) must beOk) and
    (fromName(repo, "00000") must beOkLike(_ must_== fstore))
  })

  def e3 = prop((fstore: FeatureStore) =>  {
    val base = LocalLocation(TempFiles.createTempDir("FeatureStoreTextStorage.e3").getPath)
    val repo = LocalRepository(base.path)
    (toName(repo, "00000", fstore) must beOk) and
    (repo.toStore.utf8.read(Repository.storeByName("00000")) must beOkLike(_ must_== delimitedString(fstore) + "\n"))
  })
}
