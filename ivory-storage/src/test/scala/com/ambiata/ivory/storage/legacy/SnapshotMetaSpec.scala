package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.Arbitraries._

import org.specs2._
import org.scalacheck._, Arbitrary._
import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.core.arbitraries._
import ArbitraryMetadata._
import ArbitraryValues._
import com.ambiata.ivory.scoobi.TestConfigurations._
import com.ambiata.mundane.testing.ResultTIOMatcher._

import org.specs2.matcher._

object SnapshotMetaSpec extends Specification with ScalaCheck with ThrownExpectations { def is = s2"""

 SnapshotMeta.fromIndentifier will not fail on missing .snapmeta $missing

 SnapshotMeta objects are sorted based on:
   snapshotId, date and storeId $sorting

"""

  def missing = prop ((snapshotId: SnapshotId) =>
    RepositoryBuilder.using { repository => SnapshotMeta.fromIdentifier(repository, snapshotId)} must beOkValue(None)
  ).set(minTestsOk = 1)

  def sorting = prop { snaps: List[SnapshotMeta] =>
    snaps.sorted must_== snaps.sortBy(sm => (sm.snapshotId, sm.date, sm.featureStoreId))
  }
}
