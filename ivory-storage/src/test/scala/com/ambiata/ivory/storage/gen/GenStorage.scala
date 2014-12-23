package com.ambiata.ivory.storage.gen

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.manifest.SnapshotManifest
import org.scalacheck.Gen
import org.scalacheck.Arbitrary._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import scalaz._, Scalaz._, scalacheck.ScalaCheckBinding._

object GenStorage {

  def genSameDateNewManifests(ids: SnapshotIds): Gen[List[SnapshotManifest]] = for {
    date <- arbitrary[Date]
    metas <- ids.ids.traverseU(
      (id: SnapshotId) =>
        arbitrary[CommitId].map(SnapshotManifest.createLatest(_, id, date)))
  } yield metas

  def genSameCommitIdNewManifests(ids: SnapshotIds): Gen[List[SnapshotManifest]] = for {
    commitId <- arbitrary[CommitId]
    metas <- ids.ids.traverseU(
      (id: SnapshotId) =>
        arbitrary[Date].map(SnapshotManifest.createLatest(commitId, id, _)))
  } yield metas

  def genSameNewManifests(ids: SnapshotIds): Gen[List[SnapshotManifest]] = for {
    date <- arbitrary[Date]
    commitId <- arbitrary[CommitId]
  } yield ids.ids.map(SnapshotManifest.createLatest(commitId, _, date))
}
