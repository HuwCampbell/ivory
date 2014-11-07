package com.ambiata.ivory.storage.gen

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy.SnapshotMeta
import com.ambiata.ivory.storage.metadata.{SnapshotManifest, NewSnapshotManifest}
import org.scalacheck.{Gen, Arbitrary}
import org.scalacheck.Arbitrary._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import scalaz._, Scalaz._, scalacheck.ScalaCheckBinding._

object GenStorage {
  def genSnapshotMetas(ids: SnapshotIds): Gen[List[SnapshotMeta]] =
    ids.ids.traverseU((id: SnapshotId) =>
      for {
        date <- arbitrary[Date]
        storeId <- arbitrary[FeatureStoreId]
        // commitIds should be increasing with dates, but since we are testing ordering
        // with these and the commitId is not used, this shouldnt affect the results.
        commitId <- arbitrary[Option[CommitId]]
      } yield SnapshotMeta(id, date, storeId, commitId))

  def genSameDateSnapshotMetas(ids: SnapshotIds): Gen[List[SnapshotMeta]] = for {
    date <- arbitrary[Date]
    commitId <- arbitrary[Option[CommitId]]
    metas <- ids.ids.traverseU((id: SnapshotId) => arbitrary[FeatureStoreId].map(SnapshotMeta(id, date, _, commitId)))
  } yield metas

  def genSameStoreSnapshotMetas(ids: SnapshotIds): Gen[List[SnapshotMeta]] = for {
    storeId <- arbitrary[FeatureStoreId]
    commitId <- arbitrary[Option[CommitId]]
    metas <- ids.ids.traverseU((id: SnapshotId) => arbitrary[Date].map(SnapshotMeta(id, _, storeId, commitId)))
  } yield metas

  def genSameSnapshotMetas(ids: SnapshotIds): Gen[List[SnapshotMeta]] = for {
    date <- arbitrary[Date]
    store <- arbitrary[FeatureStoreId]
    commitId <- arbitrary[Option[CommitId]]
  } yield ids.ids.map(SnapshotMeta(_, date, store, commitId))

  def genSameDateNewManifests(ids: SnapshotIds): Gen[List[NewSnapshotManifest]] = for {
    date <- arbitrary[Date]
    metas <- ids.ids.traverseU(
      (id: SnapshotId) =>
        arbitrary[CommitId].map(NewSnapshotManifest.newSnapshotMeta(id, date, _)))
  } yield metas

  def genSameCommitIdNewManifests(ids: SnapshotIds): Gen[List[NewSnapshotManifest]] = for {
    commitId <- arbitrary[CommitId]
    metas <- ids.ids.traverseU(
      (id: SnapshotId) =>
        arbitrary[Date].map(NewSnapshotManifest.newSnapshotMeta(id, _, commitId)))
  } yield metas

  def genSameNewManifests(ids: SnapshotIds): Gen[List[NewSnapshotManifest]] = for {
    date <- arbitrary[Date]
    commitId <- arbitrary[CommitId]
  } yield ids.ids.map(NewSnapshotManifest.newSnapshotMeta(_, date, commitId))
}
