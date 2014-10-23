package com.ambiata.ivory.storage

import com.ambiata.ivory.core._
import com.ambiata.ivory.data.Identifier
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.legacy._

import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.data.Arbitraries._
import com.ambiata.notion.core._
import com.ambiata.notion.core.TemporaryType._
import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._

import plan._

object Arbitraries {

  implicit def FactsetDatasetArbitrary: Arbitrary[FactsetDataset] =
    Arbitrary(arbitrary[Factset].map(p => FactsetDataset(p.id, p.partitions.partitions)))

  implicit def SnapshotDatasetArbitrary: Arbitrary[SnapshotDataset] =
    Arbitrary(arbitrary[Identifier].map(SnapshotDataset.apply))

  implicit def DatasetArbitrary: Arbitrary[Dataset] =
    Arbitrary(Gen.oneOf(arbitrary[FactsetDataset], arbitrary[SnapshotDataset]))

  implicit def FactsetVersionArbitrary: Arbitrary[FactsetVersion] =
    Arbitrary(Gen.oneOf(FactsetVersionOne, FactsetVersionTwo))

  implicit def StoreTypeArbitrary: Arbitrary[TemporaryType] =
    Arbitrary(Gen.oneOf(Posix, S3, Hdfs))

  implicit def NewSnapshotManifestArbitrary: Arbitrary[NewSnapshotManifest] =
    Arbitrary(for {
      id <- arbitrary[SnapshotId]
      date <- arbitrary[Date]
      commitId <- arbitrary[CommitId]
    } yield NewSnapshotManifest.newSnapshotMeta(id, date, commitId))

  case class SnapshotMetaList(metas: List[SnapshotMeta])

  implicit def SnapshotMetaListArbitrary: Arbitrary[SnapshotMetaList] = Arbitrary(
    for {
      ids <- arbitrary[SmallSnapshotIdList]
      // Restricted to some simple cases...
      metas <- Gen.oneOf(
          genSnapshotMetas(ids)
        , genSameDateSnapshotMetas(ids)
        , genSameStoreSnapshotMetas(ids)
        , genSameSnapshotMetas(ids))
    } yield SnapshotMetaList(metas))

  def genSnapshotMetas(ids: SmallSnapshotIdList): Gen[List[SnapshotMeta]] =
    ids.ids.traverseU((id: SnapshotId) =>
      for {
        date <- arbitrary[Date]
        storeId <- arbitrary[FeatureStoreId]
        // commitIds should be increasing with dates, but since we are testing ordering
        // with these and the commitId is not used, this shouldnt affect the results.
        commitId <- arbitrary[Option[CommitId]]
      } yield SnapshotMeta(id, date, storeId, commitId))

  def genSameDateSnapshotMetas(ids: SmallSnapshotIdList): Gen[List[SnapshotMeta]] = for {
    date <- arbitrary[Date]
    commitId <- arbitrary[Option[CommitId]]
    metas <- ids.ids.traverseU((id: SnapshotId) => arbitrary[FeatureStoreId].map(SnapshotMeta(id, date, _, commitId)))
  } yield metas

  def genSameStoreSnapshotMetas(ids: SmallSnapshotIdList): Gen[List[SnapshotMeta]] = for {
    storeId <- arbitrary[FeatureStoreId]
    commitId <- arbitrary[Option[CommitId]]
    metas <- ids.ids.traverseU((id: SnapshotId) => arbitrary[Date].map(SnapshotMeta(id, _, storeId, commitId)))
  } yield metas

  def genSameSnapshotMetas(ids: SmallSnapshotIdList): Gen[List[SnapshotMeta]] = for {
    date <- arbitrary[Date]
    store <- arbitrary[FeatureStoreId]
    commitId <- arbitrary[Option[CommitId]]
  } yield ids.ids.map(SnapshotMeta(_, date, store, commitId))

  implicit def SnapshotMetaArbitrary: Arbitrary[SnapshotMeta] =
    Arbitrary(for {
      id <- arbitrary[SnapshotId]
      date <- arbitrary[Date]
      featureStoreId <- arbitrary[FeatureStoreId]
      commitId <- arbitrary[Option[CommitId]]
    } yield SnapshotMeta(id, date, featureStoreId, commitId))

  implicit def SnapshotManifestArbitrary: Arbitrary[SnapshotManifest] =
    Arbitrary(Gen.oneOf(
      arbitrary[SnapshotMeta].map(SnapshotManifest.snapshotManifestLegacy),
      arbitrary[NewSnapshotManifest].map(SnapshotManifest.snapshotManifestNew)))
}
