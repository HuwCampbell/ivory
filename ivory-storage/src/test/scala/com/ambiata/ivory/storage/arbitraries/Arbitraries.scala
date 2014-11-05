package com.ambiata.ivory.storage.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.legacy.SnapshotMeta
import com.ambiata.ivory.storage.metadata.{SnapshotManifest, NewSnapshotManifest}
import com.ambiata.ivory.storage.fact.{FactsetVersionTwo, FactsetVersionOne, FactsetVersion}
import com.ambiata.notion.core.TemporaryType
import com.ambiata.notion.core.TemporaryType.{Hdfs, S3, Posix}
import org.scalacheck.{Gen, Arbitrary}
import org.scalacheck.Arbitrary._

trait Arbitraries {
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

object Arbitraries extends Arbitraries
