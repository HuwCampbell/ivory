package com.ambiata.ivory.storage.arbitraries

import com.ambiata.ivory.storage.metadata.SnapshotManifest

import org.scalacheck.{Gen, Arbitrary}
import org.scalacheck.Arbitrary.arbitrary


case class SnapshotManifests(metas: List[SnapshotManifest])

object SnapshotManifests {
  implicit def SnapshotManifestsArbitrary: Arbitrary[SnapshotManifests] =
    Arbitrary(Gen.oneOf(
      arbitrary[SnapshotMetas].map(
        (snaps: SnapshotMetas) =>
          SnapshotManifests(snaps.metas.map(SnapshotManifest.snapshotManifestLegacy))),
      arbitrary[NewSnapshotManifests].map(
        (snaps: NewSnapshotManifests) =>
          SnapshotManifests(snaps.metas.map(SnapshotManifest.snapshotManifestNew)))))

}
