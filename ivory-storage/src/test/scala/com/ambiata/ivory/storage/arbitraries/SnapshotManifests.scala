package com.ambiata.ivory.storage.arbitraries

import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.storage.gen.GenStorage._
import com.ambiata.ivory.storage.manifest.SnapshotManifest

import org.scalacheck.{Gen, Arbitrary}
import org.scalacheck.Arbitrary.arbitrary


case class SnapshotManifests(metas: List[SnapshotManifest])

object SnapshotManifests {
  implicit def SnapshotManifestsArbitary: Arbitrary[SnapshotManifests] =
    Arbitrary(for {
      ids <- arbitrary[SnapshotIds]
      metas <- Gen.oneOf(
          genSameDateNewManifests(ids)
        , genSameCommitIdNewManifests(ids)
        , genSameNewManifests(ids)
        )
      } yield SnapshotManifests(metas))
}
