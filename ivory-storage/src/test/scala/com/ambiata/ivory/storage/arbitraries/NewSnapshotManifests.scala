package com.ambiata.ivory.storage.arbitraries

import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.storage.metadata.NewSnapshotManifest
import com.ambiata.ivory.storage.gen.GenStorage._

import org.scalacheck.{Gen, Arbitrary}
import org.scalacheck.Arbitrary.arbitrary


case class NewSnapshotManifests(metas: List[NewSnapshotManifest])

object NewSnapshotManifests {
  implicit def NewSnapshotManifestsArbitary: Arbitrary[NewSnapshotManifests] =
    Arbitrary(for {
      ids <- arbitrary[SnapshotIds]
      metas <- Gen.oneOf(
          genSameDateNewManifests(ids)
        , genSameCommitIdNewManifests(ids)
        , genSameNewManifests(ids)
        )
      } yield NewSnapshotManifests(metas))
}
