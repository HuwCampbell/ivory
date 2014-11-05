package com.ambiata.ivory.storage.arbitraries

import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.storage.legacy.SnapshotMeta
import com.ambiata.ivory.storage.gen.GenStorage._

import org.scalacheck.{Gen, Arbitrary}
import org.scalacheck.Arbitrary.arbitrary


case class SnapshotMetas(metas: List[SnapshotMeta])

object SnapshotMetas {
  implicit def SnapshotMetasArbitrary: Arbitrary[SnapshotMetas] =
    Arbitrary(for {
      ids <- arbitrary[SnapshotIds]
      // Restricted to some simple cases...
      metas <- Gen.oneOf(
          genSnapshotMetas(ids)
        , genSameDateSnapshotMetas(ids)
        , genSameStoreSnapshotMetas(ids)
        , genSameSnapshotMetas(ids)
        )
    } yield SnapshotMetas(metas))
}
