package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._

import org.scalacheck._


case class SnapshotIds(ids: List[SnapshotId])

object SnapshotIds {
  implicit def SnapshotIdsArbitrary: Arbitrary[SnapshotIds] =
    Arbitrary(GenIdentifier.snapshots.map(SnapshotIds.apply))
}
