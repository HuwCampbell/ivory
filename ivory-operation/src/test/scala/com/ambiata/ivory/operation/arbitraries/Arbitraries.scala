package com.ambiata.ivory.operation.arbitraries

import com.ambiata.ivory.operation.extraction.snapshot.SnapshotStats
import org.scalacheck._

trait Arbitraries {

  implicit def SnapshotStatsArbitrary: Arbitrary[SnapshotStats] =
    Arbitrary(GenSnapshotStats.snapshotStats)
}

object Arbitraries extends Arbitraries
