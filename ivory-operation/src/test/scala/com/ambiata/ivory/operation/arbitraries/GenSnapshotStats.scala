package com.ambiata.ivory.operation.arbitraries

import com.ambiata.ivory.core.FeatureId
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.gen.{GenIdentifier, GenDate}
import com.ambiata.ivory.operation.extraction.snapshot.SnapshotStats
import org.scalacheck._

object GenSnapshotStats {

  def snapshotStats: Gen[SnapshotStats] = for {
    iv <- GenIdentifier.version
    dt <- GenDate.dateTime
    c  <- Arbitrary.arbitrary[Map[FeatureId, Long]].map(_.mapValues(Math.abs))
  } yield SnapshotStats(iv, dt, c)
}
