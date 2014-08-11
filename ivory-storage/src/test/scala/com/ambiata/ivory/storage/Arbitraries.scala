package com.ambiata.ivory.storage

import com.ambiata.ivory.data.Identifier
import com.ambiata.ivory.storage.fact._

import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.data.Arbitraries._

import plan._

object Arbitraries {

  implicit def FactsetDatasetArbitrary: Arbitrary[FactsetDataset] =
    Arbitrary(arbitrary[SingleFactsetPartitions].map(p => FactsetDataset(p.factset, p.partitions)))

  implicit def SnapshotDatasetArbitrary: Arbitrary[SnapshotDataset] =
    Arbitrary(arbitrary[Identifier].map(SnapshotDataset.apply))

  implicit def DatasetArbitrary: Arbitrary[Dataset] =
    Arbitrary(Gen.oneOf(arbitrary[FactsetDataset], arbitrary[SnapshotDataset]))

  implicit def FactsetVersionArbitrary: Arbitrary[FactsetVersion] =
    Arbitrary(Gen.oneOf(FactsetVersionOne, FactsetVersionTwo))
}
