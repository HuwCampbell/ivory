package com.ambiata.ivory.storage

import com.ambiata.ivory.core._
import com.ambiata.ivory.data.Identifier

import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.data.Arbitraries._

import plan._

object Arbitraries {

  implicit def FactsetDatasetArbitrary: Arbitrary[FactsetDataset] = Arbitrary(for {
    partitions <- arbitrary[SingleFactsetPartitions]
    // TODO change to just arbitrary[Factset] when factset is no longer part of partition
    factset    <- partitions.partitions.headOption.map(p => Gen.const(p.factset)).getOrElse(arbitrary[Factset])
  } yield FactsetDataset(factset, partitions.partitions))

  implicit def SnapshotDatasetArbitrary: Arbitrary[SnapshotDataset] =
    Arbitrary(arbitrary[Identifier].map(SnapshotDataset.apply))

  implicit def DatasetArbitrary: Arbitrary[Dataset] =
    Arbitrary(Gen.oneOf(arbitrary[FactsetDataset], arbitrary[SnapshotDataset]))
}
