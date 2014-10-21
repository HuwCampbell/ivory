package com.ambiata.ivory.storage

import com.ambiata.ivory.core._
import com.ambiata.ivory.data.Identifier
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.data.Arbitraries._
import com.ambiata.notion.core._
import com.ambiata.notion.core.TemporaryType._
import org.scalacheck._, Arbitrary._

import plan._

object Arbitraries {

  implicit def FactsetDatasetArbitrary: Arbitrary[FactsetDataset] =
    Arbitrary(arbitrary[Factset].map(p => FactsetDataset(p.id, p.partitions.partitions)))

  implicit def SnapshotDatasetArbitrary: Arbitrary[SnapshotDataset] =
    Arbitrary(arbitrary[Identifier].map(SnapshotDataset.apply))

  implicit def DatasetArbitrary: Arbitrary[Dataset] =
    Arbitrary(Gen.oneOf(arbitrary[FactsetDataset], arbitrary[SnapshotDataset]))

  implicit def FactsetVersionArbitrary: Arbitrary[FactsetVersion] =
    Arbitrary(Gen.oneOf(FactsetVersionOne, FactsetVersionTwo))

  implicit def StoreTypeArbitrary: Arbitrary[TemporaryType] =
    Arbitrary(Gen.oneOf(Posix, S3, Hdfs))
}
