package com.ambiata.ivory.operation.extraction.snapshot

import com.ambiata.ivory.core._, arbitraries._, ArbitraryMetadata._, ArbitraryValues._, ArbitraryFeatures._
import org.scalacheck.Arbitrary
import org.specs2.{ScalaCheck, Specification}

class SnapshotPartitionSpec extends Specification with ScalaCheck { def is = s2"""

   No extra factsets are required when the window is the same                 $sameWindow
   Adding a window will read in extra factset data                            $addedWindow
   Extending the window outside snapshot will read in extra factset data      $extendedWindow
   Extending the window within snapshot will not load any data                $extendedWindowWithinSnapshot
 """

  import SnapshotPartition._

  implicit def SnapshotWindowArb: Arbitrary[SnapshotWindow] = Arbitrary(for {
    f <- Arbitrary.arbitrary[FeatureId]
    d <- Arbitrary.arbitrary[Option[Date]]
  } yield SnapshotWindow(f, d))

  implicit def SnapshotWindowsArb: Arbitrary[SnapshotWindows] =
    Arbitrary(Arbitrary.arbitrary[List[SnapshotWindow]].map(SnapshotWindows.apply))

  def sameWindow = prop((fsid: FeatureStoreId, factsets: List[Factset], w: SnapshotWindow, d: UniqueDates) => {
    incWindow(fsid, factsets, w, d.later, Some(d.now), Some(d.now))._2.flatMap(_.store.factsetIds) ==== Nil
  })

  def addedWindow = prop((fsid: FeatureStoreId, factsets: List[Factset], w: SnapshotWindow, d: UniqueDates) => {
    val (fs, partitions) = incWindow(fsid, factsets, w, d.later, Some(d.earlier), None)
    partitions.flatMap(_.store.factsetIds).toSet ==== fs.factsetIds.toSet
  })

  def extendedWindow = prop((fsid: FeatureStoreId, factsets: List[Factset], w: SnapshotWindow, d: UniqueDates) => {
    val (fs, partitions) = incWindow(fsid, factsets, w, d.later, Some(d.earlier), Some(d.now))
    partitions.flatMap(_.store.factsetIds).toSet ==== fs.factsetIds.toSet
  })

  def extendedWindowWithinSnapshot = prop((fsid: FeatureStoreId, factsets: List[Factset], w: SnapshotWindow, d: UniqueDates) => {
    incWindow(fsid, factsets, w, d.earlier, Some(d.now), Some(d.later))._2.flatMap(_.store.factsetIds) ==== Nil
  })

  def incWindow(fsid: FeatureStoreId, factsets: List[Factset], w: SnapshotWindow, d: Date, sd1: Option[Date],
                sd2: Option[Date]): (FeatureStore, List[SnapshotPartition]) = {
    val fs = FeatureStore.fromList(fsid, factsets.map(withNamespace(w.featureId.namespace))).get
    (fs, partitionIncrementalWindowing(fs, d,
      SnapshotWindows(List(w.copy(startDate = sd1))), SnapshotWindows(List(w.copy(startDate = sd2)))))
  }

  def withNamespace(ns: Name)(f: Factset): Factset =
    f.copy(partitions = f.partitions.copy(partitions = f.partitions.partitions.map(_.copy(namespace = ns))))
}
