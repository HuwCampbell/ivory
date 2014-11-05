package com.ambiata.ivory.operation.extraction.snapshot

import com.ambiata.ivory.core._

import scalaz.Scalaz._

/**
 * Represents a slices of the partition data to load from the [[FeatureStore]].
 *
 * The date values are the start and end dates, _inclusive_, to load from the partitions.
 */
case class SnapshotPartition(store: FeatureStore, start: Date, end: Date)

/**
 * Incremental Snapshot
 * ====================
 *
 * When doing an incremental snapshot we need to load the minimum set of data from a combination of the previous
 * snapshot and the current store. Consider the following:
 *
 * Previous Snapshot
 * -----------------
 *
 * - Date: 2014/01/03
 * - Factsets
 *   - 0000001: [2014/01/01, 2014/01/02]
 *   - 0000002: [2014/01/03, 2014/01/04]
 *
 * CurrentStore
 * ------------
 *
 * - Factsets
 *   - 0000001: ...
 *   - 0000002: ...
 *   - 0000003: [2014/01/01, 2014/01/05]
 *
 * Incremental Snapshot
 * --------------------
 *
 * - Date: 2014/01/04
 * - Factsets
 *   - 0000002: [2014/01/04]
 *   - 0000003: [2014/01/01]
 * - Plus previous snapshot
 */
object SnapshotPartition {

  // If we're not incremental there's nothing we can do - load everything
  def partitionAll(store: FeatureStore, snapshotDate: Date): List[SnapshotPartition] =
    List(SnapshotPartition(store, Date.minValue, snapshotDate))

  def partitionIncremental(store: FeatureStore, prevStore: FeatureStore, snapshotDate: Date, prevSnapshotDate: Date): List[SnapshotPartition] =
    List(
      // always read partitions which haven't been seen from the previous 'snapshot date' up to the 'latest' date
      SnapshotPartition(prevStore, prevSnapshotDate, snapshotDate),
      // read factsets which haven't been seen up until the 'latest' date
      SnapshotPartition(store.diff(prevStore), Date.minValue, snapshotDate)
    )

  /**
   * For a windowed feature, calculate the extra/missed partitions prior to the snapshot to load.
   */
  def partitionIncrementalWindowing(prevStore: FeatureStore, prevSnapshotDate: Date, windows: SnapshotWindows,
                                    prevWindows: SnapshotWindows): List[SnapshotPartition] = for {
    (ns, windowStart) <- windows.byNamespace.toList
    prevNamespaces     = prevWindows.byNamespace
    ws                <- windowStart
    (sd, ed)          <- calculateDateRange(prevSnapshotDate, prevNamespaces.get(ns).flatten, ws)
  } yield SnapshotPartition(filterFeatureStore(prevStore, _.namespace === ns), sd, ed)

  def calculateDateRange(prevSnapshotDate: Date, previousWindow: Option[Date], windowStart: Date): Option[(Date, Date)] = {
    // Find the minimum date between the previous snapshot and previous window (for a single feature)
    // We know that if the previous window date exists then, by definition, it was captured in the snapshot
    val earliest = previousWindow.getOrElse(prevSnapshotDate) min prevSnapshotDate
    // If the window is prior to the previous snapshot or last window, go back to the dawn of time up until that point
    (windowStart < earliest).option(Date.minValue -> earliest)
  }

  def filterFeatureStore(store: FeatureStore, f: Partition => Boolean): FeatureStore =
    store.copy(factsets = store.factsets.map(pr => pr.copy(value = pr.value.filterByPartition(f))))
}
