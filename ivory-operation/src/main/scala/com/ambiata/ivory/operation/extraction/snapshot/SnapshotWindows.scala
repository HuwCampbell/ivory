package com.ambiata.ivory.operation.extraction.snapshot

import com.ambiata.ivory.core._

case class SnapshotWindow(featureId: FeatureId, startDate: Option[Date])

case class SnapshotWindows(windows: List[SnapshotWindow]) {

  def getByFeature(fid: FeatureId): Option[SnapshotWindow] =
    windows.find(_.featureId == fid)

  def byNamespace: Map[Name, Option[Date]] =
    windows.groupBy(_.featureId.namespace).mapValues(_.flatMap(_.startDate).sorted.headOption)
}

object SnapshotWindows {

  def empty: SnapshotWindows =
    SnapshotWindows(Nil)

  /**
   * Convert a Window to the largest possible number of days, regardless of the date in which the period ends.
   * This is used to calculate whether a fact should be captured in the snapshot.
   * WARNING: Uses Joda - do not use from Hadoop
   */
  def startingDate(window: Window, date: Date): Date =
    Date.fromLocalDate(date.localDate.minusDays(window.length * (window.unit match {
      case Days   => 1
      case Weeks  => 7
      case Months => 31
      case Years  => 366
    })))

  /** WARNING: Uses Joda - do not use from Hadoop */
  def startingDate(window: Window, date: Date): Date =
    Date.fromLocalDate(date.localDate.minusDays(toDaysMax(window)))

  def planWindow(dictionary: Dictionary, date: Date): SnapshotWindows =
    SnapshotWindows(dictionary.byConcrete.sources.map {
      case (fid, windows) => SnapshotWindow(fid, windows.virtual.flatMap(_._2.window)
        .map(startingDate(_, date)).sorted.headOption)
    }.toList)
}
