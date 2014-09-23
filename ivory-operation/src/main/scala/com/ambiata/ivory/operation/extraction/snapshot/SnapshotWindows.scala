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
   * Convert a Window to the relative calendar date (eg. 2 months from 2014-12-21 is 2014-10-21).
   * This is used to calculate the exact date used in the squash.
   * WARNING: Uses Joda - do not use from Hadoop
   */
  def startingDate(window: Window, date: Date): Date = {
    val local = date.localDate
    Date.fromLocalDate((window.unit match {
      case Days   => local.minusDays _
      case Weeks  => local.minusWeeks _
      case Months => local.minusMonths _
      case Years  => local.minusYears _
    })(window.length))
  }

  def planWindow(dictionary: Dictionary, date: Date): SnapshotWindows =
    SnapshotWindows(dictionary.byConcrete.sources.map {
      case (fid, windows) => SnapshotWindow(fid, windows.virtual.flatMap(_._2.window)
        .map(startingDate(_, date)).sorted.headOption)
    }.toList)
}
