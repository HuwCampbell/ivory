package com.ambiata.ivory.operation.extraction.snapshot

import com.ambiata.ivory.core._

case class SnapshotWindow(featureId: FeatureId, startDate: Option[Date])

case class SnapshotWindows(windows: List[SnapshotWindow]) {

  def getByFeature(fid: FeatureId): Option[SnapshotWindow] =
    windows.find(_.featureId == fid)

  def byNamespace: Map[Namespace, Option[Date]] =
    windows.groupBy(_.featureId.namespace).mapValues(_.flatMap(_.startDate).sorted.headOption)
}

object SnapshotWindows {

  def empty: SnapshotWindows =
    SnapshotWindows(Nil)

  def planWindow(dictionary: Dictionary, date: Date): SnapshotWindows =
    SnapshotWindows(dictionary.byConcrete.sources.map {
      case (fid, windows) => SnapshotWindow(fid, windows.virtual.flatMap(_._2.window)
        .map(Window.startingDate(_, date)).sorted.headOption)
    }.toList)
}
