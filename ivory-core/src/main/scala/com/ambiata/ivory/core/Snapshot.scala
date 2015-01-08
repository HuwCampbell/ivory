package com.ambiata.ivory.core

case class Snapshot(id: SnapshotId, date: Date, store: FeatureStore, dictionary: Option[(DictionaryId, Dictionary)], bytes: Long) {
  def toMetadata: SnapshotMetadata =
    SnapshotMetadata(id, date, store.id, dictionary.map(_._1))
}
