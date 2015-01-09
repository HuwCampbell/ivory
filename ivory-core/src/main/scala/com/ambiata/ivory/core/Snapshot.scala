package com.ambiata.ivory.core

case class Snapshot(
  id: SnapshotId
, date: Date
, store: FeatureStore
, dictionary: Option[Identified[DictionaryId, Dictionary]]
, bytes: Bytes
) {
  def toMetadata: SnapshotMetadata =
    SnapshotMetadata(id, date, store.id, dictionary.map(_.id))
}
