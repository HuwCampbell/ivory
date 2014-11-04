package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.{FeatureStore, Date, SnapshotId}
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.mundane.control._

/**
 * Identifier and date for the snapshot of a given store fully loaded in memory
 */
case class FeatureStoreSnapshot(snapshotId: SnapshotId, date: Date, store: FeatureStore)

object FeatureStoreSnapshot {
  def fromSnapshotMeta(repository: Repository): SnapshotMeta => ResultTIO[FeatureStoreSnapshot] = (meta: SnapshotMeta) =>
    featureStoreFromIvory(repository, meta.featureStoreId).map { store =>
      FeatureStoreSnapshot(meta.snapshotId, meta.date, store)
    }
}
