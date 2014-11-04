package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.{FeatureStore, Date, SnapshotId}
/**
 * Identifier and date for the snapshot of a given store fully loaded in memory
 */
case class FeatureStoreSnapshot(snapshotId: SnapshotId, date: Date, store: FeatureStore)
