package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage._
import com.ambiata.mundane.control._

import scalaz._, Scalaz._

object SnapshotStorage {
  def ids(repository: Repository): RIO[List[SnapshotId]] =
    repository.store.listHeads(Repository.snapshots).map(_.filterHidden).map(ids =>
      ids.flatMap(sid => SnapshotId.parse(sid.name).toList))

  def byId(repository: Repository, id: SnapshotId): RIO[Snapshot] =
    SnapshotMetadataStorage.byIdOrFail(repository, id).flatMap(byMetadata(repository, _))

  def byMetadata(repository: Repository, metadata: SnapshotMetadata): RIO[Snapshot] = for {
    store <- FeatureStoreTextStorage.fromId(repository, metadata.storeId)
    dictionary <- metadata.dictionaryId.traverseU(id => Metadata.dictionaryFromIvory(repository, id).map(id -> _))
    // FIX add size to metadata
    size <- IvoryLocation.size(repository.toIvoryLocation(Repository.snapshot(metadata.id)))
  } yield Snapshot(metadata.id, metadata.date, store, dictionary, size)
}
